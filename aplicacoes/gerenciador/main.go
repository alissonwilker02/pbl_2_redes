package main


import (
	"encoding/json"
	"fmt"
	"log"
	"net"
	"os"
	"sort"
	"strings"
	"time"

	"pbl_2_redes/compartilhado"
	exclusaomutua "pbl_2_redes/modulos_internos/exclusao_mutua"
	rede_p2p "pbl_2_redes/modulos_internos/rede_p2p"
	servidor_local "pbl_2_redes/modulos_internos/servidor_local"
)

func main() {
	if len(os.Args) < 6 {
		fmt.Println("Uso: go run main.go <Setor> <Porta_Sensores> <Porta_P2P> <SetorViz1:IP:Porta> <SetorViz2:IP:Porta>")
		os.Exit(1)
	}

	setorID := os.Args[1]
	portaSensores := os.Args[2]
	portaP2P := os.Args[3]

	if setorID != "A" && setorID != "B" && setorID != "C" {
		log.Fatalf("[ERRO FATAL] Setor '%s' inválido. Use A, B ou C.\n", setorID)
	}

	vizinhos := []string{}
	for _, arg := range os.Args[4:] {
		setor, endereco := parsearVizinho(arg)
		if setor == "" {
			log.Fatalf("[ERRO] Formato inválido: '%s'. Use Setor:IP:Porta", arg)
		}
		exclusaomutua.MapaSetorParaEndereco[setor] = endereco
		vizinhos = append(vizinhos, endereco)
	}

	fmt.Printf("\n╔══════════════════════════════════════════════════════════╗\n")
	fmt.Printf("║          GERENCIADOR DO SETOR %-3s — ESTREITO DE ORMUZ  ║\n", setorID)
	fmt.Printf("╠══════════════════════════════════════════════════════════╣\n")
	fmt.Printf("║  Porta sensores : %-5s                                  ║\n", portaSensores)
	fmt.Printf("║  Porta P2P      : %-5s                                  ║\n", portaP2P)
	for i, v := range vizinhos {
		fmt.Printf("║  Vizinho %-2d     : %-38s ║\n", i+1, v)
	}
	fmt.Printf("╚══════════════════════════════════════════════════════════╝\n\n")

	estadoGlobal := servidor_local.NovoEstado(setorID)

	go rede_p2p.IniciarServidorP2P(":"+portaP2P, estadoGlobal, vizinhos)
	go servidor_local.IniciarServidorLocal(":"+portaSensores, estadoGlobal)
	go loopDespacho(estadoGlobal, vizinhos)
	go loopHealthCheckDrones(estadoGlobal)
	go loopEnvelhecimento(estadoGlobal)

	select {}
}

// ============================================================
// LOOP DE DESPACHO
// ============================================================
func loopDespacho(estado *servidor_local.EstadoGerenciador, vizinhos []string) {
	for {
		time.Sleep(2 * time.Second)

		// Imprime status atual a cada ciclo
		imprimirStatusContinuo(estado)

		ocorrencia := estado.ProximaOcorrencia()
		if ocorrencia == nil {
			continue
		}

		// eventos de monitoramento Normal
		if ocorrencia.TipoEvento == "Normal" {
			logFase("MONITOR", estado.Setor, "Evento Normal — nenhuma ação necessária.")
			continue
		}

		//  setor já tem drone em missão? ─────────────────────────
		// re-enfileira para tentar mais tarde.
		// A ocorrência fica na fila até o drone atual terminar ou outro
		// drone ficar disponível.
		if estado.SetorJaAtendido(ocorrencia.Setor) {
			logFase("REQ-3", estado.Setor,
				fmt.Sprintf("Setor %s já monitorado. Re-enfileirando '%s' para atender depois.",
					ocorrencia.Setor, ocorrencia.TipoEvento))
			estado.AdicionarOcorrencia(*ocorrencia)
			continue
		}

		// ── RICART-AGRAWALA ───────────────────────────────────────────────
		logFase("MUTEX", estado.Setor,
			fmt.Sprintf("'%s' (crit %d) — solicitando acesso exclusivo...",
				ocorrencia.TipoEvento, ocorrencia.Criticidade))

		exclusaomutua.SolicitarAcessoRecurso(estado, vizinhos, ocorrencia.Criticidade, ocorrencia.Timestamp)
		exclusaomutua.AguardarPermissao(estado)

		logFase("MUTEX", estado.Setor, "  HELD — seção crítica obtida.")

		// ── PREEMPÇÃO DE PRIORIDADE  ───────────────────────
		// Durante a espera pelos REPLYs pode ter chegado uma ocorrência mais
		// crítica (ou com mesmo crit mas timestamp mais antigo). Se isso
		// aconteceu, devolvemos a ocorrência atual à fila e liberamos o mutex
		// para que o próximo ciclo dispute com a prioridade correta.
		if topo := estado.TopoFila(); topo != nil {
			topoVence := false
			if topo.Criticidade > ocorrencia.Criticidade {
				topoVence = true
			} else if topo.Criticidade == ocorrencia.Criticidade &&
				topo.Timestamp.Before(ocorrencia.Timestamp) {
				topoVence = true
			}
			if topoVence {
				logFase("MUTEX", estado.Setor,
					fmt.Sprintf("  Preempção: topo da fila (crit %d, ts %s) supera atual (crit %d, ts %s). Re-enfileirando e liberando mutex.",
						topo.Criticidade, topo.Timestamp.Format("15:04:05.000"),
						ocorrencia.Criticidade, ocorrencia.Timestamp.Format("15:04:05.000")))
				exclusaomutua.LiberarRecurso(estado, vizinhos)
				estado.AdicionarOcorrencia(*ocorrencia)
				continue
			}
		}

		// ── DOUBLE-CHECK  dentro da seção crítica ───────────────────
		// outro broker pode ter despachado para este setor
		// enquanto aguardávamos os REPLYs do Ricart-Agrawala.
		if estado.SetorJaAtendido(ocorrencia.Setor) {
			logFase("REQ-3", estado.Setor,
				fmt.Sprintf("Setor %s foi assumido durante espera do mutex. Re-enfileirando.",
					ocorrencia.Setor))
			exclusaomutua.LiberarRecurso(estado, vizinhos)
			servidor_local.LogarEstado(estado, "Mutex liberado — re-enfileirou por setor ocupado")
			// Re-enfileira — NÃO descarta
			estado.AdicionarOcorrencia(*ocorrencia)
			continue
		}

		// ── BUSCA DE DRONE ────────────────────────────────────────────────
		droneLivre := estado.BuscarDroneLivre()

		if droneLivre != nil {
			logFase("DRONE", estado.Setor,
				fmt.Sprintf("Drone encontrado: %s em %s", droneLivre.IDDrone, droneLivre.Endereco))

			// Shadow status: marca EM_MISSAO localmente antes de liberar mutex
			estado.Mu.Lock()
			tmp := estado.TabelaDrones[droneLivre.IDDrone]
			tmp.Status = "EM_MISSAO"
			tmp.Setor = ocorrencia.Setor
			estado.TabelaDrones[droneLivre.IDDrone] = tmp
			estado.Mu.Unlock()

			logFase("MISSÃO", estado.Setor,
				fmt.Sprintf("Enviando missão '%s' para drone %s...",
					ocorrencia.TipoEvento, droneLivre.IDDrone))

			if enviarMissaoAoDrone(droneLivre, ocorrencia) {
				// Sucesso: registra missão ativa para recuperação futura
				estado.RegistrarMissao(droneLivre.IDDrone, *ocorrencia)
				logFase("MISSÃO", estado.Setor,
					fmt.Sprintf(" Missão '%s' entregue ao drone %s.",
						ocorrencia.TipoEvento, droneLivre.IDDrone))
				servidor_local.LogarEstado(estado,
					fmt.Sprintf("Drone %s → missão '%s' no setor %s",
						droneLivre.IDDrone, ocorrencia.TipoEvento, ocorrencia.Setor))
			} else {
				// Falha no envio: drone estava offline antes de decolar
				logFase("ERRO", estado.Setor,
					fmt.Sprintf(" Falha ao contatar drone %s. Re-enfileirando missão.",
						droneLivre.IDDrone))

				// Reverte shadow status
				estado.Mu.Lock()
				tmp := estado.TabelaDrones[droneLivre.IDDrone]
				tmp.Status = "DISPONIVEL"
				tmp.Setor = ""
				estado.TabelaDrones[droneLivre.IDDrone] = tmp
				estado.Mu.Unlock()

				// Re-enfileira
				estado.AdicionarOcorrencia(*ocorrencia)
			}
		} else {
			// Nenhum drone livre agora — aguarda na fila
			logFase("FILA", estado.Setor,
				fmt.Sprintf("Nenhum drone disponível. '%s' aguarda na fila.",
					ocorrencia.TipoEvento))
			estado.AdicionarOcorrencia(*ocorrencia)
		}

		// Libera mutex para outros setores
		exclusaomutua.LiberarRecurso(estado, vizinhos)
		servidor_local.LogarEstado(estado, "Mutex liberado (RELEASED)")
		logFase("MUTEX", estado.Setor, " RELEASED.")
	}
}

// ============================================================
// ENVELHECIMENTO — aumenta criticidade de ocorrências paradas
// ============================================================
// A cada 1 minuto, percorre a fila e incrementa em +1 a criticidade
// de toda ocorrência que ainda não foi atendida, respeitando o teto 5.
// Após atualizar, re-ordena a fila para que ocorrências
// subam para a posição correta.

func loopEnvelhecimento(estado *servidor_local.EstadoGerenciador) {
	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()

	for range ticker.C {
		estado.Mu.Lock()

		promovidas := 0
		for i := range estado.FilaOcorrencias {
			oc := &estado.FilaOcorrencias[i]
			if oc.TipoEvento == "Normal" {
				continue
			}
			if oc.Criticidade < 5 {
				oc.Criticidade++
				promovidas++
			}
		}

		if promovidas > 0 {
			// Re-ordena para posicionar promovidas corretamente
			sort.SliceStable(estado.FilaOcorrencias, func(i, j int) bool {
				a, b := estado.FilaOcorrencias[i], estado.FilaOcorrencias[j]
				if a.Criticidade != b.Criticidade {
					return a.Criticidade > b.Criticidade
				}
				return a.Timestamp.Before(b.Timestamp)
			})
		}

		estado.Mu.Unlock()

		if promovidas > 0 {
			logFase("AGING", estado.Setor,
				fmt.Sprintf(" Envelhecimento: %d ocorrência(s) tiveram criticidade aumentada.", promovidas))
			servidor_local.LogarEstado(estado, fmt.Sprintf("Envelhecimento aplicado — %d ocorrências promovidas", promovidas))
		}
	}
}

// ============================================================
// HEALTH CHECK — testa TODOS os drones
// ============================================================
// Roda a cada 10s. Para cada drone na tabela:
//   - Testa conectividade TCP
//   - Se DISPONIVEL e não responde → remove da tabela
//     (estava offline mas ainda aparecia como disponível)
//   - Se EM_MISSAO e não responde → reverte para DISPONIVEL,
//     recupera a ocorrência e re-enfileira (nenhuma missão perdida)
func loopHealthCheckDrones(estado *servidor_local.EstadoGerenciador) {
	for {
		time.Sleep(10 * time.Second)

		// Snapshot de todos os drones sem segurar lock durante TCP
		estado.Mu.Lock()
		todosDrones := make([]compartilhado.StatusDrone, 0, len(estado.TabelaDrones))
		for _, d := range estado.TabelaDrones {
			todosDrones = append(todosDrones, d)
		}
		estado.Mu.Unlock()

		for _, drone := range todosDrones {
			if drone.Endereco == "" {
				continue
			}

			conn, err := net.DialTimeout("tcp", drone.Endereco, 2*time.Second)

			if err != nil {
				// ── DRONE NÃO RESPONDE ────────────────────────────────────
				switch drone.Status {

				case "DISPONIVEL":
					// Estava disponível mas não responde → remove da tabela
					logFase("HEALTH", estado.Setor,
						fmt.Sprintf(" Drone %s (DISPONIVEL) não responde → removendo da frota.",
							drone.IDDrone))
					estado.Mu.Lock()
					delete(estado.TabelaDrones, drone.IDDrone)
					estado.Mu.Unlock()
					servidor_local.LogarEstado(estado,
						fmt.Sprintf("Drone %s removido da frota (offline)", drone.IDDrone))

				case "EM_MISSAO":
					// Estava em missão mas não responde 
					logFase("HEALTH", estado.Setor,
						fmt.Sprintf("  Drone %s (EM_MISSAO) caiu. Recuperando missão...",
							drone.IDDrone))

					// MarcarDroneOffline reverte status E extrai missão em andamento
					missao := estado.MarcarDroneOffline(drone.IDDrone)

					if missao != nil {
						logFase("HEALTH", estado.Setor,
							fmt.Sprintf("  Missão '%s' (crit %d) recuperada → re-enfileirando.",
								missao.Ocorrencia.TipoEvento,
								missao.Ocorrencia.Criticidade))
						// Re-enfileira com timestamp ORIGINAL para preservar prioridade
						estado.AdicionarOcorrencia(missao.Ocorrencia)
					} else {
						logFase("HEALTH", estado.Setor,
							fmt.Sprintf("  Drone %s sem missão registrada — apenas revertendo status.",
								drone.IDDrone))
					}
					servidor_local.LogarEstado(estado,
						fmt.Sprintf("Drone %s offline — missão recuperada e re-enfileirada", drone.IDDrone))
				}

			} else {
				conn.Close()
				// Drone respondeu 
			}
		}
	}
}

// ============================================================
// STATUS
// ============================================================
func imprimirStatusContinuo(estado *servidor_local.EstadoGerenciador) {
	estado.Mu.Lock()

	setor := estado.Setor
	mutex := estado.EstadoMutex

	missoes := make([]servidor_local.MissaoEmAndamento, 0)
	for _, m := range estado.MissoesEmAndamento {
		missoes = append(missoes, m)
	}

	fila := make([]compartilhado.Ocorrencia, len(estado.FilaOcorrencias))
	copy(fila, estado.FilaOcorrencias)

	// Apenas drones que realmente respondem (filtramos DISPONIVEL aqui)
	dronesDisponiveis := []string{}
	dronesEmMissao := []string{}
	for _, d := range estado.TabelaDrones {
		switch d.Status {
		case "DISPONIVEL":
			dronesDisponiveis = append(dronesDisponiveis, d.IDDrone)
		case "EM_MISSAO":
			label := d.IDDrone
			if d.Setor != "" {
				label += "→" + d.Setor
			}
			dronesEmMissao = append(dronesEmMissao, label)
		}
	}

	estado.Mu.Unlock()

	// Cor do mutex
	mutexCor := "\033[32m"
	switch mutex {
	case "WANTED":
		mutexCor = "\033[33m"
	case "HELD":
		mutexCor = "\033[31m"
	}
	reset := "\033[0m"
	hora := time.Now().Format("15:04:05")

	fmt.Printf("\n\033[90m┌──────────────────────────────────────────────────────────┐\033[0m\n")
	fmt.Printf("\033[90m│\033[0m  \033[1;37mSETOR %s\033[0m  Mutex:%s%-8s%s  %s\033[90m│\033[0m\n",
		setor, mutexCor, mutex, reset, hora)
	fmt.Printf("\033[90m├──────────────────────────────────────────────────────────┤\033[0m\n")

	// Missões ativas
	if len(missoes) == 0 {
		fmt.Printf("\033[90m│\033[0m    Execução : \033[90mnenhuma missão ativa\033[0m                          \033[90m│\033[0m\n")
	} else {
		for _, m := range missoes {
			ev := m.Ocorrencia.TipoEvento
			if len(ev) > 22 {
				ev = ev[:22] + ".."
			}
			duracao := time.Since(m.IniciadaEm).Round(time.Second)
			fmt.Printf("\033[90m│\033[0m    Execução : \033[33m%-12s\033[0m → \033[32m%-18s\033[0m há %v \033[90m│\033[0m\n",
				m.DroneID, ev, duracao)
		}
	}

	// Fila pendente
	if len(fila) == 0 {
		fmt.Printf("\033[90m│\033[0m    Fila     : \033[90mvazia\033[0m                                         \033[90m│\033[0m\n")
	} else {
		for i, oc := range fila {
			seta := "  "
			corEv := "\033[37m"
			if i == 0 {
				seta = "\033[32m▶\033[0m "
				corEv = "\033[1;37m"
			}
			ev := oc.TipoEvento
			if len(ev) > 20 {
				ev = ev[:20] + ".."
			}
			barra := baraCritCor(oc.Criticidade)
			fmt.Printf("\033[90m│\033[0m  %s%s%s%-22s%s Crit%s \033[90m%s\033[0m     \033[90m│\033[0m\n",
				seta, corEv, fmt.Sprintf("%dº ", i+1), ev, reset, barra,
				oc.Timestamp.Format("15:04:05"))
		}
	}

	// Frota
	dispStr := "\033[90mnenhum\033[0m"
	if len(dronesDisponiveis) > 0 {
		dispStr = "\033[32m" + strings.Join(dronesDisponiveis, ", ") + "\033[0m"
	}
	missaoStr := "\033[90mnenhum\033[0m"
	if len(dronesEmMissao) > 0 {
		missaoStr = "\033[33m" + strings.Join(dronesEmMissao, ", ") + "\033[0m"
	}
	fmt.Printf("\033[90m│\033[0m     Disponív : %-50s\033[90m│\033[0m\n", dispStr)
	fmt.Printf("\033[90m│\033[0m     Em missão : %-50s\033[90m│\033[0m\n", missaoStr)
	fmt.Printf("\033[90m└──────────────────────────────────────────────────────────┘\033[0m\n")
}

// ============================================================
// ENVIAR MISSAO AO DRONE
// ============================================================
func enviarMissaoAoDrone(drone *compartilhado.StatusDrone, oc *compartilhado.Ocorrencia) bool {
	missao := compartilhado.ComandoDrone{
		Acao:        "RECONHECIMENTO_" + oc.TipoEvento,
		Localizacao: fmt.Sprintf("COORDENADAS_SETOR_%s", oc.Setor),
	}
	conn, err := net.DialTimeout("tcp", drone.Endereco, 3*time.Second)
	if err != nil {
		return false
	}
	defer conn.Close()
	dados, _ := json.Marshal(missao)
	dados = append(dados, '\n')
	_, err = conn.Write(dados)
	return err == nil
}

// ============================================================
// LOG POR FASE
// ============================================================
func logFase(fase, setor, msg string) {
	hora := time.Now().Format("15:04:05")
	cores := map[string]string{
		"MONITOR": "\033[90m",
		"REQ-3":   "\033[33m",
		"MUTEX":   "\033[36m",
		"DRONE":   "\033[35m",
		"MISSÃO":  "\033[32m",
		"ERRO":    "\033[31m",
		"FILA":    "\033[33m",
		"HEALTH":  "\033[34m",
		"AGING":   "\033[95m",
	}
	cor := cores[fase]
	if cor == "" {
		cor = "\033[37m"
	}
	fmt.Printf("%s[%s][Setor %s][%-7s]\033[0m %s\n", cor, hora, setor, fase, msg)
}

// ============================================================
// BARRA DE CRITICIDADE COM COR
// ============================================================
func baraCritCor(crit int) string {
	cores := map[int]string{1: "\033[32m", 2: "\033[32m", 3: "\033[33m", 4: "\033[33m", 5: "\033[31m"}
	cor := cores[crit]
	if cor == "" {
		cor = "\033[37m"
	}
	p, v := "", ""
	for i := 0; i < crit; i++ { p += "█" }
	for i := crit; i < 5; i++ { v += "░" }
	return cor + "[" + p + "\033[90m" + v + "\033[0m" + cor + "]" + "\033[0m"
}

// ============================================================
// PARSEAR VIZINHO
// ============================================================
func parsearVizinho(arg string) (string, string) {
	for i, c := range arg {
		if c == ':' && i > 0 {
			return arg[:i], arg[i+1:]
		}
	}
	return "", ""
}