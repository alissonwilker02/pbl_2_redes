package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net"
	"os"
	"strings"
	"time"
	"pbl_2_redes/compartilhado"
	exclusaomutua "pbl_2_redes/modulos_internos/exclusao_mutua"
	rede_p2p "pbl_2_redes/modulos_internos/rede_p2p"
	servidor_local "pbl_2_redes/modulos_internos/servidor_local"
)


// main é a função principal que inicializa o gerenciador do setor. Ela valida os argumentos de linha 
// de comando, configura o estado global do setor, armazena os vizinhos, inicia os servidores P2P e 
// local (sensores/drones) em goroutines separadas e dispara os loops contínuos de despacho e 
// monitoramento de saúde dos drones.
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
	fmt.Printf("║       🛰  GERENCIADOR DO SETOR %-3s — ESTREITO DE ORMUZ  ║\n", setorID)
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

	select {}
}






// ============================================================
// LOOP DE DESPACHO
// ============================================================
// loopDespacho é uma rotina contínua que processa a fila de ocorrências do setor. Ela ignora eventos 
// normais, utiliza o algoritmo de Ricart-Agrawala para solicitar acesso exclusivo na rede, verifica se o 
// setor alvo já está sendo atendido e, caso haja um drone livre, despacha a missão. Caso contrário ou em 
// caso de falhas, re-enfileira a ocorrência.
func loopDespacho(estado *servidor_local.EstadoGerenciador, vizinhos []string) {
	for {
		time.Sleep(2 * time.Second)

		// Imprime status atual a cada ciclo — terminal sempre informativo
		imprimirStatusContinuo(estado)

		ocorrencia := estado.ProximaOcorrencia()
		if ocorrencia == nil {
			continue
		}

		// Único descarte legítimo: eventos de monitoramento Normal
		if ocorrencia.TipoEvento == "Normal" {
			logFase("MONITOR", estado.Setor, "Evento Normal — nenhuma ação necessária.")
			continue
		}
		
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

		exclusaomutua.SolicitarAcessoRecurso(estado, vizinhos)
		exclusaomutua.AguardarPermissao(estado)

		logFase("MUTEX", estado.Setor, "HELD — seção crítica obtida.")

		
		// Necessário porque outro broker pode ter despachado para este setor
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

			// marca EM_MISSAO localmente antes de liberar mutex
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
				// registra missão ativa para recuperação futura
				estado.RegistrarMissao(droneLivre.IDDrone, *ocorrencia)
				logFase("MISSÃO", estado.Setor,
					fmt.Sprintf("Missão '%s' entregue ao drone %s.",
						ocorrencia.TipoEvento, droneLivre.IDDrone))
				servidor_local.LogarEstado(estado,
					fmt.Sprintf("Drone %s → missão '%s' no setor %s",
						droneLivre.IDDrone, ocorrencia.TipoEvento, ocorrencia.Setor))
			} else {
				// Falha no envio: drone estava offline antes de decolar
				logFase("ERRO", estado.Setor,
					fmt.Sprintf("Falha ao contatar drone %s. Re-enfileirando missão.",
						droneLivre.IDDrone))

				// Reverte  status
				estado.Mu.Lock()
				tmp := estado.TabelaDrones[droneLivre.IDDrone]
				tmp.Status = "DISPONIVEL"
				tmp.Setor = ""
				estado.TabelaDrones[droneLivre.IDDrone] = tmp
				estado.Mu.Unlock()

				// Re-enfileira — NÃO perde a ocorrência
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
		logFase("MUTEX", estado.Setor, "RELEASED.")
	}
}




// loopHealthCheckDrones monitora periodicamente a conectividade com os drones registrados efetuando pings 
// via TCP. Se um drone parar de responder, ele é removido da frota (se estava disponível) ou sua missão 
// é cancelada, recuperada e devolvida para a fila de ocorrências (se estava em missão).
func loopHealthCheckDrones(estado *servidor_local.EstadoGerenciador) {
	for {
		time.Sleep(10 * time.Second)

		
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
					// Estava disponível mas não responde - remove da tabela
					// (provavelmente foi derrubado)
					logFase("HEALTH", estado.Setor,
						fmt.Sprintf(" Drone %s (DISPONIVEL) não responde → removendo da frota.",
							drone.IDDrone))
					estado.Mu.Lock()
					delete(estado.TabelaDrones, drone.IDDrone)
					estado.Mu.Unlock()
					servidor_local.LogarEstado(estado,
						fmt.Sprintf("Drone %s removido da frota (offline)", drone.IDDrone))

				case "EM_MISSAO":
					// Estava em missão mas não responde - caiu durante a missão
					logFase("HEALTH", estado.Setor,
						fmt.Sprintf(" Drone %s (EM_MISSAO) caiu. Recuperando missão...",
							drone.IDDrone))

					// MarcarDroneOffline reverte status E extrai missão em andamento
					missao := estado.MarcarDroneOffline(drone.IDDrone)

					if missao != nil {
						logFase("HEALTH", estado.Setor,
							fmt.Sprintf(" Missão '%s' (crit %d) recuperada → re-enfileirando.",
								missao.Ocorrencia.TipoEvento,
								missao.Ocorrencia.Criticidade))
						// Re-enfileira com timestamp ORIGINAL para preservar prioridade
						estado.AdicionarOcorrencia(missao.Ocorrencia)
					} else {
						logFase("HEALTH", estado.Setor,
							fmt.Sprintf("ℹ Drone %s sem missão registrada — apenas revertendo status.",
								drone.IDDrone))
					}
					servidor_local.LogarEstado(estado,
						fmt.Sprintf("Drone %s offline — missão recuperada e re-enfileirada", drone.IDDrone))
				}

			} else {
				conn.Close()
				// Drone respondeu — está vivo, sem ação necessária
			}
		}
	}
}



// imprimirStatusContinuo exibe de forma formatada no terminal o status geral em tempo real do setor,
// incluindo a condição atual do mutex, listagem das missões em execução, o conteúdo da fila de 
// ocorrências aguardando atendimento e a relação de drones disponíveis e ocupados.
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
		fmt.Printf("\033[90m│\033[0m  Execução : \033[90mnenhuma missão ativa\033[0m                          \033[90m│\033[0m\n")
	} else {
		for _, m := range missoes {
			ev := m.Ocorrencia.TipoEvento
			if len(ev) > 22 {
				ev = ev[:22] + ".."
			}
			duracao := time.Since(m.IniciadaEm).Round(time.Second)
			fmt.Printf("\033[90m│\033[0m  Execução : \033[33m%-12s\033[0m → \033[32m%-18s\033[0m há %v \033[90m│\033[0m\n",
				m.DroneID, ev, duracao)
		}
	}

	// Fila pendente
	if len(fila) == 0 {
		fmt.Printf("\033[90m│\033[0m   Fila     : \033[90mvazia\033[0m                                         \033[90m│\033[0m\n")
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
	fmt.Printf("\033[90m│\033[0m    Disponív : %-50s\033[90m│\033[0m\n", dispStr)
	fmt.Printf("\033[90m│\033[0m    Em missão : %-50s\033[90m│\033[0m\n", missaoStr)
	fmt.Printf("\033[90m└──────────────────────────────────────────────────────────┘\033[0m\n")
}




// ============================================================
// ENVIAR MISSAO AO DRONE
// ============================================================
// enviarMissaoAoDrone tenta abrir uma conexão TCP com um drone e enviar, em formato JSON,
// as diretrizes da missão a ser executada. Retorna verdadeiro se o envio for concluído com
// sucesso ou falso caso não consiga estabelecer a conexão.
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
// logFase é uma função utilitária para registrar atividades no terminal. Ela recebe a fase atual da 
// execução (como MUTEX, MISSÃO ou ERRO), o setor e uma mensagem, imprimindo-os de maneira categorizada
// e colorida para facilitar o rastreamento visual dos eventos.
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
// baraCritCor cria uma representação visual (uma barra de progresso) da criticidade de uma ocorrência,
// que vai de 1 a 5. A função retorna uma string com blocos preenchidos e vazios, pintados nas cores
// verde (baixa), amarela (média) ou vermelha (alta), dependendo do nível de urgência.
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
// parsearVizinho processa a string recebida via argumento de linha de comando que representa um setor vizinho.
// Ele divide o formato "Setor:IP:Porta" para separar e retornar o identificador do setor e o seu respectivo
// endereço de conexão.
func parsearVizinho(arg string) (string, string) {
	for i, c := range arg {
		if c == ':' && i > 0 {
			return arg[:i], arg[i+1:]
		}
	}
	return "", ""
}