package main

// =============================================================================
// TESTE DE CONCORRÊNCIA — ESTREITO DE ORMUZ
// =============================================================================
// Demonstra os 4 cenários de concorrência do sistema distribuído:
//
//   Teste 1 — Dois setores pedem drone simultaneamente (Ricart-Agrawala)
//   Teste 2 — Drone disputado: só um setor vence
//   Teste 3 — Fila de prioridade: crítico passa na frente
//   Teste 4 — Tolerância a falhas: setor cai e sistema continua
//
// Uso: go run teste_concorrencia.go <endA> <endB> <endC>
// Exemplo: go run teste_concorrencia.go localhost:5000 localhost:5001 localhost:5002
//
// Cada endereço é a porta de SENSORES do respectivo gerenciador (não a P2P).
// =============================================================================

import (
	"encoding/json"
	"fmt"
	"net"
	"os"
	"sync"
	"time"
	"pbl_2_redes/compartilhado"
)

// Endereços dos três gerenciadores (porta de sensores)
var enderecos [3]string

func main() {
	if len(os.Args) < 4 {
		fmt.Println("Uso: go run teste_concorrencia.go <endA> <endB> <endC>")
		fmt.Println("Exemplo: go run teste_concorrencia.go localhost:5000 localhost:5001 localhost:5002")
		os.Exit(1)
	}

	enderecos[0] = os.Args[1] // Setor A
	enderecos[1] = os.Args[2] // Setor B
	enderecos[2] = os.Args[3] // Setor C

	exibirCabecalho()

	// ─────────────────────────────────────────────────────────────
	// TESTE 1 — Dois setores disputam drone ao mesmo tempo
	// Demonstra o algoritmo de Ricart-Agrawala:
	// ambos enviam REQUEST, apenas um entra em HELD por vez.
	// ─────────────────────────────────────────────────────────────
	executarTeste(1,
		"Dois setores pedem drone SIMULTANEAMENTE",
		"Ricart-Agrawala garante que apenas UM entre na seção crítica.",
		"Observe nos logs: REQUEST → REPLY → HELD → RELEASE",
		func() {
			var wg sync.WaitGroup
			wg.Add(2)

			// Setor A e B disparam ao mesmo tempo
			go func() {
				defer wg.Done()
				enviarOcorrencia(enderecos[0], "A", "sensor_teste_A1",
					"Vazamento_oleo", 4)
			}()
			go func() {
				defer wg.Done()
				enviarOcorrencia(enderecos[1], "B", "sensor_teste_B1",
					"Embarcacao_desgovernada", 4)
			}()

			wg.Wait()
		},
	)

	aguardar(8, "aguardando drones responderem antes do próximo teste")

	// ─────────────────────────────────────────────────────────────
	// TESTE 2 — Drone disputado: só um setor vence
	// Envia ocorrências críticas para TODOS os setores ao mesmo tempo.
	// Com apenas um drone disponível, apenas um setor consegue alocar.
	// Os outros re-enfileiram e aguardam.
	// ─────────────────────────────────────────────────────────────
	executarTeste(2,
		"Três setores disputam o MESMO drone",
		"Apenas um setor aloca. Os outros ficam em fila.",
		"Observe: shadow status local impede dupla alocação",
		func() {
			var wg sync.WaitGroup
			wg.Add(3)

			setores := []struct{ end, id, setor, evento string; crit int }{
				{enderecos[0], "sensor_teste_A2", "A", "bloqueio_passagem", 5},
				{enderecos[1], "sensor_teste_B2", "B", "bloqueio_passagem", 5},
				{enderecos[2], "sensor_teste_C2", "C", "bloqueio_passagem", 5},
			}

			for _, s := range setores {
				s := s
				go func() {
					defer wg.Done()
					enviarOcorrencia(s.end, s.setor, s.id, s.evento, s.crit)
				}()
			}
			wg.Wait()
		},
	)

	aguardar(12, "aguardando resolução da disputa")

	// ─────────────────────────────────────────────────────────────
	// TESTE 3 — Fila de prioridade: crítico passa na frente
	// Envia primeiro um evento de criticidade baixa (2),
	// depois imediatamente um crítico (5).
	// O de criticidade 5 deve ser atendido primeiro.
	// ─────────────────────────────────────────────────────────────
	executarTeste(3,
		"Evento CRÍTICO (5) passa na frente do evento baixo (2)",
		"sort.SliceStable garante ordem por criticidade → timestamp.",
		"Observe a fila impressa no log do gerenciador A",
		func() {
			// Primeiro envia o baixo
			enviarOcorrencia(enderecos[0], "A", "sensor_teste_A3_baixo",
				"Normal", 1)

			// Pequena pausa para garantir que o baixo chegou primeiro
			time.Sleep(200 * time.Millisecond)

			// Depois envia o crítico — deve pular na fila
			enviarOcorrencia(enderecos[0], "A", "sensor_teste_A3_critico",
				"Vazamento_oleo", 5)

			// Envia mais um médio para ver a ordenação completa
			time.Sleep(100 * time.Millisecond)
			enviarOcorrencia(enderecos[0], "A", "sensor_teste_A3_medio",
				"Embarcacao_desgovernada", 3)
		},
	)

	aguardar(5, "aguardando processamento da fila")

	// ─────────────────────────────────────────────────────────────
	// TESTE 4 — Tolerância a falhas: setor cai, sistema continua
	// Envia ocorrências para setores B e C. Setor A está fora.
	// O algoritmo de Ricart-Agrawala trata o silêncio de A como
	// REPLY implícito (timeout + ProcessarReply automático).
	// Sistema continua funcionando normalmente.
	// ─────────────────────────────────────────────────────────────
	executarTeste(4,
		"Setor A simulado como OFFLINE — B e C continuam",
		"Silêncio de A = REPLY implícito (tolerância a falhas).",
		"Observe: timeout de 2s e log '[MUTEX] Ignorando silêncio'",
		func() {
			var wg sync.WaitGroup
			wg.Add(2)

			// Só B e C enviam — A está fora (não enviamos nada pra A,
			// mas B e C vão tentar contactá-lo no Ricart-Agrawala e receber timeout)
			go func() {
				defer wg.Done()
				enviarOcorrencia(enderecos[1], "B", "sensor_teste_B4",
					"Embarcacao_desgovernada", 3)
			}()
			go func() {
				defer wg.Done()
				enviarOcorrencia(enderecos[2], "C", "sensor_teste_C4",
					"Vazamento_oleo", 4)
			}()

			wg.Wait()
		},
	)

	aguardar(10, "aguardando conclusão dos testes")

	// Resultado final
	fmt.Println()
	fmt.Println("╔══════════════════════════════════════════════════════════╗")
	fmt.Println("║              TODOS OS TESTES CONCLUÍDOS                 ║")
	fmt.Println("╠══════════════════════════════════════════════════════════╣")
	fmt.Println("║  Verifique nos logs dos gerenciadores:                   ║")
	fmt.Println("║  • Teste 1: REQUEST/REPLY/HELD nos dois setores          ║")
	fmt.Println("║  • Teste 2: Apenas 1 setor aloca, 2 re-enfileiram        ║")
	fmt.Println("║  • Teste 3: Criticidade 5 aparece antes do 3 e do 1     ║")
	fmt.Println("║  • Teste 4: '[MUTEX] Ignorando silêncio' para Setor A    ║")
	fmt.Println("╚══════════════════════════════════════════════════════════╝")
	fmt.Println()
}

// ============================================================
// EXECUTAR TESTE — Wrapper com cabeçalho visual
// ============================================================
func executarTeste(num int, titulo, explicacao, dica string, fn func()) {
	fmt.Printf("\n┌──────────────────────────────────────────────────────────┐\n")
	fmt.Printf("│  TESTE %d — %-49s│\n", num, titulo)
	fmt.Printf("├──────────────────────────────────────────────────────────┤\n")
	fmt.Printf("│  O quê:  %-51s│\n", explicacao)
	fmt.Printf("│  Dica:   %-51s│\n", dica)
	fmt.Printf("└──────────────────────────────────────────────────────────┘\n")
	fmt.Printf("  → Disparando em %s...\n\n", time.Now().Format("15:04:05.000"))

	fn()

	fmt.Printf("\n  ✓ Teste %d disparado. Acompanhe os logs dos gerenciadores.\n", num)
}

// ============================================================
// ENVIAR OCORRÊNCIA — Conecta e envia uma ocorrência ao gerenciador
// ============================================================
func enviarOcorrencia(endereco, setor, sensorID, evento string, criticidade int) {
	conn, err := net.DialTimeout("tcp", endereco, 3*time.Second)
	if err != nil {
		fmt.Printf("  [!] Não foi possível conectar em %s (setor %s): %v\n",
			endereco, setor, err)
		return
	}
	defer conn.Close()

	ocorrencia := compartilhado.Ocorrencia{
		IDSensor:    sensorID,
		Setor:       setor,
		TipoEvento:  evento,
		Criticidade: criticidade,
		Timestamp:   time.Now(),
	}

	dados, _ := json.Marshal(ocorrencia)
	dados = append(dados, '\n')
	conn.Write(dados)

	barra := baraCriticidade(criticidade)
	fmt.Printf("  [ENVIADO] Setor %s | %-25s | Crit %s\n",
		setor, evento, barra)
}

// ============================================================
// AGUARDAR — Pausa com countdown visual entre testes
// ============================================================
func aguardar(segundos int, motivo string) {
	fmt.Printf("\n  ⏳ Aguardando %ds (%s)...\n", segundos, motivo)
	for i := segundos; i > 0; i-- {
		fmt.Printf("\r  ⏳ %ds restantes...   ", i)
		time.Sleep(1 * time.Second)
	}
	fmt.Printf("\r  ✓ Pronto.                      \n")
}

// ============================================================
// BARRA DE CRITICIDADE — Visualização ASCII
// ============================================================
func baraCriticidade(crit int) string {
	preenchido := ""
	for i := 0; i < crit; i++ {
		preenchido += "█"
	}
	for i := crit; i < 5; i++ {
		preenchido += "░"
	}
	return fmt.Sprintf("[%s] %d", preenchido, crit)
}

// ============================================================
// EXIBIR CABEÇALHO
// ============================================================
func exibirCabecalho() {
	fmt.Println()
	fmt.Println("╔══════════════════════════════════════════════════════════╗")
	fmt.Println("║        SUITE DE TESTES DE CONCORRÊNCIA                  ║")
	fmt.Println("║        Sistema Distribuído — Estreito de Ormuz           ║")
	fmt.Println("╠══════════════════════════════════════════════════════════╣")
	fmt.Println("║  Testes:                                                 ║")
	fmt.Println("║  1. Ricart-Agrawala (dois setores simultâneos)           ║")
	fmt.Println("║  2. Disputa por drone (três setores, um vence)           ║")
	fmt.Println("║  3. Fila de prioridade (crítico passa na frente)         ║")
	fmt.Println("║  4. Tolerância a falhas (setor offline, sistema OK)      ║")
	fmt.Println("╚══════════════════════════════════════════════════════════╝")
	fmt.Println()
}