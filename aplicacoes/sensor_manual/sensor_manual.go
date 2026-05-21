package main

// =============================================================================
// SENSOR MANUAL - DEMONSTRAÇÃO INTERATIVA
// =============================================================================
// Permite injetar ocorrências manualmente no sistema para demonstração.
// O operador escolhe setor, tipo de evento e criticidade via terminal.
// =============================================================================

import (
	"bufio"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"os"
	"strconv"
	"strings"
	"time"
	"pbl_2_redes/compartilhado"
)

// Paleta de eventos disponíveis para demonstração
var eventosDisponiveis = []string{
	"Normal",
	"Vazamento_oleo",
	"Embarcacao_desgovernada",
	"bloqueio_passagem",
}

func main() {
	if len(os.Args) < 3 {
		fmt.Println("Uso: go run sensor_manual.go <ip_gerenciador:porta> <setor>")
		fmt.Println("Exemplo: go run sensor_manual.go localhost:5000 A")
		os.Exit(1)
	}

	enderecoServidor := os.Args[1]
	setor := os.Args[2]
	sensorID := fmt.Sprintf("sensor_manual_%s", setor)

	exibirBanner(setor, enderecoServidor)

	// Conecta ao gerenciador com retry
	conexao := conectarComRetry(enderecoServidor, sensorID)
	defer conexao.Close()

	scanner := bufio.NewScanner(os.Stdin)

	for {
		exibirMenu()

		// Lê escolha do evento
		fmt.Print("  >> Escolha o evento [1-4]: ")
		if !scanner.Scan() {
			break
		}
		escolha := strings.TrimSpace(scanner.Text())

		// Suporte a saída
		if escolha == "0" || strings.ToLower(escolha) == "sair" {
			fmt.Println("\n[SENSOR MANUAL] Encerrando. Até logo!")
			break
		}

		indice, err := strconv.Atoi(escolha)
		if err != nil || indice < 1 || indice > len(eventosDisponiveis) {
			fmt.Println("  [!] Opção inválida. Tente novamente.")
			continue
		}

		eventoEscolhido := eventosDisponiveis[indice-1]

		// Eventos "Normal" têm criticidade fixa 1
		criticidade := 1
		if eventoEscolhido != "Normal" {
			fmt.Print("  >> Criticidade [2-5]: ")
			if !scanner.Scan() {
				break
			}
			crit, err := strconv.Atoi(strings.TrimSpace(scanner.Text()))
			if err != nil || crit < 2 || crit > 5 {
				fmt.Println("  [!] Criticidade inválida. Use um valor entre 2 e 5.")
				continue
			}
			criticidade = crit
		}

		// Monta e envia a ocorrência
		ocorrencia := compartilhado.Ocorrencia{
			IDSensor:    sensorID,
			Setor:       setor,
			TipoEvento:  eventoEscolhido,
			Criticidade: criticidade,
			Timestamp:   time.Now(),
		}

		dados, err := json.Marshal(ocorrencia)
		if err != nil {
			log.Printf("[ERRO] Falha ao serializar: %v", err)
			continue
		}
		dados = append(dados, '\n')

		_, err = conexao.Write(dados)
		if err != nil {
			fmt.Printf("\n  [!] Conexão perdida. Reconectando...\n")
			conexao.Close()
			conexao = conectarComRetry(enderecoServidor, sensorID)
			// Tenta reenviar após reconexão
			conexao.Write(dados)
		}

		// Confirmação visual
		baraCrit := strings.Repeat("█", criticidade) + strings.Repeat("░", 5-criticidade)
		fmt.Printf("\n  ✓ ENVIADO → Evento: %-25s | Crit: [%s] %d | Hora: %s\n\n",
			eventoEscolhido,
			baraCrit,
			criticidade,
			time.Now().Format("15:04:05"),
		)
	}
}

// ============================================================
// CONECTAR COM RETRY - Reconexão automática
// ============================================================
func conectarComRetry(endereco, sensorID string) net.Conn {
	for {
		conn, err := net.DialTimeout("tcp", endereco, 3*time.Second)
		if err == nil {
			fmt.Printf("  [✓] Conectado ao gerenciador em %s\n\n", endereco)
			return conn
		}
		fmt.Printf("  [!] Falha ao conectar em %s. Tentando em 3s...\n", endereco)
		time.Sleep(3 * time.Second)
	}
}

// ============================================================
// EXIBIR BANNER - Cabeçalho de apresentação
// ============================================================
func exibirBanner(setor, endereco string) {
	fmt.Println()
	fmt.Println("╔══════════════════════════════════════════════════╗")
	fmt.Println("║       SENSOR MANUAL — ESTREITO DE ORMUZ          ║")
	fmt.Println("╠══════════════════════════════════════════════════╣")
	fmt.Printf( "║  Setor:       %-35s║\n", setor)
	fmt.Printf( "║  Gerenciador: %-35s║\n", endereco)
	fmt.Println("╚══════════════════════════════════════════════════╝")
	fmt.Println()
}

// ============================================================
// EXIBIR MENU - Lista de eventos disponíveis
// ============================================================
func exibirMenu() {
	fmt.Println("┌─────────────────────────────────────────────────┐")
	fmt.Println("│  EVENTOS DISPONÍVEIS                            │")
	fmt.Println("├─────────────────────────────────────────────────┤")
	for i, evento := range eventosDisponiveis {
		crit := "  (criticidade fixa: 1)"
		if evento != "Normal" {
			crit = "  (criticidade: 2-5)"
		}
		fmt.Printf("│  [%d] %-28s%s│\n", i+1, evento, crit)
	}
	fmt.Println("│  [0] Sair                                       │")
	fmt.Println("└─────────────────────────────────────────────────┘")
}