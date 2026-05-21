package rede_p2p

import (
	"bufio"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"pbl_2_redes/compartilhado"
	"pbl_2_redes/modulos_internos/exclusao_mutua"
	"pbl_2_redes/modulos_internos/servidor_local"
)

// ============================================================
// INICIAR SERVIDOR P2P - Servidor para comunicação entre setores
// ============================================================
// Cada setor (broker) abre um servidor TCP dedicado para se comunicar
// com os outros setores da malha peer-to-peer.
//
// Diferente do servidor_local (que recebe dados de sensores), este servidor
// lida exclusivamente com tráfego entre gerenciadores: REQUEST, REPLY, RELEASE,
// STATUS_DRONE, HEARTBEAT, etc.
func IniciarServidorP2P(porta string, estado *servidor_local.EstadoGerenciador, vizinhos []string) {
	// Abre socket TCP na porta especificada (ex: ":5001")
	listener, erro := net.Listen("tcp", porta)
	if erro != nil {
		log.Fatalf("[ERRO] Falha ao abrir porta P2P %s: %v", porta, erro)
	}
	defer listener.Close()

	fmt.Printf(">>> [MALHA P2P] Setor %s aguardando vizinhos na porta %s...\n", estado.Setor, porta)

	// Loop principal: aceita conexões de outros setores indefinidamente
	for {
		conexao, erro := listener.Accept()
		if erro != nil {
			log.Printf("[ERRO P2P] Falha de conexão: %v", erro)
			continue
		}
		// Cada conexão de vizinho roda em sua própria goroutine
		go processarMensagemVizinho(conexao, estado, vizinhos)
	}
}

// ============================================================
// PROCESSAR MENSAGEM VIZINHO - Handler de mensagens P2P
// ============================================================
// Função que processa mensagens recebidas de outros setores.
// Executa em goroutine separada para cada conexão.
//
// Parâmetros:
//   - conexao: Conexão TCP com o setor vizinho
//   - estado: Estado completo do setor (fila, mutex, tabela drones)
//   - vizinhos: Lista de todos os setores (para rebroadcast)
func processarMensagemVizinho(
	conexao net.Conn,
	estado *servidor_local.EstadoGerenciador,
	vizinhos []string,
) {
	defer conexao.Close()
	leitor := bufio.NewReader(conexao)

	// Loop contínuo lendo mensagens deste vizinho
	for {
		// Lê até newline (cada mensagem é um JSON terminado com \n)
		bytesRecebidos, erro := leitor.ReadBytes('\n')
		if erro != nil {
			return // Vizinho desconectou ou conexão perdida
		}

		// Decodifica JSON para estrutura MensagemP2P
		var msg compartilhado.MensagemP2P
		if erro := json.Unmarshal(bytesRecebidos, &msg); erro != nil {
			continue // Pula mensagem malformada
		}

		// ==========================================
		// SINCRONIZAÇÃO DE RELÓGIO DE LAMPORT
		// ==========================================
		// A cada mensagem recebida, atualiza o relógio lógico
		// Isso permite ordenar eventos causalmente no sistema distribuído
		// Regra: relogio_local = max(relogio_local, relogio_mensagem) + 1
		estado.SincronizarRelogio(msg.Relogio)

		// ==========================================
		// ROTEAMENTO POR TIPO DE MENSAGEM
		// ==========================================
		switch msg.TipoMensagem {

		// ----------------------------------------------------
		// REQUEST - Requisição de acesso exclusivo ao drone
		// ----------------------------------------------------
		// Um setor quer alocar um drone. Ativa o algoritmo
		// de exclusão mútua distribuída (Ricart-Agrawala)
		case "REQUEST":
			fmt.Printf("\n[P2P IN] REQUEST de Setor %s (Relógio: %d)\n",
				msg.SetorOrigem, msg.Relogio)
			exclusaomutua.ProcessarRequest(msg, estado, vizinhos)

		// ----------------------------------------------------
		// REPLY - Permissão concedida para usar o drone
		// ----------------------------------------------------
		// Resposta positiva a um REQUEST anterior.
		// Quando um setor recebe REPLY de TODOS os vizinhos,
		// pode entrar na região crítica (alocar o drone)
		case "REPLY":
			fmt.Printf("\n[P2P IN] REPLY de Setor %s (Relógio: %d)\n",
				msg.SetorOrigem, msg.Relogio)
			exclusaomutua.ProcessarReply(estado)

		// ----------------------------------------------------
		// HELLO_VIZINHO - Heartbeat/Teste de conectividade
		// ----------------------------------------------------
		// Usado para verificar quais setores estão vivos.
		case "HELLO_VIZINHO":
			fmt.Printf("\n[P2P IN] HELLO de %s (Relógio Sincronizado: %d)\n",
				msg.SetorOrigem, estado.RelogioLamport)

		// ----------------------------------------------------
		// STATUS_DRONE - Atualização do estado de um drone
		// ----------------------------------------------------
		// Quando um drone muda de status (DISPONIVEL -> EM_MISSAO),
		// a malha é notificada para sincronizar as tabelas replicadas.
		case "STATUS_DRONE":
			var statusDrone compartilhado.StatusDrone
			json.Unmarshal([]byte(msg.Payload), &statusDrone)

			// Atualiza a tabela local de drones com lock para segurança
			estado.Mu.Lock()
			statusAnterior := ""
			if droneExistente, ok := estado.TabelaDrones[statusDrone.IDDrone]; ok {
				statusAnterior = droneExistente.Status

				// =========================================================
				// CORREÇÃO E PROTEÇÃO DE ESTADO (REQUISITO 3)
				// =========================================================
				// Evita que pacotes de status parciais decodificados limpem o 
				// setor associado. Se o drone reportar que está em missão mas 
				// o payload vier sem setor (comportamento nativo do drone), 
				// preservamos o setor que o Broker definiu no shadowing local.
				if statusDrone.Status == "EM_MISSAO" && statusDrone.Setor == "" {
					statusDrone.Setor = droneExistente.Setor
				} else if statusDrone.Status == "DISPONIVEL" {
					// Quando o drone retorna à base e fica livre, limpamos o 
					// mapeamento de setor para liberar a área para novos monitoramentos.
					statusDrone.Setor = ""
				}
			}
			
			estado.TabelaDrones[statusDrone.IDDrone] = statusDrone
			estado.Mu.Unlock()

			servidor_local.LogarEstado(estado, fmt.Sprintf("Drone %s → %s",
    		statusDrone.IDDrone, statusDrone.Status))

			// CORREÇÃO 2: Re-broadcast P2P para consistência eventual (Gossip)
			// ===============================================================
			// Se for uma mudança de status nova, repassa para outros vizinhos.
			// Isso implementa um protocolo de DISSEMINAÇÃO / GOSSIP:
			// - A informação se espalha de forma confiável pela malha P2P.
			// - Evita pontos únicos de falha e tolera perda de pacotes transientes.
			if statusAnterior != statusDrone.Status {
				fmt.Printf("\n[MALHA P2P] Atualização de Estado: %s agora está %s (Disseminando via Gossip)\n",
					statusDrone.IDDrone, statusDrone.Status)

				// Reenvia a mesma mensagem para todos os vizinhos
				// Cada mensagem é repassada no máximo N vezes evitando loops infinitos
				exclusaomutua.BroadcastParaVizinhos(msg, vizinhos, estado)
			}

		// ----------------------------------------------------
		// MENSAGEM DESCONHECIDA - Fallback
		// ----------------------------------------------------
		default:
			fmt.Printf("\n[P2P IN] Mensagem desconhecida '%s' de %s\n",
				msg.TipoMensagem, msg.SetorOrigem)
		}
	}
}