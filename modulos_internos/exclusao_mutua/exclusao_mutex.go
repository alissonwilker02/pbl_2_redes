package exclusaomutua

import (
	"encoding/json"
	"log"
	"net"
	"pbl_2_redes/compartilhado"
	"pbl_2_redes/modulos_internos/servidor_local"
	"time"
)

// =============================================================================
// COMUNICAÇÃO P2P - ENVIO DE MENSAGENS E TOLERÂNCIA A FALHAS
// =============================================================================
//
// Este pacote implementa a camada de comunicação peer-to-peer com foco no
// algoritmo de exclusão mútua distribuída (Ricart-Agrawala) e resiliência
// a falhas de rede/equipamentos, conforme exigido pelo problema.

// ============================================================
// BROADCAST PARA VIZINHOS - Envia mensagem para todos os setores
// ============================================================
// Função principal para enviar mensagens P2P (especialmente REQUEST)
// para todos os outros setores da malha.
//
// Adicionamos o parâmetro 'estado' para garantir que, se um vizinho estiver offline,
// o sistema não fique preso esperando uma resposta que nunca virá (deadlock).
//
// Esta função é CRÍTICA para o algoritmo de exclusão mútua pois:
// - Quando um setor quer alocar um drone, ele envia REQUEST para TODOS os outros
// - Precisa receber REPLY de TODOS para prosseguir
// - Se algum vizinho falhar, o sistema não pode travar (tolerância a falhas)
func BroadcastParaVizinhos(msg compartilhado.MensagemP2P, vizinhos []string, estado *servidor_local.EstadoGerenciador) {
	// Serializa a mensagem uma única vez (reutilizada para todos os vizinhos)
	dadosJSON, _ := json.Marshal(msg)
	dadosJSON = append(dadosJSON, '\n')

	// Para cada vizinho (setor), envia a mensagem
	for _, vizinho := range vizinhos {
		// Goroutine separada para cada envio - evita que um vizinho lento
		// ou offline bloqueie o envio para os outros
		go func(endereco string) {
			// Timeout de 2 segundos: se o vizinho não responder, assumimos que falhou
			// Este timeout é essencial em cenários de rede instável como o Estreito de Ormuz
			conexao, erro := net.DialTimeout("tcp", endereco, 2*time.Second)

			if erro != nil {
				log.Printf("[FALHA P2P] Vizinho %s inalcançável (envio de %s). Erro: %v",
					endereco, msg.TipoMensagem, erro)

				// ---------------------------------------------------------
				// LÓGICA DE RESILIÊNCIA - CORNER CASE CRÍTICO
				// ---------------------------------------------------------
				// Se o vizinho caiu (broker destruído/comunicação perdida),
				// chamamos ProcessarReply manualmente.
				//
				// Isso decrementa o contador 'RespostasAguardadas' do estado,
				// permitindo que o setor prossiga mesmo com falhas na rede.
				//
				// POR QUE ISSO É NECESSÁRIO:
				// O algoritmo de Ricart-Agrawala tradicional espera REPLY de TODOS.
				// Se um vizinho falhar permanentemente, o sistema entraria em DEADLOCK.
				// Esta correção trata o silêncio do vizinho como um "REPLY implícito".
				//
				// ATENDE AO REQUISITO: "nenhuma solução centralizada e tolerância a falhas"
				if msg.TipoMensagem == "REQUEST" {
					log.Printf("[MUTEX] Ignorando silêncio de %s e assumindo permissão por falha.", endereco)
					ProcessarReply(estado) // Decrementa RespostasAguardadas
				}
				return
			}

			defer conexao.Close()
			conexao.Write(dadosJSON) // Envia a mensagem com sucesso
		}(vizinho)
	}
}

// ============================================================
// ENVIAR PARA VIZINHO - Mensagem direta para um único setor
// ============================================================
// Usado principalmente para enviar REPLY como resposta a um REQUEST específico.
// Diferente do Broadcast, envia apenas para um destinatário.
//
// Exemplo de uso:
//   - Setor B recebe REQUEST do Setor A
//   - B decide que pode dar permissão (não está em HELD ou prioridade menor)
//   - B chama EnviarParaVizinho com REPLY para o endereço de A
func EnviarParaVizinho(msg compartilhado.MensagemP2P, endereco string) {
	// Serializa a mensagem para JSON
	dadosJSON, _ := json.Marshal(msg)
	dadosJSON = append(dadosJSON, '\n')

	// Timeout para evitar que uma tentativa de resposta bloqueie o gerenciador
	// 2 segundos é suficiente para uma comunicação local entre setores
	conexao, erro := net.DialTimeout("tcp", endereco, 2*time.Second)
	if erro != nil {
		// Apenas log de aviso - diferente do Broadcast, não temos um estado
		// para manipular porque quem enviou o REQUEST pode estar offline
		log.Printf("[AVISO P2P] Não foi possível enviar REPLY para %s: %v", endereco, erro)
		return
	}
	defer conexao.Close()
	conexao.Write(dadosJSON) // Envia o REPLY
}

// ============================================================
// RESUMO DO ALGORITMO DE EXCLUSÃO MÚTUA COM TOLERÂNCIA A FALHAS
// ============================================================
//
// 1. Setor quer alocar um drone:
//    - estado.EstadoMutex = "WANTED"
//    - estado.RespostasAguardadas = total_vizinhos
//    - Broadcast REQUEST para todos (via BroadcastParaVizinhos)
//
// 2. Ao receber REQUEST (no processarMensagemVizinho):
//    - Se EstadoMutex == "RELEASED": envia REPLY imediato
//    - Se EstadoMutex == "WANTED" com prioridade menor: envia REPLY imediato
//    - Se EstadoMutex == "HELD" ou "WANTED" com prioridade maior: adiciona à fila de pendentes
//
// 3. Ao receber REPLY (via ProcessarReply):
//    - estado.RespostasAguardadas--
//    - Se RespostasAguardadas == 0: desbloqueia (entra em "HELD")
//    - Pode então alocar o drone com segurança
//
// 4. Tolerância a falhas (CORREÇÃO 2 e 3):
//    - Se um vizinho não responde (timeout + erro), chamamos ProcessarReply
//    - Isso decrementa RespostasAguardadas mesmo sem REPLY real
//    - Sistema prossegue mesmo com falhas de até (N-1) setores
//
// 5. Liberação do drone:
//    - estado.EstadoMutex = "RELEASED"
//    - Envia REPLY para todos os pendentes na fila
//    - Broadcast RELEASE para todos