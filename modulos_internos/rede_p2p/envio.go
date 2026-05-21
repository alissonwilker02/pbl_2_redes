package rede_p2p

import (
	"encoding/json"
	"log"
	"net"
	"pbl_2_redes/compartilhado"
	"time"
)

// ============================================================
// BROADCAST P2P - Envia mensagens para todos os setores vizinhos
// ============================================================
// Esta função implementa a comunicação peer-to-peer entre os setores (brokers)
// no sistema distribuído do Estreito de Ormuz.
//
// Como não há servidor central, os setores precisam se comunicar diretamente
// para coordenar a alocação de drones, compartilhar status e sincronizar relógios.
//
// Parâmetros:
//   - msg: MensagemP2P a ser enviada (pode ser REQUEST, REPLY, RELEASE, etc.)
//   - vizinhos: Lista de endereços (IP:porta) de todos os outros setores
//
// BroadcastP2P continua igual, mas agora recebe os endereços reais do main.go
func BroadcastP2P(msg compartilhado.MensagemP2P, vizinhos []string) {
	// Serializa a mensagem para JSON - formato padronizado de comunicação
	dadosJSON, _ := json.Marshal(msg)
	dadosJSON = append(dadosJSON, '\n') // Adiciona newline como delimitador

	// Para cada vizinho (setor), envia a mensagem em uma goroutine separada
	// Isso permite envios paralelos, tornando o sistema mais eficiente
	// e resiliente - um vizinho lento não bloqueia os outros
	for _, vizinho := range vizinhos {
		// Goroutine anônima para envio assíncrono
		// Importante: captura o valor de 'endereco' por parâmetro para evitar
		// problemas de closure (cada iteração do loop tem seu próprio endereço)
		go func(endereco string) {
			// DialTimeout com 2 segundos - conexão rápida para não travar
			// Se o vizinho estiver fora do ar (broker caiu, rede instável),
			// a função retorna erro após 2s em vez de ficar bloqueada indefinidamente
			conexao, erro := net.DialTimeout("tcp", endereco, 2*time.Second)
			if erro != nil {
				// Log de aviso - falha ao comunicar com vizinho
				// No sistema do Estreito de Ormuz, comunicação instável é esperada
				// O sistema deve continuar funcionando mesmo com falhas parciais
				log.Printf("[AVISO P2P] Vizinho %s inatingível.", endereco)
				return
			}
			defer conexao.Close()

			// Envia a mensagem serializada
			_, erroWrite := conexao.Write(dadosJSON)
			if erroWrite == nil {
				// Comentei esse Print para não poluir muito a tela depois que testarmos
				// fmt.Printf("[P2P OUT] Enviado '%s' para %s\n", msg.TipoMensagem, endereco)
			}
			// Se erroWrite != nil, simplesmente ignora - o vizinho pode ter
			// desconectado após o Dial mas antes do Write
		}(vizinho) // Passa 'vizinho' como parâmetro para evitar race condition
	}
}

// ============================================================
// PRINCIPAIS TIPOS DE MENSAGEM P2P USADOS NO SISTEMA:
// ============================================================
//
// 1. "REQUEST" - Um setor solicita acesso exclusivo a um drone
//    Quando um setor precisa alocar um drone, envia REQUEST para TODOS os outros setores
//    Contém: prioridade, relógio de Lamport, setor_origem
//
// 2. "REPLY" - Resposta positiva a um REQUEST
//    Um setor envia REPLY quando não está usando o drone ou quando recebe um REQUEST
//    com prioridade MAIOR que sua própria requisição pendente
//
// 3. "RELEASE" - Libera o drone após uso
//    Quando um setor termina de usar o drone, envia RELEASE para todos
//    Os outros setores então reavaliam suas filas de pendentes e enviam REPLYs
//
// 4. "STATUS" - Atualização de estado de drone
//    Quando um drone muda de status (DISPONIVEL -> EM_MISSAO -> ABATIDO)
//    O setor que gerenciou a mudança notifica todos os outros para manter
//    as tabelas replicadas sincronizadas
//
// 5. "HEARTBEAT" - Verificação de setores vivos
//    Usado para detectar falhas (brokers que caíram) e remover da lista de vizinhos
