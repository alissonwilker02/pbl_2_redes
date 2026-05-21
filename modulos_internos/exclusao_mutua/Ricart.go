package exclusaomutua

// =============================================================================
// RICART-AGRAWALA — Exclusão Mútua Distribuída
// =============================================================================
//
// O algoritmo garante que apenas UM gerenciador por vez possa despachar um
// drone. Funciona assim:
//
//   RELEASED → o gerenciador não quer nenhum drone no momento.
//   WANTED   → o gerenciador quer um drone e está esperando permissão de todos.
//   HELD     → o gerenciador tem permissão e está usando o recurso (despachando).
//
// Fluxo completo:
//   1. Gerenciador X quer um drone → envia REQUEST para todos os vizinhos.
//   2. Cada vizinho Y que receber o REQUEST:
//        - Se Y está RELEASED  → responde REPLY imediatamente.
//        - Se Y está HELD      → guarda X na fila, responde depois.
//        - Se Y está WANTED    → compara relógios:
//             se X tem prioridade maior (relógio menor, ou igual com ID menor)
//             → responde REPLY imediatamente.
//             caso contrário → guarda X na fila.
//   3. Quando X receber REPLY de TODOS os vizinhos → entra em HELD, despacha.
//   4. Ao terminar → volta para RELEASED e envia REPLY para todos na fila.
//
// =============================================================================

import (
	"fmt"
	"pbl_2_redes/compartilhado"
	"pbl_2_redes/modulos_internos/servidor_local"
	"time"
)

// ============================================================
// SOLICITAR ACESSO RECURSO - Entrada para querer um drone
// ============================================================
// Chamado quando o gerenciador quer despachar um drone (ex: após receber
// uma ocorrência crítica ou quando um drone fica livre).
//
// Muda o estado para WANTED e dispara os REQUESTs para os vizinhos.
// Retorna o relógio usado na requisição (para comparação posterior).
//
// ATENDE AO REQUISITO: "priorize a liberação dos drones para o setor que
// enviou a requisição primeiro" - via relógio de Lamport
func SolicitarAcessoRecurso(estado *servidor_local.EstadoGerenciador, vizinhos []string) int {
	estado.Mu.Lock()

	// Incrementa o relógio antes de enviar (regra do Lamport)
	// Cada evento local incrementa o relógio lógico
	estado.RelogioLamport++
	relogioRequisicao := estado.RelogioLamport

	// Guarda o relógio da nossa própria requisição para comparar com os outros
	// Isso permite desempatar quem pediu primeiro em caso de concorrência
	estado.RelogioMinhaRequisicao = relogioRequisicao

	// Precisa de resposta de todos os vizinhos (exclusão mútua)
	// Se temos 2 vizinhos (setores B e C), precisamos de 2 REPLYs
	estado.RespostasAguardadas = len(vizinhos)
	estado.EstadoMutex = "WANTED"

	fmt.Printf("\n[MUTEX] Setor %s → WANTED | Relógio: %d | Aguardando %d REPLY(s)\n",
		estado.Setor, relogioRequisicao, len(vizinhos))

	estado.Mu.Unlock()

	// Monta a mensagem de REQUEST
	// A prioridade usa o relógio - quanto menor o número, mais "antiga" a requisição
	msg := compartilhado.MensagemP2P{
		TipoMensagem: "REQUEST",
		SetorOrigem:  estado.Setor,
		Relogio:      relogioRequisicao,
		Prioridade:   relogioRequisicao, // usamos o relógio como prioridade
	}

	// Envia para todos os vizinhos (importado do pacote rede_p2p no escuta.go)
	// Esta função já trata falhas (timeout + reply implícito)
	BroadcastParaVizinhos(msg, vizinhos, estado)

	return relogioRequisicao
}

// ============================================================
// AGUARDAR PERMISSAO - Bloqueia até ter REPLY de todos os setores
// ============================================================
// Usa canal de comunicação com Timeout de segurança para evitar
// deadlocks caso um vizinho morra após receber o REQUEST.
func AguardarPermissao(estado *servidor_local.EstadoGerenciador) {
	// Checagem inicial rápida - otimização para caso já esteja tudo pronto
	estado.Mu.Lock()
	pronto := (estado.RespostasAguardadas <= 0 && estado.EstadoMutex == "WANTED")
	if pronto {
		estado.EstadoMutex = "HELD"
	}
	estado.Mu.Unlock()

	if pronto {
		fmt.Printf("\n[MUTEX] Setor %s → HELD (permissão imediata!)\n", estado.Setor)
		return
	}

	// Loop com select para aguardar permissão ou timeout
	for {
		select {
		case <-estado.CanalPermissao: // Bloqueia até alguém escrever no canal
			estado.Mu.Lock()
			if estado.RespostasAguardadas <= 0 && estado.EstadoMutex == "WANTED" {
				estado.EstadoMutex = "HELD"
				fmt.Printf("\n[MUTEX] Setor %s → HELD (permissão concedida por todos!)\n", estado.Setor)
				estado.Mu.Unlock()
				return
			}
			estado.Mu.Unlock()

		case <-time.After(5 * time.Second): // TIMEOUT DE SEGURANÇA
			// Se passou 5 segundos e não recebemos REPLY, algum vizinho pode ter
			// morrido DEPOIS de receber nosso REQUEST e o P2P dial não pegou.
			estado.Mu.Lock()
			if estado.EstadoMutex == "WANTED" {
				fmt.Printf("\n[AVISO MUTEX] Timeout de 5s aguardando REPLY. Forçando entrada para evitar deadlock devido a possível queda de vizinho silenciada.\n")
				estado.EstadoMutex = "HELD"
				estado.RespostasAguardadas = 0 // Zera para limpar a espera fantasma
				estado.Mu.Unlock()
				return
			}
			estado.Mu.Unlock()
		}
	}
}

// ============================================================
// LIBERAR RECURSO - Finaliza uso do drone e libera pendentes
// ============================================================
// Chamado após o drone ser despachado (ou missão concluída/falha).
// Volta para RELEASED e responde a todos que estavam na fila esperando.
//
// ATENDE AO REQUISITO: "busque atender a todas as requisições de drones,
// ainda que seja necessário mantê-la em fila distribuída"
func LiberarRecurso(estado *servidor_local.EstadoGerenciador, vizinhos []string) {
	estado.Mu.Lock()

	estado.EstadoMutex = "RELEASED"
	estado.RelogioMinhaRequisicao = 0

	// Copia a fila de pendentes para processar fora do lock
	// Isso evita deadlock (enviar mensagens enquanto seguramos o lock)
	filaPendente := make([]servidor_local.RequisicaoPendente, len(estado.RequisicoesPendentes))
	copy(filaPendente, estado.RequisicoesPendentes)
	estado.RequisicoesPendentes = estado.RequisicoesPendentes[:0] // limpa a fila

	fmt.Printf("\n[MUTEX] Setor %s → RELEASED | Enviando REPLY para %d setor(es) na fila\n",
		estado.Setor, len(filaPendente))

	estado.Mu.Unlock()

	// Responde para todos que estavam aguardando
	// Isso implementa justiça: quem esperou mais (ou tem maior prioridade)
	// será atendido assim que o recurso ficar livre
	for _, requisicao := range filaPendente {
		// Precisamos do endereço do vizinho — buscamos pelo SetorOrigem
		enderecoVizinho := buscarEnderecoVizinho(requisicao.SetorOrigem, vizinhos)
		if enderecoVizinho == "" {
			fmt.Printf("[AVISO MUTEX] Não encontrei endereço para o setor %s na lista de vizinhos\n",
				requisicao.SetorOrigem)
			continue
		}

		estado.Mu.Lock()
		estado.RelogioLamport++
		relogioAtual := estado.RelogioLamport
		estado.Mu.Unlock()

		reply := compartilhado.MensagemP2P{
			TipoMensagem: "REPLY",
			SetorOrigem:  estado.Setor,
			SetorDestino: requisicao.SetorOrigem,
			Relogio:      relogioAtual,
		}

		EnviarParaVizinho(reply, enderecoVizinho)
		fmt.Printf("[MUTEX] REPLY enviado para Setor %s\n", requisicao.SetorOrigem)
	}
}

// ============================================================
// PROCESSAR REQUEST - Lida com requisições de outros setores
// ============================================================
// Chamado quando chega uma mensagem REQUEST de outro gerenciador.
// Decide se responde REPLY agora ou guarda na fila.
//
// Esta função implementa o coração do algoritmo de Ricart-Agrawala
func ProcessarRequest(
	msg compartilhado.MensagemP2P,
	estado *servidor_local.EstadoGerenciador,
	vizinhos []string,
) {
	estado.Mu.Lock()
	defer estado.Mu.Unlock()

	deveResponderAgora := false

	switch estado.EstadoMutex {

	case "RELEASED":
		// Não queremos o recurso (não estamos tentando alocar drone)
		// Podemos responder imediatamente
		deveResponderAgora = true

	case "HELD":
		// Estamos usando o recurso (já temos um drone alocado)
		// Não podemos responder agora - guarda na fila para responder depois
		deveResponderAgora = false

	case "WANTED":
		// Também queremos o recurso (estamos tentando alocar um drone)
		// Desempate pelo relógio de Lamport (ordem de chegada)
		//
		// Quem tem relógio MENOR tem prioridade (chegou primeiro no sistema)
		// Em caso de empate, desempate pelo ID do setor (ordem lexicográfica)
		// Isso garante que sempre haverá um vencedor (total ordering)
		outroTemPrioridade := msg.Relogio < estado.RelogioMinhaRequisicao ||
			(msg.Relogio == estado.RelogioMinhaRequisicao && msg.SetorOrigem < estado.Setor)

		deveResponderAgora = outroTemPrioridade
	}

	if deveResponderAgora {
		// Responde REPLY diretamente (dá permissão para o outro setor)
		estado.RelogioLamport++
		relogioAtual := estado.RelogioLamport

		enderecoVizinho := buscarEnderecoVizinho(msg.SetorOrigem, vizinhos)
		if enderecoVizinho != "" {
			reply := compartilhado.MensagemP2P{
				TipoMensagem: "REPLY",
				SetorOrigem:  estado.Setor,
				SetorDestino: msg.SetorOrigem,
				Relogio:      relogioAtual,
			}
			// Envia em goroutine para não bloquear o lock
			// Isso permite que outras mensagens sejam processadas enquanto enviamos
			go EnviarParaVizinho(reply, enderecoVizinho)
			fmt.Printf("[MUTEX] REPLY imediato enviado para Setor %s (estado: %s)\n",
				msg.SetorOrigem, estado.EstadoMutex)
		}
	} else {
		// Guarda na fila para responder quando sairmos da seção crítica
		// O outro setor terá que esperar até que liberemos o recurso
		pendente := servidor_local.RequisicaoPendente{
			SetorOrigem: msg.SetorOrigem,
			Relogio:     msg.Relogio,
			Prioridade:  msg.Prioridade,
		}
		estado.RequisicoesPendentes = append(estado.RequisicoesPendentes, pendente)
		fmt.Printf("[MUTEX] REQUEST de %s guardado na fila (nossa prioridade é maior)\n",
			msg.SetorOrigem)
	}
}

// ============================================================
// PROCESSAR REPLY - Contabiliza respostas recebidas
// ============================================================
// Chamado quando recebemos um REPLY de um vizinho.
// Decrementa o contador. Quando chegar a zero, sinaliza que temos permissão.
func ProcessarReply(estado *servidor_local.EstadoGerenciador) {
	estado.Mu.Lock()
	defer estado.Mu.Unlock()

	if estado.RespostasAguardadas > 0 {
		estado.RespostasAguardadas--
		fmt.Printf("[MUTEX] REPLY recebido. Faltam %d resposta(s) para Setor %s entrar em HELD\n",
			estado.RespostasAguardadas, estado.Setor)
	}

	// Se chegou a zero E ainda estamos WANTED → sinaliza AguardarPermissao
	// O select com default evita bloqueio se o canal já tiver sinal
	if estado.RespostasAguardadas <= 0 && estado.EstadoMutex == "WANTED" {
		// Manda no canal sem bloquear (non-blocking send)
		// O buffer 1 garante que não perderemos o sinal
		select {
		case estado.CanalPermissao <- struct{}{}:
		default:
		}
	}
}

// =============================================================================
// FUNÇÕES AUXILIARES DE ENDEREÇAMENTO
// =============================================================================

// buscarEnderecoVizinho encontra o endereço IP:Porta de um setor pelo nome.
// Os vizinhos são passados como "IP:Porta" e os nomes dos setores são mapeados
// pela ordem: vizinhos[0] = primeiro vizinho, vizinhos[1] = segundo vizinho.
// Para resolver o nome → endereço, usamos o mapa global configurado no main.go.
func buscarEnderecoVizinho(setorDestino string, vizinhos []string) string {
	// O mapeamento setor→endereço é feito via variável de ambiente ou configuração.
	// Aqui buscamos no mapa global que é preenchido pelo main.go na inicialização.
	if endereco, ok := MapaSetorParaEndereco[setorDestino]; ok {
		return endereco
	}
	return ""
}

// ============================================================
// MAPA SETOR PARA ENDEREÇO - Configuração global
// ============================================================
// MapaSetorParaEndereco é preenchido pelo main.go durante a inicialização.
// Exemplo: {"B": "192.168.1.2:6001", "C": "192.168.1.3:6002"}
//
// Isso permite converter nomes lógicos de setores ("A", "B", "C") em
// endereços de rede reais para comunicação TCP.
var MapaSetorParaEndereco = make(map[string]string)
