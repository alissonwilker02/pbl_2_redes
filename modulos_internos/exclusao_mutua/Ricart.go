package exclusaomutua

// =============================================================================
// RICART-AGRAWALA — Exclusão Mútua com Fila de Pendentes Ordenada
// =============================================================================

//   RequisicaoPendente guarda também TimestampOcorrencia, permitindo
//          ordenação correta da fila de pendentes.
//
//    pendentes são ordenados por (crit desc, timestamp asc, ID asc)
//          e REPLY é enviado apenas para o PRIMEIRO da fila ordenada.
//          Os demais continuam esperando. Quando esse liberar, o próximo
//          recebe REPLY.
//
// Lógica de desempate (3 níveis, aplicada tanto no ProcessarRequest
// quanto na ordenação da fila de pendentes em LiberarRecurso):
//   1. Maior criticidade vence
//   2. Mesma crit - timestamp mais antigo vence 
// =============================================================================

import (
	"fmt"
	"pbl_2_redes/compartilhado"
	"pbl_2_redes/modulos_internos/servidor_local"
	"time"
)

// ============================================================
// SOLICITAR ACESSO RECURSO
// ============================================================
func SolicitarAcessoRecurso(
	estado *servidor_local.EstadoGerenciador,
	vizinhos []string,
	criticidade int,
	timestampOcorrencia time.Time,
) int {
	estado.Mu.Lock()

	estado.RelogioLamport++
	relogioRequisicao := estado.RelogioLamport
	estado.RelogioMinhaRequisicao = relogioRequisicao
	estado.CritMinhaRequisicao = criticidade
	estado.TimestampMinhaRequisicao = timestampOcorrencia

	estado.RespostasAguardadas = len(vizinhos)
	estado.EstadoMutex = "WANTED"

	fmt.Printf("\n[MUTEX] Setor %s → WANTED | Crit: %d | Detecção: %s | Rel: %d\n",
		estado.Setor, criticidade,
		timestampOcorrencia.Format("15:04:05.000"),
		relogioRequisicao)

	estado.Mu.Unlock()

	msg := compartilhado.MensagemP2P{
		TipoMensagem:        "REQUEST",
		SetorOrigem:         estado.Setor,
		Relogio:             relogioRequisicao,
		Prioridade:          criticidade,
		TimestampOcorrencia: timestampOcorrencia,
	}

	BroadcastParaVizinhos(msg, vizinhos, estado)
	return relogioRequisicao
}

// ============================================================
// AGUARDAR PERMISSAO
// ============================================================
func AguardarPermissao(estado *servidor_local.EstadoGerenciador) {
	estado.Mu.Lock()
	pronto := estado.RespostasAguardadas == 0 && estado.EstadoMutex == "WANTED"
	if pronto {
		estado.EstadoMutex = "HELD"
	}
	estado.Mu.Unlock()

	if pronto {
		fmt.Printf("\n[MUTEX] Setor %s → HELD (permissão imediata)\n", estado.Setor)
		return
	}

	for {
		<-estado.CanalPermissao
		estado.Mu.Lock()
		if estado.RespostasAguardadas == 0 && estado.EstadoMutex == "WANTED" {
			estado.EstadoMutex = "HELD"
			fmt.Printf("\n[MUTEX] Setor %s → HELD (todos REPLYs recebidos)\n", estado.Setor)
			estado.Mu.Unlock()
			return
		}
		estado.Mu.Unlock()
	}
}

// ============================================================
// LIBERAR RECURSO — CORREÇÃO DO BUG 2
// ============================================================
//   1. Ordena a fila de pendentes por (crit desc -> timestamp asc)
//   2. Envia REPLY apenas para o maior prioridade
//   3. Os demais ficam na fila — receberão REPLY quando o vencedor liberar
//
func LiberarRecurso(estado *servidor_local.EstadoGerenciador, vizinhos []string) {
	estado.Mu.Lock()

	estado.EstadoMutex = "RELEASED"
	estado.RelogioMinhaRequisicao = 0
	estado.CritMinhaRequisicao = 0
	estado.TimestampMinhaRequisicao = time.Time{}

	// Ordena os pendentes antes de decidir quem recebe REPLY
	servidor_local.OrdenarPendentes(estado.RequisicoesPendentes)

	// Copia a fila ordenada e limpa o estado
	filaPendente := make([]servidor_local.RequisicaoPendente, len(estado.RequisicoesPendentes))
	copy(filaPendente, estado.RequisicoesPendentes)
	estado.RequisicoesPendentes = estado.RequisicoesPendentes[:0]

	estado.Mu.Unlock()

	if len(filaPendente) == 0 {
		fmt.Printf("\n[MUTEX] Setor %s → RELEASED | Nenhum pendente na fila\n", estado.Setor)
		return
	}

	// ── ENVIA REPLY APENAS PARA O PRIMEIRO (maior prioridade) ────────────
	// Os outros continuam aguardando. Quando o vencedor liberar o recurso,
	// ele enviará REPLY para o próximo da fila, e assim por diante.
	vencedor := filaPendente[0]
	restantes := filaPendente[1:]

	fmt.Printf("\n[MUTEX] Setor %s → RELEASED | REPLY → Setor %s (crit %d, ts %s) | %d aguardando\n",
		estado.Setor,
		vencedor.SetorOrigem,
		vencedor.Criticidade,
		vencedor.TimestampOcorrencia.Format("15:04:05.000"),
		len(restantes))

	// Devolve os restantes para a fila — eles continuam esperando
	if len(restantes) > 0 {
		estado.Mu.Lock()
		// Reinsere na frente da fila 
		estado.RequisicoesPendentes = append(restantes, estado.RequisicoesPendentes...)
		estado.Mu.Unlock()

		for i, r := range restantes {
			fmt.Printf("[MUTEX]   %dº aguardando: Setor %s (crit %d, ts %s)\n",
				i+2, r.SetorOrigem, r.Criticidade,
				r.TimestampOcorrencia.Format("15:04:05.000"))
		}
	}

	// Incrementa relógio e envia REPLY para o vencedor
	estado.Mu.Lock()
	estado.RelogioLamport++
	relogioAtual := estado.RelogioLamport
	estado.Mu.Unlock()

	enderecoVizinho := buscarEnderecoVizinho(vencedor.SetorOrigem, vizinhos)
	if enderecoVizinho == "" {
		fmt.Printf("[AVISO MUTEX] Endereço não encontrado para setor %s\n", vencedor.SetorOrigem)
		return
	}

	reply := compartilhado.MensagemP2P{
		TipoMensagem: "REPLY",
		SetorOrigem:  estado.Setor,
		SetorDestino: vencedor.SetorOrigem,
		Relogio:      relogioAtual,
	}

	EnviarParaVizinho(reply, enderecoVizinho)
}

// ============================================================
// PROCESSAR REQUEST 
// ============================================================
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

		//  se A tem ocorrência na fila com prioridade >= à do
		// REQUEST de B, A trata o caso como se estivesse em WANTED —
		// compara timestamps e só cede se B realmente tiver prioridade.
		topoFila := estado.TopoFilaSemLock()
		if topoFila != nil && topoFila.TipoEvento != "Normal" {
			// Há ocorrência local relevante aguardando — aplica mesma
			// lógica de desempate do caso WANTED
			critOutro := msg.Prioridade
			critNossa := topoFila.Criticidade
			tsOutro := msg.TimestampOcorrencia
			tsNossa := topoFila.Timestamp

			var outroTemPrioridade bool
			var motivo string

			if critOutro != critNossa {
				outroTemPrioridade = critOutro > critNossa
				motivo = fmt.Sprintf("criticidade %d vs %d (fila local)", critOutro, critNossa)
			} else if !tsOutro.Equal(tsNossa) {
				outroTemPrioridade = tsOutro.Before(tsNossa)
				motivo = fmt.Sprintf("timestamp %s vs %s (fila local)",
					tsOutro.Format("15:04:05.000"),
					tsNossa.Format("15:04:05.000"))
			} else {
				outroTemPrioridade = msg.SetorOrigem < estado.Setor
				motivo = fmt.Sprintf("ID %s vs %s (fila local)", msg.SetorOrigem, estado.Setor)
			}

			deveResponderAgora = outroTemPrioridade
			vencedor := estado.Setor
			if outroTemPrioridade {
				vencedor = msg.SetorOrigem
			}
			fmt.Printf("[MUTEX] RELEASED com fila pendente — disputa (%s) → Setor %s vence\n", motivo, vencedor)
		} else {
			// Nenhuma ocorrência local relevante — cede normalmente
			deveResponderAgora = true
		}

	case "HELD":
		deveResponderAgora = false

	case "WANTED":
		critOutro := msg.Prioridade
		critNossa := estado.CritMinhaRequisicao
		tsOutro := msg.TimestampOcorrencia
		tsNossa := estado.TimestampMinhaRequisicao

		var outroTemPrioridade bool
		var motivo string

		if critOutro != critNossa {
			// Nível 1: maior criticidade vence
			outroTemPrioridade = critOutro > critNossa
			motivo = fmt.Sprintf("criticidade %d vs %d", critOutro, critNossa)

		} else if !tsOutro.Equal(tsNossa) {
			// Nível 2: mesma criticidade - sensor mais antigo vence
			outroTemPrioridade = tsOutro.Before(tsNossa)
			motivo = fmt.Sprintf("timestamp %s vs %s",
				tsOutro.Format("15:04:05.000"),
				tsNossa.Format("15:04:05.000"))

		} else {
			// Nível 3: mesmo timestamp - menor ID 
			outroTemPrioridade = msg.SetorOrigem < estado.Setor
			motivo = fmt.Sprintf("ID %s vs %s", msg.SetorOrigem, estado.Setor)
		}

		deveResponderAgora = outroTemPrioridade

		vencedor := estado.Setor
		if outroTemPrioridade {
			vencedor = msg.SetorOrigem
		}
		fmt.Printf("[MUTEX] Disputa (%s) → Setor %s vence\n", motivo, vencedor)
	}

	if deveResponderAgora {
		estado.RelogioLamport++
		enderecoVizinho := buscarEnderecoVizinho(msg.SetorOrigem, vizinhos)
		if enderecoVizinho != "" {
			reply := compartilhado.MensagemP2P{
				TipoMensagem: "REPLY",
				SetorOrigem:  estado.Setor,
				SetorDestino: msg.SetorOrigem,
				Relogio:      estado.RelogioLamport,
			}
			go EnviarParaVizinho(reply, enderecoVizinho)
			fmt.Printf("[MUTEX] REPLY imediato → Setor %s (estado local: %s)\n",
				msg.SetorOrigem, estado.EstadoMutex)
		}
	} else {
		//guarda TimestampOcorrencia no pendente
		pendente := servidor_local.RequisicaoPendente{
			SetorOrigem:         msg.SetorOrigem,
			Relogio:             msg.Relogio,
			Criticidade:         msg.Prioridade,
			TimestampOcorrencia: msg.TimestampOcorrencia, // BUG 1 corrigido
		}
		estado.RequisicoesPendentes = append(estado.RequisicoesPendentes, pendente)
		fmt.Printf("[MUTEX] REQUEST de %s (crit %d, ts %s) enfileirado\n",
			msg.SetorOrigem, msg.Prioridade,
			msg.TimestampOcorrencia.Format("15:04:05.000"))
	}
}

// ============================================================
// PROCESSAR REPLY
// ============================================================
func ProcessarReply(estado *servidor_local.EstadoGerenciador) {
	estado.Mu.Lock()
	defer estado.Mu.Unlock()

	if estado.RespostasAguardadas > 0 {
		estado.RespostasAguardadas--
		fmt.Printf("[MUTEX] REPLY recebido — faltam %d para Setor %s → HELD\n",
			estado.RespostasAguardadas, estado.Setor)
	}

	if estado.RespostasAguardadas == 0 && estado.EstadoMutex == "WANTED" {
		select {
		case estado.CanalPermissao <- struct{}{}:
		default:
		}
	}
}

// ============================================================
// AUXILIARES
// ============================================================
func buscarEnderecoVizinho(setorDestino string, _ []string) string {
	if endereco, ok := MapaSetorParaEndereco[setorDestino]; ok {
		return endereco
	}
	return ""
}

var MapaSetorParaEndereco = make(map[string]string)