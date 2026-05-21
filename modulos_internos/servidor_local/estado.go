package servidor_local

import (
	"fmt"
	"pbl_2_redes/compartilhado"
	"sort"
	"sync"
	"time"
)

// ============================================================
// REQUISICAO PENDENTE — fila do Ricart-Agrawala
// ============================================================
// Guarda TODOS os campos necessários para ordenar corretamente
// quem deve receber o próximo REPLY ao liberar o recurso.
//

type RequisicaoPendente struct {
	SetorOrigem         string
	Relogio             int
	Criticidade         int       // criticidade da ocorrência 
	TimestampOcorrencia time.Time // timestamp real da detecção pelo sensor
}

// ============================================================
// MISSAO EM ANDAMENTO
// ============================================================
type MissaoEmAndamento struct {
	DroneID    string
	Ocorrencia compartilhado.Ocorrencia
	IniciadaEm time.Time
}

// ============================================================
// ESTADO GERENCIADOR
// ============================================================
type EstadoGerenciador struct {
	Setor           string
	FilaOcorrencias []compartilhado.Ocorrencia
	RelogioLamport  int

	// === RICART-AGRAWALA ===
	EstadoMutex             string
	RespostasAguardadas     int
	RelogioMinhaRequisicao  int
	CritMinhaRequisicao     int       // criticidade da ocorrência atual
	TimestampMinhaRequisicao time.Time // timestamp da detecção atual

	// Fila de pendentes — ordenada antes de enviar REPLYs
	RequisicoesPendentes []RequisicaoPendente

	CanalPermissao chan struct{}
	Mu             sync.Mutex

	TabelaDrones       map[string]compartilhado.StatusDrone
	MissoesEmAndamento map[string]MissaoEmAndamento
}

func NovoEstado(setor string) *EstadoGerenciador {
	return &EstadoGerenciador{
		Setor:                   setor,
		FilaOcorrencias:         make([]compartilhado.Ocorrencia, 0),
		RelogioLamport:          0,
		EstadoMutex:             "RELEASED",
		RespostasAguardadas:     0,
		RelogioMinhaRequisicao:  0,
		CritMinhaRequisicao:     0,
		TimestampMinhaRequisicao: time.Time{},
		RequisicoesPendentes:    make([]RequisicaoPendente, 0),
		CanalPermissao:          make(chan struct{}, 1),
		TabelaDrones:            make(map[string]compartilhado.StatusDrone),
		MissoesEmAndamento:      make(map[string]MissaoEmAndamento),
	}
}

// ============================================================
// REGISTRAR / CONCLUIR MISSAO
// ============================================================
func (e *EstadoGerenciador) RegistrarMissao(droneID string, oc compartilhado.Ocorrencia) {
	e.Mu.Lock()
	defer e.Mu.Unlock()
	e.MissoesEmAndamento[droneID] = MissaoEmAndamento{
		DroneID:    droneID,
		Ocorrencia: oc,
		IniciadaEm: time.Now(),
	}
}

func (e *EstadoGerenciador) ConcluirMissao(droneID string) {
	e.Mu.Lock()
	defer e.Mu.Unlock()
	delete(e.MissoesEmAndamento, droneID)
}

// ============================================================
// ADICIONAR OCORRENCIA
// ============================================================
func (e *EstadoGerenciador) AdicionarOcorrencia(novaOcorrencia compartilhado.Ocorrencia) {
	e.Mu.Lock()

	e.RelogioLamport++
	e.FilaOcorrencias = append(e.FilaOcorrencias, novaOcorrencia)

	sort.SliceStable(e.FilaOcorrencias, func(i, j int) bool {
		if e.FilaOcorrencias[i].Criticidade != e.FilaOcorrencias[j].Criticidade {
			return e.FilaOcorrencias[i].Criticidade > e.FilaOcorrencias[j].Criticidade
		}
		return e.FilaOcorrencias[i].Timestamp.Before(e.FilaOcorrencias[j].Timestamp)
	})

	filaCopia := make([]compartilhado.Ocorrencia, len(e.FilaOcorrencias))
	copy(filaCopia, e.FilaOcorrencias)
	tabelaCopia := copiarTabela(e.TabelaDrones)
	setor := e.Setor
	mutex := e.EstadoMutex
	relogio := e.RelogioLamport

	e.Mu.Unlock()

	msg := fmt.Sprintf("Nova ocorrência: %s (crit %d) do sensor %s",
		novaOcorrencia.TipoEvento, novaOcorrencia.Criticidade, novaOcorrencia.IDSensor)
	logarEstadoDireto(setor, mutex, relogio, filaCopia, tabelaCopia, msg)
}

// ============================================================
// SINCRONIZAR RELOGIO
// ============================================================
func (e *EstadoGerenciador) SincronizarRelogio(relogioRecebido int) {
	e.Mu.Lock()
	defer e.Mu.Unlock()
	if relogioRecebido > e.RelogioLamport {
		e.RelogioLamport = relogioRecebido
	}
	e.RelogioLamport++
}

// ============================================================
// TOPO FILA — retorna a próxima ocorrência sem removê-la
// ============================================================
// Usado pelo loopDespacho para verificar preempção de prioridade
// após obter o HELD: se chegou algo mais urgente durante a espera
// pelos REPLYs, o ciclo atual devolve a ocorrência e libera o mutex.
func (e *EstadoGerenciador) TopoFila() *compartilhado.Ocorrencia {
	e.Mu.Lock()
	defer e.Mu.Unlock()
	return e.TopoFilaSemLock()
}

// TopoFilaSemLock — versão sem lock, para uso quando Mu já está segurado
// pelo chamador
func (e *EstadoGerenciador) TopoFilaSemLock() *compartilhado.Ocorrencia {
	if len(e.FilaOcorrencias) == 0 {
		return nil
	}
	copia := e.FilaOcorrencias[0]
	return &copia
}

// ============================================================
// PROXIMA OCORRENCIA
// ============================================================
func (e *EstadoGerenciador) ProximaOcorrencia() *compartilhado.Ocorrencia {
	e.Mu.Lock()
	defer e.Mu.Unlock()
	if len(e.FilaOcorrencias) == 0 {
		return nil
	}
	oc := e.FilaOcorrencias[0]
	e.FilaOcorrencias = e.FilaOcorrencias[1:]
	return &oc
}

// ============================================================
// BUSCAR DRONE LIVRE
// ============================================================
func (e *EstadoGerenciador) BuscarDroneLivre() *compartilhado.StatusDrone {
	e.Mu.Lock()
	defer e.Mu.Unlock()
	for _, drone := range e.TabelaDrones {
		if drone.Status == "DISPONIVEL" {
			copia := drone
			return &copia
		}
	}
	return nil
}

// ============================================================
// SETOR JA ATENDIDO 
// ============================================================
func (e *EstadoGerenciador) SetorJaAtendido(setor string) bool {
	e.Mu.Lock()
	defer e.Mu.Unlock()
	for _, drone := range e.TabelaDrones {
		if drone.Status == "EM_MISSAO" && drone.Setor == setor {
			return true
		}
	}
	return false
}

// ============================================================
// MARCAR DRONE OFFLINE 
// ============================================================
func (e *EstadoGerenciador) MarcarDroneOffline(droneID string) *MissaoEmAndamento {
	e.Mu.Lock()
	if d, ok := e.TabelaDrones[droneID]; ok {
		d.Status = "DISPONIVEL"
		d.Setor = ""
		e.TabelaDrones[droneID] = d
	}
	var missao *MissaoEmAndamento
	if m, ok := e.MissoesEmAndamento[droneID]; ok {
		copia := m
		missao = &copia
		delete(e.MissoesEmAndamento, droneID)
	}
	e.Mu.Unlock()
	return missao
}

// ============================================================
// ORDENAR PENDENTES — aplica a mesma lógica de prioridade do desempate
// ============================================================
// Chamado em LiberarRecurso antes de enviar REPLYs.
// Garante que o pendente de maior prioridade receba REPLY primeiro,
// tornando a fila distribuída corretamente ordenada.

func OrdenarPendentes(fila []RequisicaoPendente) {
	sort.SliceStable(fila, func(i, j int) bool {
		a, b := fila[i], fila[j]
		if a.Criticidade != b.Criticidade {
			return a.Criticidade > b.Criticidade
		}
		if !a.TimestampOcorrencia.Equal(b.TimestampOcorrencia) {
			return a.TimestampOcorrencia.Before(b.TimestampOcorrencia)
		}
		return a.SetorOrigem < b.SetorOrigem
	})
}

// ============================================================
// AUXILIARES
// ============================================================
func copiarTabela(t map[string]compartilhado.StatusDrone) map[string]compartilhado.StatusDrone {
	copia := make(map[string]compartilhado.StatusDrone)
	for k, v := range t {
		copia[k] = v
	}
	return copia
}

func criticidadeBarra(crit int) string {
	p, v := "", ""
	for i := 0; i < crit; i++ { p += "█" }
	for i := crit; i < 5; i++ { v += "░" }
	return "[" + p + v + "]"
}