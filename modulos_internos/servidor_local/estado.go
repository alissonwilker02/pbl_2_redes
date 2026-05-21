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
type RequisicaoPendente struct {
	SetorOrigem string
	Relogio     int
	Prioridade  int
}

// ============================================================
// MISSAO EM ANDAMENTO — rastreia ocorrência ativa por drone
// ============================================================
// Permite que o health check recupere a missão se o drone cair.
type MissaoEmAndamento struct {
	DroneID    string
	Ocorrencia compartilhado.Ocorrencia
	IniciadaEm time.Time
}

// ============================================================
// ESTADO GERENCIADOR
// ============================================================
type EstadoGerenciador struct {
	Setor                  string
	FilaOcorrencias        []compartilhado.Ocorrencia
	RelogioLamport         int
	EstadoMutex            string
	RespostasAguardadas    int
	RelogioMinhaRequisicao int
	RequisicoesPendentes   []RequisicaoPendente
	CanalPermissao         chan struct{}
	TabelaDrones           map[string]compartilhado.StatusDrone
	MissoesEmAndamento     map[string]MissaoEmAndamento
	Mu                     sync.Mutex
}

func NovoEstado(setor string) *EstadoGerenciador {
	return &EstadoGerenciador{
		Setor:                  setor,
		FilaOcorrencias:        make([]compartilhado.Ocorrencia, 0),
		RelogioLamport:         0,
		EstadoMutex:            "RELEASED",
		RespostasAguardadas:    0,
		RelogioMinhaRequisicao: 0,
		RequisicoesPendentes:   make([]RequisicaoPendente, 0),
		CanalPermissao:         make(chan struct{}, 1),
		TabelaDrones:           make(map[string]compartilhado.StatusDrone),
		MissoesEmAndamento:     make(map[string]MissaoEmAndamento),
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
// CORREÇÃO DO DEADLOCK: liberamos o lock ANTES de qualquer I/O
// (print e logger). LogarEstado adquire o lock internamente,
// então não pode ser chamado enquanto o lock está segurado.
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

	// Copia para usar fora do lock
	filaCopia := make([]compartilhado.Ocorrencia, len(e.FilaOcorrencias))
	copy(filaCopia, e.FilaOcorrencias)
	tabelaCopia := copiarTabela(e.TabelaDrones)
	setor := e.Setor
	mutex := e.EstadoMutex
	relogio := e.RelogioLamport

	e.Mu.Unlock() // libera ANTES de qualquer I/O

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
// BUSCAR DRONE LIVRE — cópia explícita (evita bug de ponteiro em map)
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
// SETOR JA ATENDIDO — verifica se o setor tem drone EM_MISSAO
// ============================================================
// Usado APENAS para decidir se deve mandar mais um drone,
// NÃO para descartar a ocorrência.
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
// REMOVER DRONES OFFLINE — limpa tabela de drones inativos
// ============================================================
// Chamado pelo health check para drones que não respondem TCP.
// Retorna a missão que estava sendo executada (se houver) para
// que o chamador possa re-enfileirar a ocorrência.
func (e *EstadoGerenciador) MarcarDroneOffline(droneID string) (missao *MissaoEmAndamento) {
	e.Mu.Lock()

	// Reverte status e limpa setor vinculado
	if d, ok := e.TabelaDrones[droneID]; ok {
		d.Status = "DISPONIVEL"
		d.Setor = ""
		e.TabelaDrones[droneID] = d
	}

	// Recupera missão em andamento, se houver
	if m, ok := e.MissoesEmAndamento[droneID]; ok {
		copia := m
		missao = &copia
		delete(e.MissoesEmAndamento, droneID)
	}

	e.Mu.Unlock()
	return missao
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