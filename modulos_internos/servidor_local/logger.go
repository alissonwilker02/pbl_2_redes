package servidor_local


import (
	"encoding/json"
	"fmt"
	"os"
	"pbl_2_redes/compartilhado"
	"time"
)

type SnapshotSetor struct {
	Setor           string                               `json:"setor"`
	EstadoMutex     string                               `json:"estado_mutex"`
	RelogioLamport  int                                  `json:"relogio_lamport"`
	FilaOcorrencias []compartilhado.Ocorrencia           `json:"fila_ocorrencias"`
	TabelaDrones    map[string]compartilhado.StatusDrone `json:"tabela_drones"`
	UltimoEvento    string                               `json:"ultimo_evento"`
	AtualizadoEm    time.Time                            `json:"atualizado_em"`
}

// LogarEstado — para chamadas de FORA do lock (loopDespacho, escuta.go)
func LogarEstado(e *EstadoGerenciador, ultimoEvento string) {
	e.Mu.Lock()
	filaCopia := make([]compartilhado.Ocorrencia, len(e.FilaOcorrencias))
	copy(filaCopia, e.FilaOcorrencias)
	tabelaCopia := copiarTabela(e.TabelaDrones)
	setor := e.Setor
	mutex := e.EstadoMutex
	relogio := e.RelogioLamport
	e.Mu.Unlock()

	logarEstadoDireto(setor, mutex, relogio, filaCopia, tabelaCopia, ultimoEvento)
}

// logarEstadoDireto — para chamadas de DENTRO do lock (AdicionarOcorrencia)
// Recebe dados já copiados, não toca no mutex.
func logarEstadoDireto(
	setor, mutex string,
	relogio int,
	fila []compartilhado.Ocorrencia,
	tabela map[string]compartilhado.StatusDrone,
	ultimoEvento string,
) {
	snapshot := SnapshotSetor{
		Setor:           setor,
		EstadoMutex:     mutex,
		RelogioLamport:  relogio,
		FilaOcorrencias: fila,
		TabelaDrones:    tabela,
		UltimoEvento:    ultimoEvento,
		AtualizadoEm:    time.Now(),
	}

	dados, err := json.MarshalIndent(snapshot, "", "  ")
	if err != nil {
		return
	}

	caminho := fmt.Sprintf("/tmp/estado_setor_%s.json", setor)
	caminhoTmp := caminho + ".tmp"

	if err := os.WriteFile(caminhoTmp, dados, 0644); err != nil {
		return
	}
	os.Rename(caminhoTmp, caminho)
}