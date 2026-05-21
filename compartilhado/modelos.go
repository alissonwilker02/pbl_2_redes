package compartilhado

import "time"

// Ocorrencia representa um evento detectado na região monitorada.
type Ocorrencia struct {
	IDSensor    string    `json:"id_sensor"`
	Setor       string    `json:"setor"`
	TipoEvento  string    `json:"tipo_evento"`
	Criticidade int       `json:"criticidade"`
	Timestamp   time.Time `json:"timestamp"` // momento da detecção pelo sensor
}

// MensagemP2P define o formato das mensagens trocadas entre setores.
type MensagemP2P struct {
	TipoMensagem        string    `json:"tipo_mensagem"`
	SetorOrigem         string    `json:"setor_origem"`
	SetorDestino        string    `json:"setor_destino"`
	Relogio             int       `json:"relogio"`
	Prioridade          int       `json:"prioridade"`          // criticidade da ocorrência
	TimestampOcorrencia time.Time `json:"timestamp_ocorrencia"` // timestamp real da detecção
	                                                             // Permite desempatar por chegada
	                                                             // real quando criticidades iguais
	Payload             string    `json:"payload"`
}

// StatusDrone representa o estado atual de um drone na frota compartilhada.
type StatusDrone struct {
	IDDrone  string `json:"id_drone"`
	Endereco string `json:"endereco"`
	Setor    string `json:"setor"`
	Status   string `json:"status"`
}

// ComandoDrone define instruções enviadas pelos setores para controlar os drones.
type ComandoDrone struct {
	Acao        string `json:"acao"`
	Localizacao string `json:"localizacao"`
}