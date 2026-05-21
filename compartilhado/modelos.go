package compartilhado 

import "time"


// Ocorrencia representa um evento detecdado na região monitorada 
// Esta estrutura é usada para comunicar situações que podem exigir envios de drones
type Ocorrencia struct {
	IDSensor   string    `json:"id_sensor"` // Identifica qual sensor fez a detecção 
	Setor	   string	 `json:"setor"` // Setor marítmo onde a ocorrência foi detectada 
	TipoEvento string	 `json:"tipo_evento"` // Tipo: bloqueio, embarcação à deriva...
	Criticidade int		 `json:"criticidade"` // Nível de gravidade (ex: 1 a 5) - usado para priorização 
	Timestamp time.Time  `json:"timestamp"` // Momento exato da detecção da ocorrência 
}


// MensagemP2P define o formato das mensagens trocadas entre os setores 
// Como nn há servidor central, os setores se comunicam diretamente via P2P 
type MensagemP2P struct {
	TipoMensagem string `json:"tipo_mensagem"` // Tipo: Requisição, Resposta, Alocação, Liberação...
	SetorOrigem  string `json:"setor_origem"` // Setor que está enviando a mensagem 
	SetorDestino string `json:"setor_destino"` // Setor destinatário 
	Relogio      int    `json:"relogio"` // Relógio lógico de lamport para ordenar eventos 
	Prioridade   int    `json:"prioridade"` // Prioridade de mensagem - baseada na criticidade e tempo de espera
	Payload      string `json:"payload"` // Dados da mensagem em JSON (ex: StatusDrone, RequisiçãoDrone)
}


// StatusDrone representa o estado atual de um drone na frota compartilhada 
// Como os drones são recursos distribuídos entre setores, esta estrutura permite rastrear cada drone
type StatusDrone struct {
	IDDrone  string `json:"id_drone"` // Identificador único do drone
	Endereco string `json:"endereco"` // IP e Porta onde o drone pode ser conectado diretamente 
	Setor    string `json:"setor"` // Setor onde o drone está atualmente 
	Status   string `json:"status"`	// Situação: disponível, em missão...
}


// ComandoDrone define instruções enviadas pelos setores para controlar os drones
// Usado para despachar drones para atender ocorrências
type ComandoDrone struct {
	Acao	string 	`json:"acao"` // Ação a executar: inspecionar, retornar, cancelar...
	Localizacao  string `json:"localizacao"` // Setor de destino para a ação
}