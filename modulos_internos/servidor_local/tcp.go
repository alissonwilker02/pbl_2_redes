package servidor_local

import (
	"bufio"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"pbl_2_redes/compartilhado"
)


// IniciarServidorLocal cria um servidor TCP que escuta conexões de dispositivos locais
// (sensores) dentro do mesmo setor marítimo
// Cada setor tem seu próprio servidor local para receber ocorrências dos sensores
func IniciarServidorLocal(porta string, estado *EstadoGerenciador) {
	// Abre um socket TCP na porta especificada (ex: ":5000")
	// Fica escutando por conexões de sensores do setor
	listener, erro := net.Listen("tcp", porta)
	if erro != nil {
		log.Fatalf("[ERRO] Não foi possível abrir a porta %s: %v", porta, erro)
	}
	defer listener.Close()

	fmt.Printf(">>> [GERENCIADOR %s] Ouvindo conexões locais na porta %s...\n", estado.Setor, porta)

	// Loop principal: aceita conexões de sensores indefinidamente
	for {
		conexao, erro := listener.Accept()
		if erro != nil {
			log.Printf("[ERRO] Falha ao aceitar conexão: %v", erro)
			continue
		}

		// Cada sensor conectado ganha sua própria goroutine (thread leve)
		// Isso permite que múltiplos sensores enviem dados simultaneamente
		// Atende ao requisito de "grande volume de eventos simultâneos"
		go lidarComDispositivo(conexao, estado)
	}
}


// lidarComDispositivo gerencia a comunicação com um sensor específico
// Executa em sua própria goroutine para cada sensor conectado
func lidarComDispositivo(conexao net.Conn, estado *EstadoGerenciador) {
	defer conexao.Close() // Garante que a conexão será fechada ao final da função
	leitor := bufio.NewReader(conexao) // Buffer de leitura para ler linha por linha
	
	// Loop contínuo de leitura das mensagens enviadas pelo sensor
	for {
		// Lê até encontrar '\n' (cada mensagem JSON é terminada com quebra de linha)
		bytesRecebidos, erro := leitor.ReadBytes('\n')
		if erro != nil {
			return // Sensor desconectou ou perdeu conexão - encerra esta goroutine
		}

		// Decodifica o JSON recebido para a estrutura Ocorrencia
		var dadoRecebido compartilhado.Ocorrencia
		erro = json.Unmarshal(bytesRecebidos, &dadoRecebido)
		
		if erro != nil {
			log.Printf("[ERRO] Falha ao decodificar JSON: %v", erro)
			continue // Pula esta mensagem corrompida e continua lendo
		}

		// ==========================================
		// TRAVA DE EXCLUSIVIDADE - VERIFICAÇÃO DE SEGURANÇA
		// ==========================================
		// CORREÇÃO 4: Impede que sensores de um setor enviem dados para outro setor
		// Isso é CRÍTICO no sistema distribuído porque:
		// 1. Cada setor gerencia apenas seus próprios sensores
		// 2. Evita poluição de dados entre setores
		// 3. Sem essa trava, um sensor malicioso ou mal configurado poderia causar confusão
		if dadoRecebido.Setor != estado.Setor {
			fmt.Printf("\n[ALERTA DE SEGURANÇA] Sensor '%s' (Setor %s) tentou conectar ao Setor %s. Encerrando conexão.\n", 
				dadoRecebido.IDSensor, dadoRecebido.Setor, estado.Setor)
			
			// O return encerra a goroutine e fecha a conexão
			// O sensor terá que se reconectar (já implementado no código do sensor com reconexão automática)
			return 
		}

		// Adiciona a ocorrência à fila/estado do gerenciador do setor
		// Aqui pode disparar lógica de alocação de drones se a ocorrência for crítica
		estado.AdicionarOcorrencia(dadoRecebido)

		// Log da ocorrência recebida com timestamp para depuração
		fmt.Printf("[%s] Recebido do %s | Evento: %s | Criticidade: %d\n", 
			dadoRecebido.Timestamp.Format("15:04:05"), 
			dadoRecebido.IDSensor, 
			dadoRecebido.TipoEvento, 
			dadoRecebido.Criticidade)
	}
}