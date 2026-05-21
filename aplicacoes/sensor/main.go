package main

import (
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"net"
	"os"
	"pbl_2_redes/compartilhado"
	"time"
)

//Função que tenta conectar um servidor de forma resiliente
//Como o problema descreve comunicação instável e possível destruição de equipamentos, 
//esta função implementa reconexão automática com retry e timeout 
func conectarComServidor(endereco string, sensorID string) net.Conn {
	for {
		//DialTimeout com 3 segundos - evita bloqueio indefinido se o servidor estiver fora 
		conn, err := net.DialTimeout("tcp", endereco, 3*time.Second)
		if err == nil {
			fmt.Printf("[%s] Conectado ao servidor %s\n", sensorID, endereco)
			return conn
		}
		// Log de erro e tentativa novamente após 5 segundos 
		// Isso atende ao requisito de tolerancia de falhas do sistema 
		log.Printf("[%s] Falha ao conectar, tentando novamente em 5s: %v", sensorID, err)
		time.Sleep(5 * time.Second)
	}
}

func main() {
	// Validação dos argumentos de linha de comando 
	// O Sensor precisa saber onde está o broker do seu setor e qual o setor que ele pertence  
	if len(os.Args) < 3 {
		fmt.Println("Uso: go run main.go <ip_servidor:porta> <setor>")
		fmt.Println("Exemplo: ./sensor 192.168.0.10:5000 A")
		os.Exit(1)
	}

	enderecoServidor := os.Args[1] // Endereço do broker do setor (ex: "192.168.0.10:5000")
	setorMaritmo := os.Args[2] // Setor onde este sensor está instalado (ex: "A", "B", "C")

	// Geração do ID único do sensor
	// Usa variável de ambiente SENSOR_ID se disponível, se n usa hostname
	// Isso permite identificar cada sensor na rede distribuida  
	sensorID := os.Getenv("SENSOR_ID")
	if sensorID == "" {
		hostname, erro := os.Hostname()
		if erro != nil {
			hostname = "unknown"
		}
		// Pega os últimos 4 caracteres do hostname para criar um ID curto e único
		if len(hostname) > 4 {
			sensorID = fmt.Sprintf("sensor_%s_%s", setorMaritmo, hostname[len(hostname)-4:])
		} else {
			sensorID = fmt.Sprintf("sensor_%s_%s", setorMaritmo, hostname)
		}
	}

	// Lista de possíveis eventos que o sensor pode detectar
	// De acordo com o problema: suspeita de bloqueio, embarcação à deriva,
	// congestionamento, objeto não identificado, risco ambiental
	eventos := []string{"Normal", "Vazamento_oleo", "Embarcacao_desgovernada", "bloqueio_passagem"}

	fmt.Printf(">>> [%s] Iniciando no Setor %s\n", sensorID, setorMaritmo)
	fmt.Printf(">>> Envio de dados para: %s\n", enderecoServidor)

	// Estabelece conexão inicial com o broker do setor
	conexao := conectarComServidor(enderecoServidor, sensorID)
	defer conexao.Close()

	// Loop principal - sensores geram dados aleatoriamente de forma autônoma
	// Conforme especificado no problema: "os sensores simulados não precisam mais possuir
	// interface gráfica, devendo gerar dados aleatoriamente de forma autônoma para testar a carga"
	for {
		// Intervalo de 20 segundos entre cada detecção - simula sensores reais
		time.Sleep(20 * time.Second)

		// Sorteia aleatoriamente um tipo de evento
		eventoSorteado := eventos[rand.Intn(len(eventos))]

		// Lógica de definição da criticidade:
		// Evento "Normal" tem criticidade 1 (baixa)
		// Eventos anormais têm criticidade entre 2 e 5 (conforme problema: 1 a 5)
		criticidade := 1

		if eventoSorteado != "Normal" {
			criticidade = rand.Intn(4) + 2
		}

		// Cria a estrutura de ocorrência conforme definida no pacote compartilhado
		ocorrencia := compartilhado.Ocorrencia{
			IDSensor:    sensorID,
			Setor:       setorMaritmo,
			TipoEvento:  eventoSorteado,
			Criticidade: criticidade,
			Timestamp:   time.Now(),
		}

		// Serializa a ocorrência para JSON - formato de comunicação entre componentes
		conversao_ocorrencia_json, erro := json.Marshal(ocorrencia)
		if erro != nil {
			log.Printf("Erro ao serializar: %v", erro)
			continue
		}

		// Adiciona newline para delimitar mensagens no protocolo TCP
		conversao_ocorrencia_json = append(conversao_ocorrencia_json, '\n')

		_, erro = conexao.Write(conversao_ocorrencia_json)
		if erro != nil {
			// Em caso de falha na conexão (ex: broker caiu, rede instável)
			// Reconecta automaticamente - tolerância a falhas
			log.Printf("[%s] Conexão perdida, reconectando: %v", sensorID, erro)
			conexao.Close()
			conexao = conectarComServidor(enderecoServidor, sensorID)
			continue
		}

		// Log de confirmação do envio com timestamp para depuração
		fmt.Printf("[%s] Evento enviado: %s | Criticidade: %d\n",
			time.Now().Format("15:04:05"), eventoSorteado, criticidade)
	}
}