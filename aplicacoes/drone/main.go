package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"net"
	"os"
	"pbl_2_redes/compartilhado"
	"time"
)



// main é a rotina principal que inicializa o drone. Ela processa os argumentos da linha de comando, 
// define o IP do drone (priorizando a variável de ambiente para contornar problemas no Docker), 
// anuncia que o drone está disponível para a rede de gerenciadores e inicia um servidor TCP em loop 
// infinito para escutar e aceitar missões.
func main() {
	if len(os.Args) < 4 {
		fmt.Println("Uso: go run main.go <ID_Drone> <Porta_Local> <Gerenciador1:IP:Porta> ...")
		os.Exit(1)
	}

	droneID := os.Args[1]
	minhaPorta := os.Args[2]
	gerenciadores := os.Args[3:]


	meuIP := os.Getenv("DRONE_IP")
	if meuIP == "" {
		meuIP = obterIPLocal()
		fmt.Printf("[AVISO] DRONE_IP não definido. Usando IP detectado: %s\n", meuIP)
		fmt.Printf("[AVISO] Se o gerenciador não conseguir conectar, defina -e DRONE_IP=<seu_ip>\n")
	}
	meuEndereco := fmt.Sprintf("%s:%s", meuIP, minhaPorta)

	// ─── BANNER ───────────────────────────────────────────────────────────
	fmt.Printf("\n╔══════════════════════════════════════╗\n")
	fmt.Printf("║   DRONE: %-26s║\n", droneID)
	fmt.Printf("╠══════════════════════════════════════╣\n")
	fmt.Printf("║  Endereço : %-26s║\n", meuEndereco)
	fmt.Printf("║  Gerenciadores registrados: %-9d║\n", len(gerenciadores))
	fmt.Printf("╚══════════════════════════════════════╝\n\n")

	// Anuncia disponibilidade para todos os gerenciadores
	broadcastStatus(droneID, meuEndereco, "DISPONIVEL", gerenciadores)

	listener, erro := net.Listen("tcp", ":"+minhaPorta)
	if erro != nil {
		log.Fatalf("[ERRO] Falha ao abrir porta %s: %v", minhaPorta, erro)
	}
	defer listener.Close()

	fmt.Printf("[%s]  Aguardando missões na porta %s...\n", droneID, minhaPorta)

	for {
		conexao, erro := listener.Accept()
		if erro != nil {
			log.Printf("[%s] Erro ao aceitar conexão: %v", droneID, erro)
			continue
		}
		go executarMissao(conexao, droneID, meuEndereco, gerenciadores)
	}
}




// ============================================================
// EXECUTAR MISSAO
// ============================================================
// executarMissao lê as instruções enviadas por um gerenciador via TCP, decodifica o comando JSON 
// e altera o status do drone para "EM_MISSAO" via broadcast. A função simula a duração da missão 
// através de uma pausa de tempo aleatória e, ao terminar, avisa novamente aos gerenciadores que o 
// drone está "DISPONIVEL".
func executarMissao(conexao net.Conn, droneID, meuEndereco string, gerenciadores []string) {
	defer conexao.Close()
	leitor := bufio.NewReader(conexao)

	bytesRecebidos, erro := leitor.ReadBytes('\n')
	if erro != nil {
		log.Printf("[%s] Erro ao ler missão: %v", droneID, erro)
		return
	}

	var comando compartilhado.ComandoDrone
	if erro := json.Unmarshal(bytesRecebidos, &comando); erro != nil {
		log.Printf("[%s] Erro ao decodificar comando: %v", droneID, erro)
		return
	}

	fmt.Printf("\n╔══════════════════════════════════════╗\n")
	fmt.Printf("║   MISSÃO RECEBIDA — %s\n", droneID)
	fmt.Printf("╠══════════════════════════════════════╣\n")
	fmt.Printf("║  Ação       : %s\n", comando.Acao)
	fmt.Printf("║  Localização: %s\n", comando.Localizacao)
	fmt.Printf("╚══════════════════════════════════════╝\n")

	broadcastStatus(droneID, meuEndereco, "EM_MISSAO", gerenciadores)

	duracaoMissao := time.Duration(5+rand.Intn(10)) * time.Second
	fmt.Printf("[%s]  Em trânsito... duração estimada: %v\n", droneID, duracaoMissao)
	time.Sleep(duracaoMissao)

	fmt.Printf("[%s]  Missão concluída! Retornando à base.\n", droneID)
	broadcastStatus(droneID, meuEndereco, "DISPONIVEL", gerenciadores)
}





// ============================================================
// BROADCAST STATUS
// ============================================================
// broadcastStatus notifica todos os gerenciadores da rede sobre o estado atual do drone (ex: DISPONIVEL 
// ou EM_MISSAO). A função empacota essas informações em uma mensagem JSON e abre conexões TCP simultâneas 
// (em goroutines) para cada gerenciador cadastrado, ignorando e tolerando eventuais falhas de comunicação.
func broadcastStatus(droneID, endereco, status string, gerenciadores []string) {
	statusDrone := compartilhado.StatusDrone{
		IDDrone:  droneID,
		Endereco: endereco,
		Status:   status,
	}

	pacoteJSON, _ := json.Marshal(statusDrone)
	msgP2P := compartilhado.MensagemP2P{
		TipoMensagem: "STATUS_DRONE",
		SetorOrigem:  droneID,
		Payload:      string(pacoteJSON),
	}

	dadosJSON, _ := json.Marshal(msgP2P)
	dadosJSON = append(dadosJSON, '\n')

	fmt.Printf("[%s]  Broadcast → %s para %d gerenciador(es)\n",
		droneID, status, len(gerenciadores))

	for _, ipPortaVizinho := range gerenciadores {
		go func(enderecoGerenciador string) {
			conexao, erro := net.DialTimeout("tcp", enderecoGerenciador, 2*time.Second)
			if erro != nil {
				fmt.Printf("[%s]  Gerenciador %s inalcançável (tolerância a falhas OK)\n",
					droneID, enderecoGerenciador)
				return
			}
			defer conexao.Close()
			conexao.Write(dadosJSON)
		}(ipPortaVizinho)
	}
}




// ============================================================
// OBTER IP LOCAL — fallback quando DRONE_IP não está definido
// ============================================================
// obterIPLocal varre as interfaces de rede da máquina buscando um endereço IPv4 que não seja de loopback. 
// Ela serve como mecanismo de fallback para identificar automaticamente o IP do dispositivo caso a 
// variável de ambiente DRONE_IP não tenha sido especificada.
func obterIPLocal() string {
	conexoes, erro := net.Interfaces()
	if erro != nil {
		return "127.0.0.1"
	}
	for _, interf := range conexoes {
		if interf.Flags&net.FlagLoopback != 0 {
			continue
		}
		enderecos, err := interf.Addrs()
		if err != nil {
			continue
		}
		for _, endereco := range enderecos {
			if ipNet, ok := endereco.(*net.IPNet); ok && !ipNet.IP.IsLoopback() {
				if ipNet.IP.To4() != nil {
					return ipNet.IP.String()
				}
			}
		}
	}
	return "127.0.0.1"
}