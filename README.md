# Problema 2: Desbloqueio do Estreito de Ormuz

Este projeto foi desenvolvido para a disciplina **TEC502 - MI Concorrência e Conectividade** da **UEFS**. O sistema consiste numa infraestrutura distribuída **Peer-to-Peer (P2P)** desenvolvida para coordenar uma frota de drones de monitorização marítima no Estreito de Ormuz, garantindo resiliência e operação contínua sem nenhum servidor central.

---

## Arquitetura do Sistema

Diferente do Problema 1 (que adotava um Broker Central em formato Cliente-Servidor), este sistema adota uma arquitetura **100% Descentralizada**. Cada setor opera de forma autónoma e negocia recursos (drones) diretamente através da malha P2P, utilizando o algoritmo de exclusão mútua distribuída de **Ricart-Agrawala**.

### Componentes da Arquitetura

**CAMADA DE DETEÇÃO (IoT)**
- `Sensor Automático` - Emula deteções na rede e gera ocorrências autônomas (TCP)
- `Sensor Manual` - Interface interativa para injeção manual de anomalias (TCP)

**CAMADA DE GERENCIAMENTO (P2P)**
- `Gerenciador de Setor (A, B, C)` - Atua como Broker do próprio setor. Mantém filas de prioridades e coordena uso da frota.
- `Comunicação P2P` - Negociação via REQUEST/REPLY para acesso à secção crítica.

**CAMADA DE AÇÃO E APRESENTAÇÃO**
- `Drone Autônomo` - Mini-servidor TCP aguardando ligações para missões

### Fluxo de Dados

| Origem | Destino | Protocolo | Dados Trafegados |
| ------ | ------- | --------- | ---------------- |
| Sensor (Auto/Manual) | Gerenciador do Setor | TCP | Ocorrência (Criticidade, Tipo, Timestamp) |
| Gerenciador A | Gerenciadores B e C | TCP (P2P) | Mensagens REQUEST, REPLY e STATUS_DRONE |
| Gerenciador Alocador | Drone | TCP | Comando de Missão com coordenadas |

---

## Mecanismos Distribuídos e Protocolos

| Mecanismo / Conceito | Onde atua | Motivo / Impacto no Sistema |
| -------------------- | --------- | --------------------------- |
| **Ricart-Agrawala** | Entre Gerenciadores | **Exclusão Mútua:** Evita que 2 setores aloquem o mesmo drone simultaneamente. |
| **Relógio de Lamport** | Entre Gerenciadores | **Ordenação Lógica:** Desempata pedidos concorrentes na malha P2P usando carimbos de tempo. |
| **Protocolo Gossip** | Entre Gerenciadores | **Consistência Eventual:** Propaga (dissemina) o estado dos drones ("Disponível" / "Em Missão") por todos os nós da rede. |
| **Fila Estável Dupla** | Gerenciadores (Local) | Garante o **REQ 1**: Atende ocorrências de maior criticidade primeiro, e em caso de empate, atende a mais antiga. |

---

## Estrutura de Diretórios

```text
.
├── aplicacoes/
│   ├── drone/                 # Executável do Drone (Mini-servidor TCP)
│   ├── gerenciador/           # Executável do Setor (Escuta P2P e Sensores)
│   ├── sensor/                # Emulador Automático de Sensores
│   ├── sensor_manual/         # Interface para interação/demonstração
│   ├── teste/                 # Script de teste de concorrência massiva
│   └── dashboard/             # Interface via terminal do status geral
├── compartilhado/             # Estruturas de dados globais (Modelos JSON)
├── modulos_internos/
│   ├── exclusao_mutua/        # Lógica de Ricart-Agrawala e Broadcast P2P
│   ├── rede_p2p/              # Servidor e repasse de mensagens Gossip
│   └── servidor_local/        # Fila de prioridades e Relógio de Lamport
└── empacotamento_docker/      # Dockerfiles para todos os componentes
```

---

## Como Executar

O sistema foi preparado para rodar via **Docker**, atendendo aos requisitos de isolamento. Para rodar, substitua `<SEU_IP_AQUI>` pelo IP da máquina no laboratório (você pode descobrir seu IP com o comando `ip a` ou `ipconfig`, ex: `172.16.103.14`).

### Passo 1: Construir as Imagens (Build)

Na pasta raiz do projeto, execute:

```bash
docker build -f empacotamento_docker/dockerfile.gerenciador -t alissonwilkersc/gerenciador:v1 .
docker build -f empacotamento_docker/dockerfile.drone       -t alissonwilkersc/drone:v1 .
docker build -f empacotamento_docker/dockerfile.sensor      -t alissonwilkersc/sensor:v1 .
docker build -f empacotamento_docker/dockerfile.sensor_manual -t alissonwilkersc/sensor_manual:v1 .
docker build -f empacotamento_docker/dockerfile.teste       -t alissonwilkersc/teste:v1 .
docker build -f empacotamento_docker/dockerfile.dashboard   -t alissonwilkersc/dashboard:v1 .
```

### Passo 2: Executar Gerenciadores (Brokers) e Dashboard

> O Dashboard precisa rodar na mesma máquina que os Gerenciadores (usa o volume `/tmp` para leitura de ficheiros partilhados).

```bash
# Dashboard (Em um terminal isolado)
docker run -it --rm -v /tmp:/tmp alissonwilkersc/dashboard:v1

# Gerenciador Setor A (Usa porta 5010 para Sensores e 6000 para P2P)
docker run -it --rm -p 5010:5010 -p 6000:6000 alissonwilkersc/gerenciador:v1 \
  A 5010 6000 B:<SEU_IP_AQUI>:6001 C:<SEU_IP_AQUI>:6002

# Gerenciador Setor B (Usa porta 5001 para Sensores e 6001 para P2P)
docker run -it --rm -p 5001:5001 -p 6001:6001 alissonwilkersc/gerenciador:v1 \
  B 5001 6001 A:<SEU_IP_AQUI>:6000 C:<SEU_IP_AQUI>:6002

# Gerenciador Setor C (Usa porta 5002 para Sensores e 6002 para P2P)
docker run -it --rm -p 5002:5002 -p 6002:6002 alissonwilkersc/gerenciador:v1 \
  C 5002 6002 A:<SEU_IP_AQUI>:6000 B:<SEU_IP_AQUI>:6001
```

### Passo 3: Executar Drones

Os drones necessitam de saber os IPs dos gerenciadores para enviar o anúncio de disponibilidade (Broadcast inicial).

```bash
# Drone 01
docker run -it --rm -p 7001:7001 alissonwilkersc/drone:v1 \
  drone_01 7001 <SEU_IP_AQUI>:6000 <SEU_IP_AQUI>:6001 <SEU_IP_AQUI>:6002

# Drone 02
docker run -it --rm -p 7002:7002 alissonwilkersc/drone:v1 \
  drone_02 7002 <SEU_IP_AQUI>:6000 <SEU_IP_AQUI>:6001 <SEU_IP_AQUI>:6002
```

### Passo 4: Executar Sensores

Os sensores ligam-se aos Brokers do seu próprio setor e geram o fluxo de trabalho do sistema.

**Opção A — Automáticos** (geram eventos a cada 20s em background):

```bash
docker run -it --rm -e SENSOR_ID="sensor_A_01" alissonwilkersc/sensor:v1 <SEU_IP_AQUI>:5010 A
docker run -it --rm -e SENSOR_ID="sensor_B_01" alissonwilkersc/sensor:v1 <SEU_IP_AQUI>:5001 B
docker run -it --rm -e SENSOR_ID="sensor_C_01" alissonwilkersc/sensor:v1 <SEU_IP_AQUI>:5002 C
```

**Opção B — Manuais** (interativos para demonstrações):

```bash
# Substituir porta/Setor se for testar no Setor B ou C
docker run -it --rm alissonwilkersc/sensor_manual:v1 <SEU_IP_AQUI>:5010 A
```

---

## Comportamentos Automáticos de Tolerância a Falhas

Para garantir que o requisito **REQ 4** (nenhuma ocorrência seja perdida) seja estritamente cumprido mesmo sob falhas de rede:

**Double-Check e Shadowing Local:** Antes de libertar a exclusão mútua (Mutex), o Gerenciador marca ativamente a zona do drone na sua tabela local, evitando desperdício operacional (dois drones monitorizando o mesmo setor em simultâneo).

**Tratamento de Timeout P2P (Liveness):** O algoritmo de Ricart-Agrawala não colapsa em deadlock se um setor vizinho ficar offline. É aplicado um mecanismo de "Reply Implícito" após 2 segundos, garantindo a vitalidade do sistema.

**Health Check Circular:** Uma goroutine assíncrona rastreia constantemente o socket TCP dos drones alocados. Se o drone perder a ligação durante a missão (drone abatido / falha de rede), a ocorrência que estava a ser atendida é recuperada e re-inserida na fila mantendo o Timestamp original intacto, permitindo o despacho de outro drone sobresselente sem perda de dados.