# Problema 2 — Desbloqueio do Estreito de Ormuz 

Este repositório contém a solução para o Problema 2 da disciplina TEC502 (MI Concorrência). O projeto consiste num **Sistema Distribuído P2P** desenvolvido para coordenar uma frota de drones de monitorização marítima, operando num cenário de alta criticidade e comunicação instável, sem qualquer ponto central de falha.

##  Arquitetura do Sistema

A infraestrutura foi desenhada para ser totalmente descentralizada (Peer-to-Peer), garantindo tolerância a falhas e consistência eventual. O sistema é composto por três módulos principais:

* **Gerenciador de Setor (Broker):** O "cérebro" de cada setor (A, B e C). Mantém uma fila de prioridades local e coordena a exclusão mútua com os outros setores via rede P2P.
* **Drone:** Atua como um mini-servidor TCP autónomo. Anuncia a sua disponibilidade na malha, recebe comandos de voo diretos e reporta o seu estado (DISPONIVEL / EM_MISSAO).
* **Sensor:** Dispositivo de telemetria IoT. Gera eventos aleatórios de anomalias (Vazamentos, Embarcações à deriva) com níveis de criticidade (1 a 5) e possui reconexão automática ao seu Gerenciador.

##  Algoritmos e Conceitos Aplicados

Para cumprir os rigorosos requisitos de concorrência e evitar a duplicação de despachos, foram implementados os seguintes conceitos clássicos de sistemas distribuídos:

1. **Algoritmo de Ricart-Agrawala:** Garante a Exclusão Mútua Distribuída. Apenas um setor pode aceder à frota de drones e alocar um recurso num dado instante. Implementado com tolerância a falhas (Reply implícito por timeout).
2. **Relógios Lógicos de Lamport:** Utilizados no algoritmo de exclusão mútua para garantir a ordenação causal dos eventos e resolver empates de requisições concorrentes.
3. **Protocolo Gossip (Disseminação):** Mecanismo de consistência eventual onde as mudanças de estado dos drones são propagadas pela malha P2P, mantendo a tabela de recursos replicada em todos os nós.
4. **Fila de Prioridades Dupla:** Ordenação estável baseada na criticidade do evento (descendente) e no Timestamp de chegada (ascendente).

##  Tecnologias Utilizadas

* **Linguagem:** Go (Golang) - *Goroutines, Channels, Mutexes*
* **Comunicação:** Sockets TCP puros (JSON Payload)
* **Infraestrutura:** Docker e Docker Hub

##  Como Executar

O sistema está empacotado em contentores Docker para garantir o isolamento. Substitua `IP_DA_MAQUINA` pelo IP correspondente da rede local.

### 1. Iniciar os Gerenciadores (Brokers)
```bash
# Setor A
docker run -it --rm -p 5010:5010 -p 6000:6000 alissonwilkersc/gerenciador:v3 A 5010 6000 B:IP_DA_MAQUINA:6001 C:IP_DA_MAQUINA:6002

# Setor B
docker run -it --rm -p 5001:5001 -p 6001:6001 alissonwilkersc/gerenciador:v3 B 5001 6001 A:IP_DA_MAQUINA:6000 C:IP_DA_MAQUINA:6002

# Setor C
docker run -it --rm -p 5002:5002 -p 6002:6002 alissonwilkersc/gerenciador:v3 C 5002 6002 A:IP_DA_MAQUINA:6000 B:IP_DA_MAQUINA:6001