# 🚀 EMR Serverless Simulation

Este projeto é uma **simulação local de um ambiente de Big Data** robusto. O objetivo é demonstrar uma orquestração de ETL utilizando **Apache Airflow** para gerenciar containers efêmeros (serverless) que executam jobs **Apache Spark**.

A arquitetura simula o funcionamento de um serviço como o **AWS EMR Serverless**: o Airflow inicia um container Spark sob demanda, executa o processamento de dados e, ao final, o container é encerrado automaticamente, otimizando o uso de recursos.

---

## 🏗️ Arquitetura de Dados

O projeto segue a **Arquitetura Medalhão (Medallion Architecture)**, na qual os dados fluem por camadas de refinamento dentro de um Data Lake simulado (diretório local `/data`).

### 🔄 Pipeline de Processamento (`steps.py`)

1. **Ingestion (Transient)**
   Consome dados de uma API externa e salva os dados brutos no formato **JSON**.

2. **Raw (Bronze)**
   Lê os arquivos JSON da camada de ingestão e converte os dados para o formato **Parquet** (colunar).

3. **Trusted (Silver)**
   Lê os dados da camada Raw e realiza **tipagem, padronização e limpezas**.

4. **Refined (Gold)**
   Realiza **joins** entre as tabelas tratadas e gera um **relatório final agregado**.

---

## 🛠️ Tecnologias Utilizadas

* **Apache Airflow** - Orquestração e agendamento de pipelines
* **Apache Spark (PySpark)** - Processamento distribuído
* **Docker & Docker Compose** - Containerização e infraestrutura
* **Telegram Bot** - Alertas e monitoramento de execução
* **Python** - Implementação dos scripts de ETL

---

## ⚙️ Instalação e Execução

Siga os passos abaixo **na ordem indicada** para configurar e executar o ambiente local.

---

### 1️⃣ Ajuste de Permissões do Docker (Linux / WSL)

Para que o Airflow consiga criar containers Spark dinamicamente, é necessário ajustar o grupo de usuários do Docker.

1. Descubra o **ID do grupo Docker** executando no terminal:

```bash
getent group docker | cut -d: -f3
```

> O resultado será um número, por exemplo: `999`, `998` ou `130`.

2. Abra o arquivo `docker-compose.yaml`, localize o serviço `airflow` e ajuste a propriedade `group_add` com o ID encontrado:

```yaml
services:
  airflow:
    group_add:
      - "999"  # Substitua pelo ID do grupo Docker da sua máquina
```

---

### 2️⃣ Preparação do Data Lake

Crie o diretório local que simula o Data Lake e conceda permissão de escrita para os containers.

```bash
mkdir -p data
chmod 777 data
```

---

### 3️⃣ Build da Imagem Spark

O Airflow executa os jobs Spark utilizando a imagem **`projeto-spark-custom`**. Essa imagem **precisa ser criada manualmente** antes de subir o ambiente.

```bash
docker build -t projeto-spark-custom:latest ./docker/spark
```

---

### 4️⃣ Executar o Ambiente

Inicie todos os serviços de orquestração:

```bash
docker-compose up
```

Após a inicialização, acesse a interface do Airflow:

* **URL:** [http://localhost:8080](http://localhost:8080)
* **Usuário:** `admin`
* **Senha:** `Aparecerá ao final da compilação do docker-compose`

---

## 🔔 Configuração de Notificações (Telegram)

O projeto possui integração com **Telegram** para envio de alertas de sucesso ou falha das DAGs.

---

### 📌 Parte A — Criar o Bot

1. No Telegram, converse com o bot **@BotFather**.
2. Envie o comando:

```text
/newbot
```

3. Guarde o **Token** gerado.
4. Envie uma mensagem (ex: "Oi") para o seu novo bot para habilitar o envio de mensagens.

---

### 📌 Parte B — Configurar o Código

No arquivo `dags/base_dag.py`, atualize a variável:

```python
TELEGRAM_CHAT_ID = "SEU_CHAT_ID_AQUI"
```

> 💡 **Dica:** Descubra seu Chat ID enviando `/start` para o bot **@userinfobot**.

---

### 📌 Parte C — Criar a Conexão no Airflow

1. Acesse o Airflow
2. Vá em **Admin → Connections**
3. Clique em **+ (Create)**
4. Preencha os campos:

| Campo               | Valor                       |
| ------------------- | --------------------------- |
| **Connection Id**   | `telegram_default`          |
| **Connection Type** | `Telegram`                  |
| **Password**        | Token gerado pelo BotFather |

5. Clique em **Save**