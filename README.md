# Data Lakehouse Completo: CoinGecko ETL (Airflow, AWS Glue, PostgreSQL DW)

Este projeto implementa uma pipeline completa de engenharia de dados que realiza toda a ETL de informações de criptomoedas.  
Primeiramente acontece a coleta de dados da API pública da CoinGecko, processa e transforma as informações em diferentes camadas de armazenamento do Bucket S3(bronze, silver, gold), e por fim, armazena duas versões dos dados no PostgreSQL:



| Tipo de Banco              | Modelo       | Objetivo   |
|:--------------------|:-------------:|--------------:|
|  **OLTP**    | Normalizado (3FN) | Garantir ACID |
|  **DW** | Modelo Estrela | Facilitar consultas analíticas e relatórios (Power BI) |

## DIAGRAMA TEXTO - Fluxo
API (CoinGecko) -> Airflow (execução semanal) -> AWS S3 (Camada Bronze) ->  AWS Glue / PySpark (Limpeza e Transformações) -> AWS S3 (Camada Silver) -> 
Transformações (Camada Gold) -> PostgreSQL (OLTP + DW Estrela) -> Power BI (Dashboards)

## 📈IMAGEM DIAGRAMA GERAL (Ainda preciso fazer)
###### Em andamento ...

## ▶️ EXECUÇÃO DO PROJETO

### 1️⃣ Criando Usuário IAM
- Criar usuário IAM .
- Habilitar Programmatic access (Access Key ID + Secret Access Key).
- Conceder permissões para testes (AmazonS3FullAccess + AWSGlueConsoleFullAccess) .
- Salvar as keys em local seguro.

### 2️⃣ Criar Bucket S3
- Criar bucket.
- Criar estrutura de pastas.
```
bronze/
silver/
gold/
```

### 3️⃣ Configurar Dockerfile
```
FROM apache/airflow:3.1.1

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

RUN pip install --no-cache-dir \
    boto3 \
    pandas \
    numpy \
    psycopg2-binary
```

### 4️⃣ Configurar Docker Compose
- Execute os comandos:
```
#1 - docker-compose build

#2 - docker-compose run --rm airflow-init

#3 - docker-compose up -d

docker-compose down (Quando terminar)

```
### 5️⃣ Configurar .env
```
# Docker Airflow
AIRFLOW_IMAGE_NAME=apache/airflow:3.0.0
AIRFLOW_UID=50000

# API
COINGECKO_API_KEY=SUA_API_KEY
API_URL= LINK_DA_API
LOCAL_PATH=/tmp/coins_markets.json

# AWS
BUCKET_NAME= NOME_DO_SEU_BUCKET
S3_PATH=bronze/coins_markets
PROFILE_NAME=NOME_DO_SEU_USER

# AWS credentials
AWS_ACCESS_KEY_ID=SUA_CHAVE
AWS_SECRET_ACCESS_KEY=SUA_CHAVE_SECRETA

```
### 6️⃣ Criar DAG no Airflow
- Executar semanalmente (toda segunda-feira).
- Extrair dados da API e salvar localmente em JSON.
- Fazer upload para S3 (camada bronze - dados raw).

### 7️⃣ Criar Glue Job para Camada Silver

- Glue job executa script.
- Lê dados do S3 (bronze).
- Limpa, normaliza e transforma os dados.
- Salva em Parquet no S3 (camada silver).

### 8️⃣ Criar Camada Gold
- Realizar agregações e métricas. 
- Salvar resultados finais em S3 gold.

### 9️⃣ Modelar Bancos de Dados
###### Em andamento...
- Schema OLTP (normalizado).
- Schema DW (modelo estrela).

### 1️⃣0️⃣ Ingestão nos Bancos
- Carregar dados da camada gold no PostgreSQL.

### 1️⃣1️⃣ Power BI
- Conectar ao schema DW do PostgreSQL.
- Criar dashboards interativos de análises de criptomoedas.