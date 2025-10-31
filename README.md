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

# ▶️ EXECUÇÃO DO PROJETO

## 1️⃣ Criando Usuário IAM
- Criar usuário IAM .
- Habilitar Programmatic access (Access Key ID + Secret Access Key).
- Conceder permissões para testes (AmazonS3FullAccess + AWSGlueConsoleFullAccess + AWSLakeFormationDataAdmin) .
- Salvar as keys em local seguro.

## 2️⃣ Criar Bucket S3
- Criar bucket.
- Criar estrutura de pastas.
```
bronze/
silver/
gold/
```

## 3️⃣ Configurar Dockerfile
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

## 4️⃣ Configurar Docker Compose
- Execute os comandos:
```
#1 - docker-compose build

#2 - docker-compose run --rm airflow-init

#3 - docker-compose up -d

docker-compose down (Quando terminar)

```
## 5️⃣ Configurar .env
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
## 6️⃣ Criar DAG no Airflow
- Executar semanalmente (toda segunda-feira).
- Extrair dados da API e salvar localmente em JSON.
- Fazer upload para S3 (camada bronze - dados raw).

## 7️⃣ Criar Glue Job para Camada Silver

## 7.1 Criar Role para Governança no Lake Formation
### 1. No IAM, criar nova role:
- Caso de uso: Lake Formation.
- Permissões: (AWSLakeFormationDataAdmin + AWSGlueConsoleFullAccess + AmazonS3FullAccess)
- Salvar role (ex.: GlueLakeFormationAdminRole).

## 7.2 Configurar Lake Formation
### 1. Ir em Administrative roles and tasks → Add admin
- Adicionar a role criada e o usuário IAM.

### 2. Em Data locations, adicionar o bucket S3 inteiro:
- Escolha seu path: s3://nome_do_seu_bucket
- Selecionar a IAM role criada.  
- Clicar em Grant e conceder permissões para o usuario IAM.

## 7.3 Criar Database no Lake Formation

- Database Bronze: Clique em databases -> create database .
- Location seus arquivos raw (layer bronze) .

## 7.4 Criar role Glue
- Ir no IAM -> Criar role
- Aws Service / Use case : Glue .

### Permissões
 - (AmazonS3FullAccess + AWSGlueServiceRole )



## 7.5 Criar Crawler Bronze

- Glue -> Crawler -> create crawler.
- Coloque o path de onde está o json extraido (arquivo raw). 
- Coloque sua role Glue criada anteriormente .
- Colocar o database que criamos.
- Configurar o Scheduler para toda segunda as 09:00 (mas é UTC).
```
cron(0 12 ? * 2 *) 
```
- Rodei manualmente o Crawler a primeira vez.

## 7.6 Criar Glue Job para Camada Silver (Script)
### Transformações que queremos :

- Remover colunas desnecessárias
- Tratar campos nulos
- Padronizar datas (last_updated) para timestamp
- Explodir JSON aninhado (array)
- Remover duplicados
- Salvar em Parquet na camada Silver


### Colunas finais : 
```
id, symbol, name, date, last_updated, current_price, market_cap, volume,
price_change_24h, percentage_7d, ath, atl
```

## Configurações necessárias (Do Script) :

### 1. Colocar a Role Glue criada no script. 

### 2. Parâmetros do job:
```
--S3_INPUT_PATH s3://seu-bucket/bronze/coins_markets/
--S3_OUTPUT_PATH s3://seu-bucket/silver/coins_markets/
```
### 3. Scheduler: toda segunda-feira às 10:00: 
```
cron(0 13 ? * 2 *)
```
## 7.7 Exemplo de transformação : 

### Antes: 
```
 {
        "id": "bitcoin",
        "symbol": "btc",
        "name": "Bitcoin",
        "image": "https://coin-images.coingecko.com/coins/images/1/large/bitcoin.png?1696501400",
        "current_price": 106574,
        "market_cap": 2127884242358,
        "market_cap_rank": 1,
        "fully_diluted_valuation": 2127884242358,
        "total_volume": 71983632523,
        "high_24h": 111758,
        "low_24h": 106522,
        "price_change_24h": -4046.871374368362,
        "price_change_percentage_24h": -3.65834,
        "market_cap_change_24h": -81060466237.28638,
        "market_cap_change_percentage_24h": -3.66965,
        "circulating_supply": 19942003.0,
        "total_supply": 19942003.0,
        "max_supply": 21000000.0,
        "ath": 126080,
        "ath_change_percentage": -15.57379,
        "ath_date": "2025-10-06T18:57:42.558Z",
        "atl": 67.81,
        "atl_change_percentage": 156876.91391,
        "atl_date": "2013-07-06T00:00:00.000Z",
        "roi": null,
        "last_updated": "2025-10-30T20:02:16.030Z",
        "price_change_percentage_1h_in_currency": -0.39510474430048337,
        "price_change_percentage_24h_in_currency": -3.6583375813917964,
        "price_change_percentage_7d_in_currency": -3.3426460949101213
    }

```

### Depois: 
#### Arquivo convertido de parquet pra json para fins de exemplo!
```
{
    "id":"aave",
    "symbol":"aave",
    "name":"Aave",
    "last_updated":"2025-10-30 21:45:27.899000000",
    "current_price":211.24,"market_cap":3223271500,
    "volume":314855460,
    "price_change_24h":-23.291466577591734,
    "percentage_7d":-5.670162832082476,
    "ath":661.69,
    "atl":26.02
    }
```

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