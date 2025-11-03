import os
import io
import boto3
import pandas as pd
import psycopg2

# variaveis de ambiente
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION = os.getenv("AWS_DEFAULT_REGION")
BUCKET_NAME = os.getenv("BUCKET_NAME")
S3_KEY = os.getenv("S3_KEY")

DB_HOST = os.getenv("DB_HOST")
DB_NAME = os.getenv("DB_NAME_DW")
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")
DB_PORT = os.getenv("DB_PORT")

# lendo arquivos do s3
print("Baixando arquivo do S3...")
s3 = boto3.client(
    "s3",
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    region_name=AWS_REGION
)

buffer = io.BytesIO()
s3.download_fileobj(BUCKET_NAME, S3_KEY, buffer)
buffer.seek(0)
df = pd.read_parquet(buffer)
print(f"{len(df)} registros lidos do Parquet.")

# conexao banco
conn = psycopg2.connect(
    host=DB_HOST,
    dbname=DB_NAME,
    user=DB_USER,
    password=DB_PASS,
    port=DB_PORT
)
cur = conn.cursor()

# inserindo a fonte manualmente
fonte_data = {
    "nome_fonte": "coingecko",
    "tipo_fonte": "API",
    "url_base": "https://www.coingecko.com/pt-br",
    "frequencia_coleta": "semanalmente",
    "formato_dado": "json"
}

cur.execute("""
    INSERT INTO dim_fonte (nome_fonte, tipo_fonte, url_base, frequencia_coleta, formato_dado)
    VALUES (%s, %s, %s, %s, %s)
    ON CONFLICT (nome_fonte) DO NOTHING
    RETURNING id_fonte;
""", (
    fonte_data["nome_fonte"],
    fonte_data["tipo_fonte"],
    fonte_data["url_base"],
    fonte_data["frequencia_coleta"],
    fonte_data["formato_dado"]
))

result = cur.fetchone()
if result:
    id_fonte = result[0]
else:
    cur.execute("SELECT id_fonte FROM dim_fonte WHERE nome_fonte = %s;", (fonte_data["nome_fonte"],))
    id_fonte = cur.fetchone()[0]

print(f"Fonte 'coingecko' inserida com id {id_fonte}")

#dim moeda
for _, row in df.iterrows():
    cur.execute("""
        INSERT INTO dim_moeda (id_original, nome_moeda, max_supply)
        VALUES (%s, %s, %s)
        ON CONFLICT (id_original) DO NOTHING;
    """, (row["id"], row["name"], row.get("max_supply")))


cur.execute("SELECT id_original, id_moeda FROM dim_moeda;")
moeda_map = {r[0]: r[1] for r in cur.fetchall()}


df["last_updated"] = pd.to_datetime(df["last_updated"])

tempos = []
for t in df["last_updated"].unique():
    data_hora = pd.to_datetime(t)
    tempos.append((
        data_hora,
        data_hora.date(),
        data_hora.hour,
        data_hora.day,
        data_hora.month,
        data_hora.year
    ))

cur.executemany("""
    INSERT INTO dim_tempo (data_hora, data, hora, dia, mes, ano)
    VALUES (%s, %s, %s, %s, %s, %s)
    ON CONFLICT (data_hora) DO NOTHING;
""", tempos)


cur.execute("SELECT data_hora, id_tempo FROM dim_tempo;")
tempo_map = {str(r[0]): r[1] for r in cur.fetchall()}

print("Tabela dim_tempo atualizada.")


for _, row in df.iterrows():
    id_moeda = moeda_map.get(row["id"])
    id_tempo = tempo_map.get(str(pd.to_datetime(row["last_updated"])))

    if not id_moeda or not id_tempo:
        continue

    cur.execute("""
        INSERT INTO fact_dados_cripto (
            id_moeda, id_fonte, id_tempo,
            current_price, high_24, low_24, avg_price, price_range, volatility,
            price_change, percentage_7d, market_cap, market_cap_rank, market_cap_share,
            volume, circulating_supply, fully_diluted_valuation, drawdown_since_ath,
            rank_by_marketcap, top_n_gainers, top_n_losers
        )
        VALUES (%s, %s, %s,
                %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s,
                %s, %s, %s, %s,
                %s, %s, %s)
    """, (
        id_moeda,
        id_fonte,
        id_tempo,
        row.get("current_price"),
        row.get("high_24h"),
        row.get("low_24h"),
        row.get("avg_price"),
        row.get("price_range"),
        row.get("volatility"),
        row.get("price_change_24h"),
        row.get("percentage_7d"),
        row.get("market_cap"),
        row.get("market_cap_rank"),
        row.get("market_cap_share"),
        row.get("volume"),
        row.get("circulating_supply"),
        row.get("fully_diluted_valuation"),
        row.get("drawdown_since_ath"),
        row.get("rank_by_marketcap"),
        row.get("top_n_gainers"),
        row.get("top_n_losers")
    ))

conn.commit()
cur.close()
conn.close()
print("Dados carregados com sucesso!")
