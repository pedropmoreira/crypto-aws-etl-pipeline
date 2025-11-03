import os
import io
import boto3
import pandas as pd
import psycopg2

AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION = os.getenv("AWS_DEFAULT_REGION")
BUCKET_NAME = os.getenv("BUCKET_NAME")
S3_KEY = os.getenv("S3_KEY")

DB_HOST = os.getenv("DB_HOST")
DB_NAME_FN = os.getenv("DB_NAME_FN")
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")
DB_PORT = os.getenv("DB_PORT")


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


conn = psycopg2.connect(
    host=DB_HOST,
    dbname=DB_NAME_FN,
    user=DB_USER,
    password=DB_PASS,
    port=DB_PORT
)
cur = conn.cursor()

for _, row in df.iterrows():
    cur.execute("""
        INSERT INTO moeda (id_moeda, snapshot_id, id_original, max_supply, nome, last_update)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (id_moeda, snapshot_id) DO NOTHING;
    """, (
        row.get("id").replace("-", "_").__hash__() % 1000000,  
        row.get("snapshot_id"),
        row.get("id"),
        row.get("max_supply"),
        row.get("name"),
        pd.to_datetime(row.get("last_updated"))
    ))


for _, row in df.iterrows():
    id_moeda = row.get("id").replace("-", "_").__hash__() % 1000000
    cur.execute("""
        INSERT INTO precos (
            id_moeda, snapshot_id,
            current_price, high_24h, low_24h, avg_price, price_range, volatility,
            price_change_24h, price_change_percentage_7d
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT DO NOTHING;
    """, (
        id_moeda,
        row.get("snapshot_id"),
        row.get("current_price"),
        row.get("high_24h"),
        row.get("low_24h"),
        row.get("avg_price"),
        row.get("price_range"),
        row.get("volatility"),
        row.get("price_change_24h"),
        row.get("percentage_7d")
    ))



for _, row in df.iterrows():
    id_moeda = row.get("id").replace("-", "_").__hash__() % 1000000
    cur.execute("""
        INSERT INTO mercado (
            id_moeda, snapshot_id,
            market_cap_share, market_cap_rank, market_cap, circulating_supply,
            fully_diluted_valuation, total_volume, top_n_gainers, top_n_losers
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT DO NOTHING;
    """, (
        id_moeda,
        row.get("snapshot_id"),
        row.get("market_cap_share"),
        row.get("market_cap_rank"),
        row.get("market_cap"),
        row.get("circulating_supply"),
        row.get("fully_diluted_valuation"),
        row.get("total_volume") or row.get("volume"),
        row.get("top_n_gainers"),
        row.get("top_n_losers")
    ))


for _, row in df.iterrows():
    id_moeda = row.get("id").replace("-", "_").__hash__() % 1000000
    cur.execute("""
        INSERT INTO historico (
            id_moeda, snapshot_id,
            atl, ath, drawdown_since_ath
        )
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT DO NOTHING;
    """, (
        id_moeda,
        row.get("snapshot_id"),
        row.get("atl"),
        row.get("ath"),
        row.get("drawdown_since_ath")
    ))


conn.commit()
cur.close()
conn.close()
print("Dados carregados com sucesso no modelo normalizado (3FN)!")
