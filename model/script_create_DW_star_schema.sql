-- sempre começar com as dimensões : 
-- dimensão moeda : 

CREATE TABLE dim_moeda (
    id_moeda SERIAL PRIMARY KEY,
    id_original VARCHAR(100) NOT NULL,
    nome_moeda VARCHAR(150),
    max_supply NUMERIC
);


CREATE TABLE dim_tempo (
    id_tempo SERIAL PRIMARY KEY,       
    data_hora TIMESTAMP NOT NULL,
    data DATE,
    hora INT,
    dia INT,
    mes INT,
    ano INT
);

CREATE TABLE dim_fonte (
    id_fonte SERIAL PRIMARY KEY,
    nome_fonte VARCHAR(100),
    tipo_fonte VARCHAR(50),
    url_base VARCHAR(300),
    frequencia_coleta VARCHAR(50),
    formato_dado VARCHAR(50)
);


CREATE TABLE fact_dados_cripto (
    id_fato_dados SERIAL PRIMARY KEY,
    id_moeda INT NOT NULL REFERENCES dim_moeda(id_moeda),
    id_fonte INT NOT NULL REFERENCES dim_fonte(id_fonte),
    id_tempo INT NOT NULL REFERENCES dim_tempo(id_tempo),
    id_snapshot INT NOT NULL REFERENCES dim_snapshot(id_snapshot),
    current_price NUMERIC,
    high_24 NUMERIC,
    low_24 NUMERIC,
    avg_price NUMERIC,
    price_range NUMERIC,
    volatility NUMERIC,
    price_change NUMERIC,
    percentage_7d NUMERIC,
    market_cap NUMERIC,
    market_cap_rank INT,
    market_cap_share NUMERIC,
    volume NUMERIC,
    circulating_supply NUMERIC,
    fully_diluted_valuation NUMERIC,
    drawdown_since_ath NUMERIC,
    rank_by_marketcap INT,
	top_n_gainers INT CHECK (top_n_gainers IN (0,1)),
	top_n_losers INT CHECK (top_n_losers IN (0,1))
);

