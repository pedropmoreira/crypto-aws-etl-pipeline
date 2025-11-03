CREATE TABLE moeda (
    id_moeda INT NOT NULL,
    snapshot_id VARCHAR(100) NOT NULL,
    id_original VARCHAR(100) NOT NULL,
    max_supply NUMERIC,
    nome VARCHAR(150),
    last_update TIMESTAMP,
    CONSTRAINT pk_moeda PRIMARY KEY (id_moeda, snapshot_id)
);


CREATE TABLE precos (
    id_preco SERIAL PRIMARY KEY,
    id_moeda INT NOT NULL,
    snapshot_id VARCHAR(100) NOT NULL,
    current_price NUMERIC,
    high_24h NUMERIC,
    low_24h NUMERIC,
    avg_price NUMERIC,
    price_range NUMERIC,
    volatility NUMERIC,
    price_change_24h NUMERIC,
    price_change_percentage_7d NUMERIC,
    CONSTRAINT fk_precos_moeda 
        FOREIGN KEY (id_moeda, snapshot_id)
        REFERENCES moeda(id_moeda, snapshot_id)
        ON DELETE CASCADE
);


CREATE TABLE mercado (
    id_mercado SERIAL PRIMARY KEY,
    id_moeda INT NOT NULL,
    snapshot_id VARCHAR(100) NOT NULL,
    market_cap_share NUMERIC,
    market_cap_rank INT,
    market_cap NUMERIC,
    circulating_supply NUMERIC,
    fully_diluted_valuation NUMERIC,
    total_volume NUMERIC,
    top_n_gainers INT CHECK (top_n_gainers IN (0,1)),
    top_n_losers INT CHECK (top_n_losers IN (0,1)),
    CONSTRAINT fk_mercado_moeda 
        FOREIGN KEY (id_moeda, snapshot_id)
        REFERENCES moeda(id_moeda, snapshot_id)
        ON DELETE CASCADE
);


CREATE TABLE historico (
    id_historico SERIAL PRIMARY KEY,
    id_moeda INT NOT NULL,
    snapshot_id VARCHAR(100) NOT NULL,
    atl NUMERIC,
    ath NUMERIC,
    drawdown_since_ath NUMERIC,
    CONSTRAINT fk_historico_moeda 
        FOREIGN KEY (id_moeda, snapshot_id)
        REFERENCES moeda(id_moeda, snapshot_id)
        ON DELETE CASCADE
);
