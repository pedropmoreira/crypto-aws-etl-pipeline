-- view dimensoes + fato 
CREATE OR REPLACE VIEW vw_dados_cripto_completo AS
SELECT 
    f.id_fato_dados,
    m.nome_moeda,
    m.max_supply,
    t.data_hora,
    t.data,
    t.hora,
    t.dia,
    t.mes,
    t.ano,
    fonte.nome_fonte,
    f.current_price,
    f.high_24,
    f.low_24,
    f.avg_price,
    f.price_range,
    f.volatility,
    f.price_change,
    f.percentage_7d,
    f.market_cap,
    f.market_cap_rank,
    f.market_cap_share,
    f.volume,
    f.circulating_supply,
    f.fully_diluted_valuation,
    f.drawdown_since_ath,
    f.rank_by_marketcap,
    f.top_n_gainers,
    f.top_n_losers
FROM fact_dados_cripto f
JOIN dim_moeda m ON f.id_moeda = m.id_moeda
JOIN dim_tempo t ON f.id_tempo = t.id_tempo
JOIN dim_fonte fonte ON f.id_fonte = fonte.id_fonte;


-- top 10 moedas com mais marketcap no snapshot
CREATE OR REPLACE VIEW vw_top10_marketcap AS
SELECT
    nome_moeda,
    market_cap,
    current_price,
    volume,
    market_cap_rank,
    data
FROM vw_dados_cripto_completo
WHERE market_cap IS NOT NULL
ORDER BY market_cap DESC
LIMIT 10;




-- total market cap , media, volume total ...
CREATE OR REPLACE VIEW vw_marketcap_summary AS
SELECT
    data,
    SUM(market_cap) AS total_market_cap,
    AVG(market_cap) AS avg_market_cap,
    SUM(volume) AS total_volume,
    COUNT(DISTINCT nome_moeda) AS total_moedas
FROM vw_dados_cripto_completo
GROUP BY data;

-- volume x market_cap 
CREATE OR REPLACE VIEW vw_volume_vs_marketcap AS
SELECT
    nome_moeda,
    market_cap,
    volume,
    current_price,
    market_cap_rank
FROM vw_dados_cripto_completo
WHERE market_cap IS NOT NULL AND volume IS NOT NULL;


