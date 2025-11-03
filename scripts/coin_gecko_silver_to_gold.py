import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import DoubleType, StringType
import hashlib
from pyspark.sql.functions import udf


args = getResolvedOptions(sys.argv, ['JOB_NAME','S3_INPUT_PATH','S3_OUTPUT_PATH'])
S3_INPUT_PATH = args['S3_INPUT_PATH']
S3_OUTPUT_PATH = args['S3_OUTPUT_PATH']

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# lendo o parquet do silver 
df = spark.read.parquet(S3_INPUT_PATH)

# tratando alguns dados pra double 
num_cols = ['current_price','high_24h','low_24h','market_cap','market_cap_rank',
            'fully_diluted_valuation','volume','price_change_24h','percentage_7d',
            'ath','atl','circulating_supply','max_supply']

for c in num_cols:
    if c in df.columns:
        df = df.withColumn(c, F.col(c).cast(DoubleType()))

df = df.withColumn('last_updated', F.to_timestamp('last_updated'))

# preços derivados
df = df.withColumn('avg_price', (F.col('high_24h') + F.col('low_24h')) / 2)
df = df.withColumn('price_range', F.col('high_24h') - F.col('low_24h'))
df = df.withColumn('volatility', F.col('price_range') / F.col('avg_price'))

# frça histórica
df = df.withColumn('drawdown_since_ath', (F.col('ath') - F.col('current_price')) / F.col('ath'))

# oferta e diluição (trata nulls de max_supply)
df = df.withColumn(
    'supply_utilization',
    F.when(F.col('max_supply').isNotNull(), F.col('circulating_supply') / F.col('max_supply'))
     .otherwise(F.lit(None))
)

# Market Cap Share
total_market_cap = df.agg(F.sum('market_cap').alias('total')).collect()[0]['total']
df = df.withColumn('market_cap_share', F.col('market_cap') / F.lit(total_market_cap))

# Rank de Market Cap
window_rank = Window.orderBy(F.col('market_cap').desc())
df = df.withColumn('rank_by_marketcap', F.dense_rank().over(window_rank))

# Top gainers / losers
window_change = Window.orderBy(F.col('price_change_24h').desc())
df = df.withColumn('top_n_gainers', F.when(F.row_number().over(window_change) <= 5, 1).otherwise(0))

window_loss = Window.orderBy(F.col('price_change_24h').asc())
df = df.withColumn('top_n_losers', F.when(F.row_number().over(window_loss) <= 5, 1).otherwise(0))


# Snapshot ID
df = df.withColumn("last_updated_str", F.date_format("last_updated", "yyyyMMddHHmmss"))
df = df.withColumn("snapshot_id", F.concat(F.col("id"), F.col("last_updated_str")))

# colunas finais do nosso gold
final_cols = ['id','name','current_price','high_24h','low_24h','avg_price','price_range','volatility',
              'price_change_24h','percentage_7d','market_cap','market_cap_rank','market_cap_share',
              'ath','atl','drawdown_since_ath','circulating_supply','max_supply',
              'fully_diluted_valuation','supply_utilization','rank_by_marketcap','top_n_gainers',
              'top_n_losers','volume','last_updated','snapshot_id']

df_final = df.select(*final_cols)

# salvando em parquet 
df_final.write.mode("overwrite").parquet(S3_OUTPUT_PATH)


job.commit()
