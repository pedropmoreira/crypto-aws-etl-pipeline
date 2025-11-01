import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, TimestampType

# parametros de entrada 
args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'S3_INPUT_PATH',
    'S3_OUTPUT_PATH'
])

S3_INPUT_PATH = args['S3_INPUT_PATH']
S3_OUTPUT_PATH = args['S3_OUTPUT_PATH']

# incialização spark 
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# lendo camada bronze
print(f"Lendo dados do caminho: {S3_INPUT_PATH}")
df = spark.read.option("multiline", "true").json(S3_INPUT_PATH)

print("Schema inicial:")
df.printSchema()

# explodindo array 
if 'array' in df.columns:
    print("Detectado campo 'array' — explodindo registros...")
    df = df.select(F.explode(F.col('array')).alias('c'))
    df = df.select('c.*')

# renomeando campos 
possible_renames = {
    'total_volume': 'volume',
    'price_change_percentage_7d_in_currency': 'percentage_7d'
}

for old, new in possible_renames.items():
    if old in df.columns:
        df = df.withColumnRenamed(old, new)

# time stamp no last_updated
if 'last_updated' in df.columns:
    df = df.withColumn(
        'last_updated',
        F.to_timestamp(F.col('last_updated'), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
    )
else:
    df = df.withColumn('last_updated', F.lit(None).cast(TimestampType()))

# cast nas colunas numericas 
num_cols = [
    'current_price', 'market_cap', 'volume',
    'price_change_24h', 'ath', 'atl', 'percentage_7d',
    'high_24h', 'low_24h',
    'market_cap_rank', 'fully_diluted_valuation',
    'circulating_supply', 'max_supply'
]

for c in num_cols:
    if c in df.columns:
        df = df.withColumn(c, F.col(c).cast(DoubleType()))

# apenas colunas relevantes. 
desired_cols = [
    'id', 'symbol', 'name', 'last_updated',
    'current_price', 'high_24h', 'low_24h',
    'market_cap', 'market_cap_rank', 'fully_diluted_valuation',
    'volume', 'price_change_24h', 'percentage_7d',
    'ath', 'atl', 'circulating_supply', 'max_supply'
]

cols_existentes = [c for c in desired_cols if c in df.columns]
df_final = df.select(*cols_existentes)

# tratando nulos e duplicados 
df_final = df_final.dropDuplicates(['id', 'last_updated'])
df_final = df_final.filter(F.col('id').isNotNull())

#  alguns testes pra debug 
print("Schema final limpo:")
df_final.printSchema()
print(f"Total de registros finais: {df_final.count()}")

# gravando na silver 
print(f"Gravando dados tratados em: {S3_OUTPUT_PATH}")
df_final.write.mode("append").parquet(S3_OUTPUT_PATH)

print("Job concluído com sucesso!")
job.commit()
