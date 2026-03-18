# Databricks notebook source
from pyspark.sql import functions as F
from datetime import datetime

# COMMAND ----------

# DBTITLE 1,Configs
BRONZE_VOLUME = "/Volumes/fuel_pipeline/bronze/raw_files"
LOAD_DATE     = datetime.now().strftime("%Y-%m-%d")

# COMMAND ----------

# DBTITLE 1,Verificar arquivos disponíveis no Volume
print("Arquivos no Volume:")
files = dbutils.fs.ls(BRONZE_VOLUME)
for f in files:
    print(f"   {f.name}  ({f.size:,} bytes)")

# COMMAND ----------

# DBTITLE 1,Ler o CSV
# Ajuste o nome do arquivo conforme o upload
FILE_NAME = files[0].name  # pega o primeiro arquivo automaticamente

print(f"\nLendo arquivo: {FILE_NAME}")

df_raw = (
    spark.read
    .option("header", "true")
    .option("sep", "\t")
    .option("encoding", "utf-8")
    .csv(f"{BRONZE_VOLUME}/{FILE_NAME}")
)

print("Arquivo lido com sucesso")

# COMMAND ----------

# DBTITLE 1,Minimal fix: rename columns to valid Delta names
valid_columns = [c
    .replace(" ", "_")
    .replace("/", "_")
    .replace("-", "_")
    .replace(",", "_")
    .replace(";", "_")
    .replace("{", "_")
    .replace("}", "_")
    .replace("(", "_")
    .replace(")", "_")
    .replace("\n", "_")
    .replace("\t", "_")
    .replace("=", "_")
    for c in df_raw.columns]
df_raw = df_raw.toDF(*valid_columns)

print(f"Dimensões: {df_raw.count():,} linhas × {len(df_raw.columns)} colunas")
print("\nSchema:")
df_raw.printSchema()

print("\nAmostra:")
display(df_raw.limit(10))

# COMMAND ----------

# DBTITLE 1,Adicionar colunas de auditoria e salvar como Delta
print("\nSalvando na camada Bronze (Delta Lake)...")

(
    df_raw
    .withColumn("_load_date",   F.lit(LOAD_DATE))
    .withColumn("_source_file", F.lit(FILE_NAME))
    .write
    .format("delta")
    .mode("overwrite")
    .option("mergeSchema", "true")
    .saveAsTable("fuel_pipeline.bronze.anp_combustiveis")
)

print("Tabela salva: fuel_pipeline.bronze.anp_combustiveis")

# COMMAND ----------

# DBTITLE 1,Validação Final
print("\nContagem final:")
display(spark.sql("""
    SELECT _source_file,
           _load_date,
           COUNT(*) as total_registros
      FROM fuel_pipeline.bronze.anp_combustiveis
     GROUP BY _source_file, _load_date
"""))