# Databricks notebook source
# MAGIC %md
# MAGIC # Limpeza, tipagem e padronização dos dados (Bronze → Silver)

# COMMAND ----------

# DBTITLE 1,Imports
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, DateType

# COMMAND ----------

# DBTITLE 1,Ler Bronze
print("=" * 60)
print("  TRANSFORMAÇÃO BRONZE → SILVER")
print("=" * 60)

df_bronze = spark.table("fuel_pipeline.bronze.anp_combustiveis")
print(f"\nBronze carregado: {df_bronze.count():,} registros")

# COMMAND ----------

# DBTITLE 1,Renomear colunas (remover acentos e caracteres especiais)
print("\nRenomeando colunas...")

df_renamed = df_bronze \
    .withColumnRenamed("DATA_INICIAL",                  "data_inicial") \
    .withColumnRenamed("DATA_FINAL",                    "data_final") \
    .withColumnRenamed("REGIÃO",                        "regiao") \
    .withColumnRenamed("ESTADO",                        "estado") \
    .withColumnRenamed("PRODUTO",                       "produto") \
    .withColumnRenamed("NÚMERO_DE_POSTOS_PESQUISADOS",  "num_postos_pesquisados") \
    .withColumnRenamed("UNIDADE_DE_MEDIDA",             "unidade_medida") \
    .withColumnRenamed("PREÇO_MÉDIO_REVENDA",           "preco_medio_revenda") \
    .withColumnRenamed("DESVIO_PADRÃO_REVENDA",         "desvio_padrao_revenda") \
    .withColumnRenamed("PREÇO_MÍNIMO_REVENDA",          "preco_minimo_revenda") \
    .withColumnRenamed("PREÇO_MÁXIMO_REVENDA",          "preco_maximo_revenda") \
    .withColumnRenamed("MARGEM_MÉDIA_REVENDA",          "margem_media_revenda") \
    .withColumnRenamed("COEF_DE_VARIAÇÃO_REVENDA",      "coef_variacao_revenda") \
    .withColumnRenamed("PREÇO_MÉDIO_DISTRIBUIÇÃO",      "preco_medio_distribuicao") \
    .withColumnRenamed("DESVIO_PADRÃO_DISTRIBUIÇÃO",    "desvio_padrao_distribuicao") \
    .withColumnRenamed("PREÇO_MÍNIMO_DISTRIBUIÇÃO",     "preco_minimo_distribuicao") \
    .withColumnRenamed("PREÇO_MÁXIMO_DISTRIBUIÇÃO",     "preco_maximo_distribuicao") \
    .withColumnRenamed("COEF_DE_VARIAÇÃO_DISTRIBUIÇÃO", "coef_variacao_distribuicao")

print("✅ Colunas renomeadas")

# COMMAND ----------

# DBTITLE 1,Converter tipos
print("\nConvertendo tipos de dados...")

colunas_decimal = [
    "preco_medio_revenda",      "desvio_padrao_revenda",
    "preco_minimo_revenda",     "preco_maximo_revenda",
    "margem_media_revenda",     "coef_variacao_revenda",
    "preco_medio_distribuicao", "desvio_padrao_distribuicao",
    "preco_minimo_distribuicao","preco_maximo_distribuicao",
    "coef_variacao_distribuicao"
]

df_typed = df_renamed

for col in colunas_decimal:
    df_typed = df_typed.withColumn(
        col,
        F.when((F.col(col) == '-') | (F.col(col).isNull()), None)
         .otherwise(F.regexp_replace(F.col(col), ',', '.').cast(DoubleType()))
    )

df_typed = df_typed \
    .withColumn("data_inicial",           F.to_date("data_inicial", "yyyy-MM-dd")) \
    .withColumn("data_final",             F.to_date("data_final",   "yyyy-MM-dd")) \
    .withColumn("num_postos_pesquisados", F.col("num_postos_pesquisados").cast(IntegerType()))

print("✅ Tipos convertidos")

# COMMAND ----------

# DBTITLE 1,Limpeza
print("\n Aplicando limpeza...")

nulos_antes = df_typed.filter(F.col("preco_medio_revenda").isNull()).count()
print(f"   Registros com preco_medio_revenda nulo: {nulos_antes:,}")

df_clean = df_typed \
    .filter(F.col("data_inicial").isNotNull()) \
    .filter(F.col("estado").isNotNull()) \
    .filter(F.col("produto").isNotNull()) \
    .filter(F.col("preco_medio_revenda").isNotNull()) \
    .dropDuplicates(["data_inicial", "data_final", "estado", "produto"])

df_clean = df_clean \
    .withColumn("regiao",  F.upper(F.trim(F.col("regiao")))) \
    .withColumn("estado",  F.upper(F.trim(F.col("estado")))) \
    .withColumn("produto", F.upper(F.trim(F.col("produto")))) \
    .withColumn("ano",     F.year("data_inicial")) \
    .withColumn("mes",     F.month("data_inicial"))

print(f"Registros após limpeza: {df_clean.count():,}")

# COMMAND ----------

# DBTITLE 1,Validação de qualidade
print("\nRelatório de qualidade:")

print("\n   Produtos únicos:")
display(df_clean.groupBy("produto").count().orderBy("produto"))

print("\n   Registros por ano:")
display(df_clean.groupBy("ano").count().orderBy("ano"))

print("\n   Range de preços (revenda):")
display(df_clean.select(
    F.round(F.min("preco_medio_revenda"), 3).alias("preco_min"),
    F.round(F.avg("preco_medio_revenda"), 3).alias("preco_medio"),
    F.round(F.max("preco_medio_revenda"), 3).alias("preco_max")
))

# COMMAND ----------

# DBTITLE 1,Salvar como Delta na camada Silver
print("\nSalvando na camada Silver...")

(
    df_clean
    .write
    .format("delta")
    .mode("overwrite")
    .option("mergeSchema", "true")
    .partitionBy("ano", "produto")
    .saveAsTable("fuel_pipeline.silver.anp_combustiveis")
)

print("Tabela salva: fuel_pipeline.silver.anp_combustiveis")

# COMMAND ----------

# DBTITLE 1,Validação final
print("\nValidação final:")
display(spark.sql("""
    SELECT
        ano,
        COUNT(*)                           AS registros,
        COUNT(DISTINCT estado)             AS estados,
        COUNT(DISTINCT produto)            AS produtos,
        ROUND(AVG(preco_medio_revenda), 3) AS preco_medio_revenda
    FROM fuel_pipeline.silver.anp_combustiveis
    GROUP BY ano
    ORDER BY ano
"""))

print("\nSILVER CONCLUÍDO!")

# COMMAND ----------

# DBTITLE 1,Validação final
print("\nValidação final:")
display(spark.sql("""
    SELECT
        ano,
        COUNT(*)                           AS registros,
        COUNT(DISTINCT estado)             AS estados,
        COUNT(DISTINCT produto)            AS produtos,
        ROUND(AVG(preco_medio_revenda), 3) AS preco_medio_revenda
    FROM fuel_pipeline.silver.anp_combustiveis
    GROUP BY ano
    ORDER BY ano
"""))

print("\nSILVER CONCLUÍDO!")

# COMMAND ----------

# MAGIC %md
# MAGIC ### CORREÇÃO SILVER — substituir sentinelas -99999 por null

# COMMAND ----------

# DBTITLE 1,CORREÇÃO SILVER
print("\n" + "=" * 60)
print("  CORREÇÃO DE SENTINELAS -99999 (dado ausente ANP)")
print("=" * 60)

colunas_sentinela = [
    "margem_media_revenda",
    "preco_medio_distribuicao",
    "desvio_padrao_distribuicao",
    "preco_minimo_distribuicao",
    "preco_maximo_distribuicao",
    "coef_variacao_distribuicao",
    "coef_variacao_revenda",
    "desvio_padrao_revenda"
]

print("Corrigindo sentinelas -99999 na Silver...")

df_silver = spark.table("fuel_pipeline.silver.anp_combustiveis")

df_corrigido = df_silver
for col in colunas_sentinela:
    df_corrigido = df_corrigido.withColumn(
        col,
        F.when(F.col(col) < -900, None).otherwise(F.col(col))
    )

(
    df_corrigido
    .write
    .format("delta")
    .mode("overwrite")
    .option("mergeSchema", "true")
    .partitionBy("ano", "produto")
    .saveAsTable("fuel_pipeline.silver.anp_combustiveis")
)

print("Silver corrigida!")

print("\nValidação pós-correção:")
display(spark.sql("""
    SELECT
        MIN(margem_media_revenda)      AS min_margem,
        MAX(margem_media_revenda)      AS max_margem,
        MIN(preco_medio_distribuicao)  AS min_dist,
        MAX(preco_medio_distribuicao)  AS max_dist,
        COUNT(*) FILTER (WHERE margem_media_revenda     IS NULL) AS nulos_margem,
        COUNT(*) FILTER (WHERE preco_medio_distribuicao IS NULL) AS nulos_dist
    FROM fuel_pipeline.silver.anp_combustiveis
"""))

print("02_transform_silver.py CONCLUÍDO!")