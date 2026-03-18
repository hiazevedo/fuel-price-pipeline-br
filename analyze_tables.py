# Databricks notebook source
from pyspark.sql import functions as F

print("=" * 60)
print("  ANÁLISE DE TABELAS — fuel-price-pipeline-br")
print("=" * 60)

# COMMAND ----------

for tabela in ["preco_por_estado_ano", "ranking_estados", "variacao_anual", "resumo_brasil"]:
    df = spark.table(f"fuel_pipeline.gold.{tabela}")
    print(f"\n{'='*50}")
    print(f"  {tabela}")
    print(f"  {df.count():,} registros | {len(df.columns)} colunas")
    print(f"  Colunas: {df.columns}")
    df.limit(3).show(truncate=False)

# COMMAND ----------

print("\n--- PRODUTOS disponíveis ---")
spark.table("fuel_pipeline.gold.resumo_brasil").select("produto").distinct().orderBy("produto").show(20, False)

print("\n--- RANGE de anos ---")
spark.table("fuel_pipeline.gold.resumo_brasil").agg(F.min("ano"), F.max("ano")).show()

print("\n--- ESTADOS disponíveis ---")
spark.table("fuel_pipeline.gold.preco_por_estado_ano").select("estado","regiao").distinct().orderBy("regiao","estado").show(30, False)

print("\n--- Preço nacional por produto (resumo) ---")
spark.table("fuel_pipeline.gold.resumo_brasil").groupBy("produto").agg(
    F.round(F.min("preco_minimo_nacional"),2).alias("min"),
    F.round(F.avg("preco_medio_nacional"),2).alias("media"),
    F.round(F.max("preco_maximo_nacional"),2).alias("max"),
).orderBy("produto").show(False)
