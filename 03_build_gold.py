# Databricks notebook source
# MAGIC %md
# MAGIC # Construção da camada Gold com agregações analíticas

# COMMAND ----------

# DBTITLE 1,Imports
from pyspark.sql import functions as F

# COMMAND ----------

# DBTITLE 1,Ler Silver
print("=" * 60)
print("  CONSTRUÇÃO DA CAMADA GOLD")
print("=" * 60)

df_silver = spark.table("fuel_pipeline.silver.anp_combustiveis")
print(f"\nSilver carregado: {df_silver.count():,} registros")

# COMMAND ----------

# DBTITLE 1,Tabela Gold 1
# =============================================================================
# Preço médio por Estado, Produto e Ano
# Responde: "Como evoluiu o preço de cada combustível por estado ao longo dos anos?"
# =============================================================================
print("\n🔨 Construindo gold_preco_por_estado_ano...")

df_gold_estado_ano = df_silver.groupBy("ano", "mes", "regiao", "estado", "produto") \
    .agg(
        F.round(F.avg("preco_medio_revenda"),      3).alias("preco_medio_revenda"),
        F.round(F.min("preco_minimo_revenda"),      3).alias("preco_minimo_revenda"),
        F.round(F.max("preco_maximo_revenda"),      3).alias("preco_maximo_revenda"),
        F.round(F.avg("margem_media_revenda"),      3).alias("margem_media_revenda"),
        F.round(F.avg("preco_medio_distribuicao"),  3).alias("preco_medio_distribuicao"),
        F.sum("num_postos_pesquisados")              .alias("total_postos_pesquisados")
    ) \
    .orderBy("ano", "mes", "estado", "produto")

(
    df_gold_estado_ano
    .write.format("delta")
    .mode("overwrite")
    .partitionBy("ano", "produto")
    .saveAsTable("fuel_pipeline.gold.preco_por_estado_ano")
)
print(f"gold.preco_por_estado_ano salvo: {df_gold_estado_ano.count():,} registros")

# COMMAND ----------

# DBTITLE 1,Tabela Gold 2
# =============================================================================
# Ranking de estados mais caros por combustível e ano
# Responde: "Quais estados têm os combustíveis mais caros?"
# =============================================================================
print("\nConstruindo gold_ranking_estados...")

from pyspark.sql.window import Window

window_rank = Window.partitionBy("ano", "produto").orderBy(F.desc("preco_medio_revenda"))

df_gold_ranking = df_silver \
    .groupBy("ano", "estado", "regiao", "produto") \
    .agg(F.round(F.avg("preco_medio_revenda"), 3).alias("preco_medio_revenda")) \
    .withColumn("ranking_preco", F.rank().over(window_rank))

(
    df_gold_ranking
    .write.format("delta")
    .mode("overwrite")
    .partitionBy("ano", "produto")
    .saveAsTable("fuel_pipeline.gold.ranking_estados")
)
print(f"gold.ranking_estados salvo: {df_gold_ranking.count():,} registros")

# COMMAND ----------

# DBTITLE 1,Tabela Gold 3
# =============================================================================
# Variação anual de preço por produto (YoY)
# Responde: "Quanto o preço subiu/desceu em relação ao ano anterior?"
# =============================================================================
print("\nConstruindo gold_variacao_anual...")

window_yoy = Window.partitionBy("produto").orderBy("ano")

df_gold_yoy = df_silver \
    .groupBy("ano", "produto") \
    .agg(F.round(F.avg("preco_medio_revenda"), 3).alias("preco_medio_revenda")) \
    .withColumn("preco_ano_anterior", F.lag("preco_medio_revenda", 1).over(window_yoy)) \
    .withColumn(
        "variacao_pct",
        F.round(
            (F.col("preco_medio_revenda") - F.col("preco_ano_anterior"))
            / F.col("preco_ano_anterior") * 100, 2
        )
    ) \
    .orderBy("produto", "ano")

(
    df_gold_yoy
    .write.format("delta")
    .mode("overwrite")
    .saveAsTable("fuel_pipeline.gold.variacao_anual")
)
print(f"gold.variacao_anual salvo: {df_gold_yoy.count():,} registros")

# COMMAND ----------

# DBTITLE 1,Tabela Gold 4
# =============================================================================
# Resumo geral (visão executiva)
# Responde: "Qual o panorama geral do mercado de combustíveis no Brasil?"
# =============================================================================
print("\nConstruindo gold_resumo_brasil...")

df_gold_resumo = df_silver \
    .groupBy("ano", "produto") \
    .agg(
        F.round(F.avg("preco_medio_revenda"),   3).alias("preco_medio_nacional"),
        F.round(F.min("preco_minimo_revenda"),   3).alias("preco_minimo_nacional"),
        F.round(F.max("preco_maximo_revenda"),   3).alias("preco_maximo_nacional"),
        F.round(F.avg("margem_media_revenda"),   3).alias("margem_media_nacional"),
        F.countDistinct("estado")                 .alias("estados_pesquisados"),
        F.sum("num_postos_pesquisados")            .alias("total_postos_pesquisados")
    ) \
    .orderBy("produto", "ano")

(
    df_gold_resumo
    .write.format("delta")
    .mode("overwrite")
    .saveAsTable("fuel_pipeline.gold.resumo_brasil")
)
print(f"gold.resumo_brasil salvo: {df_gold_resumo.count():,} registros")

# COMMAND ----------

# Preview das 4 tabelas Gold
print("\n" + "=" * 60)
print("  VALIDAÇÃO FINAL — CAMADA GOLD")
print("=" * 60)

print("\n[1/4] Preço médio da Gasolina Comum por estado em 2021:")
display(spark.sql("""
    SELECT estado, regiao, preco_medio_revenda, total_postos_pesquisados
    FROM fuel_pipeline.gold.preco_por_estado_ano
    WHERE ano = 2021 AND produto = 'GASOLINA COMUM' AND mes = 1
    ORDER BY preco_medio_revenda DESC
    LIMIT 10
"""))

print("\n[2/4] Top 5 estados mais caros para Gasolina em 2021:")
display(spark.sql("""
    SELECT estado, regiao, preco_medio_revenda, ranking_preco
    FROM fuel_pipeline.gold.ranking_estados
    WHERE ano = 2021 AND produto = 'GASOLINA COMUM'
    ORDER BY ranking_preco
    LIMIT 5
"""))

print("\n[3/4] Variação anual de preço da Gasolina Comum (YoY):")
display(spark.sql("""
    SELECT ano, preco_medio_revenda, preco_ano_anterior, variacao_pct
    FROM fuel_pipeline.gold.variacao_anual
    WHERE produto = 'GASOLINA COMUM'
    ORDER BY ano
"""))

print("\n[4/4] Resumo nacional — todos os combustíveis (últimos 5 anos):")
display(spark.sql("""
    SELECT ano, produto, preco_medio_nacional, margem_media_nacional,
           estados_pesquisados, total_postos_pesquisados
    FROM fuel_pipeline.gold.resumo_brasil
    WHERE ano >= 2017
    ORDER BY produto, ano
"""))

print(""" CAMADA GOLD CONCLUÍDA!

gold.preco_por_estado_ano  → evolução por estado
gold.ranking_estados       → ranking de preços
gold.variacao_anual        → variação YoY
gold.resumo_brasil         → panorama nacional
""")