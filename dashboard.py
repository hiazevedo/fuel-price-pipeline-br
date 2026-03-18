# Databricks notebook source
# MAGIC %md
# MAGIC # Dashboard — Preços de Combustíveis BR
# MAGIC > Fonte: ANP | Série histórica 2004–2021 | Atualizado via fuel-price-pipeline-br

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Evolução do Preço Médio Nacional

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   ano,
# MAGIC   produto,
# MAGIC   ROUND(preco_medio_nacional, 2) AS preco_medio
# MAGIC FROM fuel_pipeline.gold.resumo_brasil
# MAGIC WHERE produto IN ('GASOLINA COMUM', 'ETANOL HIDRATADO', 'ÓLEO DIESEL', 'GNV')
# MAGIC ORDER BY produto, ano

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Variação % Anual — Gasolina Comum

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   ano,
# MAGIC   produto,
# MAGIC   ROUND(preco_medio_revenda, 2)  AS preco_medio,
# MAGIC   ROUND(variacao_pct, 1)          AS variacao_pct
# MAGIC FROM fuel_pipeline.gold.variacao_anual
# MAGIC WHERE produto IN ('GASOLINA COMUM', 'ETANOL HIDRATADO', 'ÓLEO DIESEL')
# MAGIC   AND ano > 2004
# MAGIC ORDER BY produto, ano

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Faixa de Preços por Ano — Gasolina Comum

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   ano,
# MAGIC   ROUND(preco_minimo_nacional, 2)  AS preco_minimo,
# MAGIC   ROUND(preco_medio_nacional, 2)   AS preco_medio,
# MAGIC   ROUND(preco_maximo_nacional, 2)  AS preco_maximo
# MAGIC FROM fuel_pipeline.gold.resumo_brasil
# MAGIC WHERE produto = 'GASOLINA COMUM'
# MAGIC ORDER BY ano

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Ranking de Estados — Gasolina Comum (2021)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   estado,
# MAGIC   regiao,
# MAGIC   ROUND(preco_medio_revenda, 2) AS preco_medio,
# MAGIC   ranking_preco
# MAGIC FROM fuel_pipeline.gold.ranking_estados
# MAGIC WHERE produto = 'GASOLINA COMUM'
# MAGIC   AND ano = 2021
# MAGIC ORDER BY ranking_preco

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Preço Médio por Região — Todos os Produtos (2021)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   regiao,
# MAGIC   produto,
# MAGIC   ROUND(AVG(preco_medio_revenda), 2) AS preco_medio_regional
# MAGIC FROM fuel_pipeline.gold.ranking_estados
# MAGIC WHERE ano = 2021
# MAGIC   AND produto IN ('GASOLINA COMUM', 'ETANOL HIDRATADO', 'ÓLEO DIESEL S10')
# MAGIC GROUP BY regiao, produto
# MAGIC ORDER BY produto, regiao

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Razão Etanol / Gasolina por Ano
# MAGIC > Abaixo de 0,7 → etanol é mais vantajoso

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH pivot AS (
# MAGIC   SELECT
# MAGIC     ano,
# MAGIC     MAX(CASE WHEN produto = 'GASOLINA COMUM'    THEN preco_medio_nacional END) AS gasolina,
# MAGIC     MAX(CASE WHEN produto = 'ETANOL HIDRATADO'  THEN preco_medio_nacional END) AS etanol
# MAGIC   FROM fuel_pipeline.gold.resumo_brasil
# MAGIC   GROUP BY ano
# MAGIC )
# MAGIC SELECT
# MAGIC   ano,
# MAGIC   ROUND(gasolina, 2)            AS preco_gasolina,
# MAGIC   ROUND(etanol, 2)              AS preco_etanol,
# MAGIC   ROUND(etanol / gasolina, 3)   AS razao_etanol_gasolina
# MAGIC FROM pivot
# MAGIC WHERE gasolina IS NOT NULL AND etanol IS NOT NULL
# MAGIC ORDER BY ano
