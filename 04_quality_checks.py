# Databricks notebook source
# MAGIC %md
# MAGIC # Check de Qualidade
# MAGIC ## Validações de qualidade em todas as camadas do pipeline

# COMMAND ----------

from pyspark.sql import functions as F
from datetime import datetime

# COMMAND ----------

print("=" * 60)
print("  QUALITY CHECKS — PIPELINE fuel-price-pipeline-br")
print(f"  Executado em: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print("=" * 60)

# COMMAND ----------

# DBTITLE 1,Helper para registrar resultado dos checks
resultados = []

def check(camada, tabela, descricao, passou, detalhe=""):
    status = "✅ PASSOU" if passou else "❌ FALHOU"
    resultados.append((camada, tabela, descricao, status, detalhe))
    print(f"  {status} | {camada}.{tabela} | {descricao} {detalhe}")

# COMMAND ----------

print("\n==== BRONZE ====")

df_bronze = spark.table("fuel_pipeline.bronze.anp_combustiveis")
bronze_count = df_bronze.count()

# Check 1: Volume mínimo de registros
check("bronze", "anp_combustiveis", "Volume mínimo (> 100k registros)",
      bronze_count > 100_000, f"→ {bronze_count:,} registros")

# Check 2: Colunas obrigatórias existem
colunas_esperadas_bronze = {"DATA_INICIAL", "DATA_FINAL", "ESTADO", "PRODUTO",
                             "_load_date", "_source_file"}
colunas_bronze = set(df_bronze.columns)
check("bronze", "anp_combustiveis", "Colunas obrigatórias presentes",
      colunas_esperadas_bronze.issubset(colunas_bronze),
      f"→ {len(colunas_bronze)} colunas encontradas")

# Check 3: Sem registros sem data
nulos_data = df_bronze.filter(F.col("DATA_INICIAL").isNull()).count()
check("bronze", "anp_combustiveis", "Sem nulos em DATA_INICIAL",
      nulos_data == 0, f"→ {nulos_data} nulos encontrados")

# Check 4: Auditoria — load_date preenchido
nulos_load = df_bronze.filter(F.col("_load_date").isNull()).count()
check("bronze", "anp_combustiveis", "Coluna _load_date preenchida",
      nulos_load == 0, f"→ {nulos_load} nulos")

# COMMAND ----------

print("\n==== SILVER ====")

df_silver = spark.table("fuel_pipeline.silver.anp_combustiveis")
silver_count = df_silver.count()

# Check 5: Silver tem menos ou igual registros que Bronze (limpeza aplicada)
check("silver", "anp_combustiveis", "Contagem Silver ≤ Bronze",
      silver_count <= bronze_count,
      f"→ Silver: {silver_count:,} | Bronze: {bronze_count:,}")

# Check 6: Tipos corretos
from pyspark.sql.types import DoubleType, DateType, IntegerType
schema_silver = {f.name: type(f.dataType) for f in df_silver.schema.fields}

check("silver", "anp_combustiveis", "preco_medio_revenda é DoubleType",
      schema_silver.get("preco_medio_revenda") == DoubleType)

check("silver", "anp_combustiveis", "data_inicial é DateType",
      schema_silver.get("data_inicial") == DateType)

check("silver", "anp_combustiveis", "num_postos_pesquisados é IntegerType",
      schema_silver.get("num_postos_pesquisados") == IntegerType)

# Check 7: Sem nulos em colunas críticas
for col in ["data_inicial", "estado", "produto", "preco_medio_revenda"]:
    n = df_silver.filter(F.col(col).isNull()).count()
    check("silver", "anp_combustiveis", f"Sem nulos em '{col}'",
          n == 0, f"→ {n} nulos")

# Check 8: Sem duplicatas
total = df_silver.count()
distintos = df_silver.dropDuplicates(["data_inicial", "data_final", "estado", "produto"]).count()
check("silver", "anp_combustiveis", "Sem duplicatas por chave natural",
      total == distintos, f"→ {total - distintos} duplicatas encontradas")

# Check 9: Preços positivos
precos_negativos = df_silver.filter(F.col("preco_medio_revenda") <= 0).count()
check("silver", "anp_combustiveis", "Preços de revenda positivos",
      precos_negativos == 0, f"→ {precos_negativos} registros inválidos")

# Check 10: Range de anos esperado
anos = df_silver.agg(F.min("ano").alias("min"), F.max("ano").alias("max")).collect()[0]
check("silver", "anp_combustiveis", "Anos no range esperado (2004–2021)",
      anos["min"] >= 2004 and anos["max"] <= 2021,
      f"→ {anos['min']} a {anos['max']}")

# COMMAND ----------

print("\n==== GOLD ====")

# Check 11: Todas as tabelas Gold existem
tabelas_gold = ["preco_por_estado_ano", "ranking_estados", "variacao_anual", "resumo_brasil"]
for tabela in tabelas_gold:
    try:
        n = spark.table(f"fuel_pipeline.gold.{tabela}").count()
        check("gold", tabela, "Tabela existe e tem dados", n > 0, f"→ {n:,} registros")
    except Exception as e:
        check("gold", tabela, "Tabela existe e tem dados", False, f"→ ERRO: {str(e)}")

# Check 12: Ranking sem gaps (1 até N por grupo)
df_rank = spark.table("fuel_pipeline.gold.ranking_estados")
rank_min = df_rank.agg(F.min("ranking_preco")).collect()[0][0]
check("gold", "ranking_estados", "Ranking começa em 1",
      rank_min == 1, f"→ rank mínimo: {rank_min}")

# Check 13: Variação anual — primeiro ano sem YoY (esperado nulo)
df_yoy = spark.table("fuel_pipeline.gold.variacao_anual")
primeiro_ano = df_yoy.agg(F.min("ano")).collect()[0][0]
nulos_yoy = df_yoy.filter(
    (F.col("ano") == primeiro_ano) & F.col("preco_ano_anterior").isNull()
).count()
check("gold", "variacao_anual", f"Primeiro ano ({primeiro_ano}) sem preco_ano_anterior",
      nulos_yoy > 0, f"→ {nulos_yoy} registros com nulo esperado")

# Check 14: Resumo Brasil cobre todos os estados
df_resumo = spark.table("fuel_pipeline.gold.resumo_brasil")
max_estados = df_resumo.agg(F.max("estados_pesquisados")).collect()[0][0]
check("gold", "resumo_brasil", "Cobertura nacional (≥ 20 estados)",
      max_estados >= 20, f"→ máximo de {max_estados} estados em um período")

# COMMAND ----------

# DBTITLE 1,RELATÓRIO FINAL
print("\n" + "=" * 60)
print("  RELATÓRIO FINAL DE QUALIDADE")
print("=" * 60)

df_report = spark.createDataFrame(
    resultados,
    ["camada", "tabela", "check", "status", "detalhe"]
)
display(df_report)

total_checks  = len(resultados)
passou_checks = sum(1 for r in resultados if "PASSOU" in r[3])
falhou_checks = total_checks - passou_checks

print(f""" RESULTADO FINAL DOS QUALITY CHECKS
Total de checks : {total_checks:<5}
✅ Passou       : {passou_checks:<5}
❌ Falhou       : {falhou_checks:<5}
Taxa de sucesso : {(passou_checks/total_checks*100):.1f}%
""")

if falhou_checks == 0:
    print("Pipeline 100% aprovado! Pronto para o portfólio.")
else:
    print("Revise os checks que falharam antes de publicar.")
