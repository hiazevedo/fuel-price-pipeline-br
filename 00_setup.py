# Databricks notebook source
# MAGIC %md
# MAGIC # Projeto: fuel-price-pipeline-br

# COMMAND ----------

# DBTITLE 1,Criar o Catalog
spark.sql("""
    CREATE CATALOG IF NOT EXISTS fuel_pipeline
    COMMENT 'Catalog principal do projeto fuel-price-pipeline-br'
""")

print("Catalog criado: fuel_pipeline")

# COMMAND ----------

# DBTITLE 1,Criar os Schemas (equivalente aos databases
spark.sql("CREATE SCHEMA IF NOT EXISTS fuel_pipeline.bronze COMMENT 'Raw data - sem transformação'")
spark.sql("CREATE SCHEMA IF NOT EXISTS fuel_pipeline.silver COMMENT 'Dados limpos e tipados'")
spark.sql("CREATE SCHEMA IF NOT EXISTS fuel_pipeline.gold   COMMENT 'Dados analíticos agregados'")

print("Schemas criados: bronze, silver, gold")

# COMMAND ----------

# DBTITLE 1,Criar Volumes
spark.sql("CREATE VOLUME IF NOT EXISTS fuel_pipeline.bronze.raw_files COMMENT 'Arquivos brutos ingeridos'")
spark.sql("CREATE VOLUME IF NOT EXISTS fuel_pipeline.silver.processed  COMMENT 'Dados processados'")
spark.sql("CREATE VOLUME IF NOT EXISTS fuel_pipeline.gold.analytical   COMMENT 'Dados analíticos'")

print("Volumes criados")

# COMMAND ----------

# DBTITLE 1,Definir paths
BRONZE_VOLUME = "/Volumes/fuel_pipeline/bronze/raw_files"
SILVER_PATH   = "fuel_pipeline.silver"
GOLD_PATH     = "fuel_pipeline.gold"

# COMMAND ----------

print(f""" SETUP CONCLUÍDO COM SUCESSO!
-> Catalog : fuel_pipeline
-> Bronze  : fuel_pipeline.bronze (Volume: raw_files)
-> Silver  : fuel_pipeline.silver
-> Gold    : fuel_pipeline.gold
""")

# COMMAND ----------

# DBTITLE 1,Validar estrutura
print("Schemas no catalog:")
display(spark.sql("SHOW SCHEMAS IN fuel_pipeline"))

print("Volumes no schema bronze:")
display(spark.sql("SHOW VOLUMES IN fuel_pipeline.bronze"))