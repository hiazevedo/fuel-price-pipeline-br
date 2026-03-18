# Databricks notebook source
# MAGIC %md
# MAGIC # 📊 fuel-price-pipeline-br — Estudos
# MAGIC
# MAGIC ## Sobre o Projeto
# MAGIC
# MAGIC Pipeline de engenharia de dados end-to-end construído com **Databricks Free Edition**,
# MAGIC utilizando a **Medallion Architecture** (Bronze → Silver → Gold) com **Delta Lake** e
# MAGIC **Unity Catalog**. Os dados cobrem **17 anos de histórico** de preços de combustíveis
# MAGIC no Brasil (2004–2021), fornecidos pela ANP (Agência Nacional do Petróleo).
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Arquitetura do Pipeline
# MAGIC ```
# MAGIC [Fonte: ANP / Kaggle]
# MAGIC         ↓
# MAGIC   ┌─────────────┐
# MAGIC   │   BRONZE    │  Raw data — 120.823 registros, sem transformação
# MAGIC   │ Delta Lake  │  Auditoria: _load_date, _source_file
# MAGIC   └──────┬──────┘
# MAGIC          ↓
# MAGIC   ┌─────────────┐
# MAGIC   │   SILVER    │  Dados limpos, tipados e sem sentinelas (-99999)
# MAGIC   │ Delta Lake  │  Particionado por ano e produto
# MAGIC   └──────┬──────┘
# MAGIC          ↓
# MAGIC   ┌─────────────┐
# MAGIC   │    GOLD     │  4 tabelas analíticas prontas para consumo
# MAGIC   │ Delta Lake  │  Window Functions, Rankings, YoY
# MAGIC   └─────────────┘
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Estrutura dos Notebooks
# MAGIC
# MAGIC | Notebook | Descrição |
# MAGIC |---|---|
# MAGIC | `00_setup.py` | Criação do Catalog, Schemas e Volumes no Unity Catalog |
# MAGIC | `01_ingest_bronze.py` | Ingestão do CSV e persistência como Delta Lake |
# MAGIC | `02_transform_silver.py` | Limpeza, tipagem e correção de sentinelas ANP |
# MAGIC | `03_build_gold.py` | Construção das 4 tabelas analíticas |
# MAGIC | `04_quality_checks.py` | 22 checks automatizados — taxa de sucesso: 100% |
# MAGIC | `05_visualizacoes.py` | 6 visualizações com SparkSQL + Matplotlib/Seaborn |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Tabelas Gold Geradas
# MAGIC
# MAGIC | Tabela | Registros | Descrição |
# MAGIC |---|---|---|
# MAGIC | `gold.preco_por_estado_ano` | 28.391 | Evolução de preços por estado, mês e produto |
# MAGIC | `gold.ranking_estados` | 2.676 | Ranking de estados por preço médio de revenda |
# MAGIC | `gold.variacao_anual` | 104 | Variação percentual YoY por produto |
# MAGIC | `gold.resumo_brasil` | 104 | Panorama nacional agregado por ano e produto |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Insights dos Dados
# MAGIC
# MAGIC ### 1. Evolução do Preço Médio Nacional (2004–2021)
# MAGIC O GLP (gás de cozinha) registrou o maior crescimento absoluto da série histórica,
# MAGIC saindo de aproximadamente **R$ 32,00** em 2004 para **R$ 85,00** em 2021 —
# MAGIC um aumento de **165% em 17 anos**. Os combustíveis líquidos como gasolina e etanol
# MAGIC praticamente triplicaram de preço no mesmo período.
# MAGIC
# MAGIC ### 2. Variação Anual da Gasolina Comum — YoY
# MAGIC O ano de **2021 registrou o maior salto da série: +20,6%**, reflexo direto da
# MAGIC crise energética pós-pandemia e da desvalorização cambial. Os únicos anos com
# MAGIC deflação nos preços foram **2007 (-2,0%)**, **2019 (-0,7%)** e **2020 (-2,5%)**,
# MAGIC este último influenciado pela queda da demanda durante a pandemia de COVID-19.
# MAGIC
# MAGIC ### 3. Mapa de Calor — Preços por Estado e Ano
# MAGIC A visualização evidencia uma **aceleração generalizada de preços a partir de 2016**,
# MAGIC quando todos os estados passaram a registrar preços acima de R$ 4,50/litro.
# MAGIC O **Acre** se destaca como o estado consistentemente mais caro ao longo de toda
# MAGIC a série histórica, refletindo os custos logísticos da região Norte.
# MAGIC
# MAGIC ### 4. Ranking de Estados — Gasolina Comum (2021)
# MAGIC Em 2021, o **Acre** registrou o maior preço médio de revenda (**R$ 5,793/litro**),
# MAGIC enquanto o **Amapá** foi o estado mais barato (**R$ 4,464/litro**).
# MAGIC A diferença de **R$ 1,33/litro** entre estados para o mesmo produto evidencia
# MAGIC o impacto da logística, tributação estadual (ICMS) e concorrência local nos preços.
# MAGIC
# MAGIC ### 5. Revenda vs Distribuição por Região (2021)
# MAGIC O **Sudeste** apresentou o maior preço médio de revenda (**R$ 5,26/litro**),
# MAGIC mesmo sendo a região mais industrializada e com melhor infraestrutura logística
# MAGIC do país. Esse aparente paradoxo é explicado pela **alíquota de ICMS de São Paulo**,
# MAGIC uma das mais altas do Brasil para combustíveis.
# MAGIC
# MAGIC ### 6. Margem Média de Revenda por Produto (2004–2021)
# MAGIC O **GLP** apresenta margem de revenda significativamente superior à gasolina e
# MAGIC ao etanol, com crescimento contínuo ao longo dos anos — atingindo cerca de
# MAGIC **R$ 17,00 por botijão** em 2018. Isso indica que o mercado de gás de cozinha
# MAGIC é estruturalmente mais lucrativo para revendedores do que os combustíveis líquidos.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Skills Demonstradas
# MAGIC
# MAGIC - **Medallion Architecture** — Bronze, Silver e Gold com Delta Lake
# MAGIC - **Unity Catalog** — Catalog, Schemas e Volumes (padrão moderno Databricks)
# MAGIC - **PySpark** — Transformações, limpeza de dados e tipagem
# MAGIC - **Window Functions** — Ranking (`RANK`) e análise temporal (`LAG`)
# MAGIC - **Particionamento Delta** — por `ano` e `produto` para performance
# MAGIC - **Tratamento de dados reais** — sentinelas `-99999` da ANP substituídos por `null`
# MAGIC - **Quality Checks automatizados** — 22 validações cobrindo Bronze, Silver e Gold
# MAGIC - **SparkSQL** — Queries analíticas em todas as camadas
# MAGIC - **Visualização de dados** — Matplotlib e Seaborn com tema customizado
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Dataset
# MAGIC
# MAGIC | Atributo | Detalhe |
# MAGIC |---|---|
# MAGIC | Fonte | ANP — Agência Nacional do Petróleo, Gás Natural e Biocombustíveis |
# MAGIC | Origem | Kaggle — Gas Prices in Brazil |
# MAGIC | Período | 2004 a 2021 |
# MAGIC | Registros | 120.823 |
# MAGIC | Colunas | 18 (após renomeação e limpeza) |
# MAGIC | Produtos | Gasolina Comum, Gasolina Aditivada, Etanol Hidratado, GLP, GNV, Óleo Diesel, Óleo Diesel S10 |
# MAGIC | Estados cobertos | 27 (todos os estados brasileiros + DF) |