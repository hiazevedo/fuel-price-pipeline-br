# Fuel Price Pipeline BR

> Pipeline de dados de preços de combustíveis brasileiros com Medallion Architecture no Databricks

![Databricks](https://img.shields.io/badge/Databricks-FF3621?style=for-the-badge&logo=databricks&logoColor=white)
![Apache Spark](https://img.shields.io/badge/Apache_Spark-E25A1C?style=for-the-badge&logo=apachespark&logoColor=white)
![Delta Lake](https://img.shields.io/badge/Delta_Lake-003366?style=for-the-badge&logo=delta&logoColor=white)
![Unity Catalog](https://img.shields.io/badge/Unity_Catalog-0194E2?style=for-the-badge&logo=databricks&logoColor=white)
![Python](https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white)

---

## 📌 Sobre o projeto

Pipeline completo de engenharia de dados que processa **120.823 registros** de preços de combustíveis da ANP (Agência Nacional do Petróleo) entre 2004 e 2021, utilizando a arquitetura Medallion (Bronze → Silver → Gold) no Databricks Free Edition com Unity Catalog.

---

## Arquitetura

```
ANP Dataset (TSV)
      │
      ▼
┌─────────────┐
│   BRONZE    │  Ingestão raw — Delta Lake
│ 120.823 reg │  Colunas de auditoria (_load_date, _source_file)
└──────┬──────┘
       │
       ▼
┌─────────────┐
│   SILVER    │  Limpeza + transformações
│ 120.823 reg │  • Renomeação de colunas (remoção de acentos)
└──────┬──────┘  • Conversão de decimais BR (vírgula → ponto)
       │         • Tratamento de sentinelas ANP (-99999 → null)
       │         • Remoção de duplicatas e nulos críticos
       ▼         • Particionamento por ano e produto
┌─────────────┐
│    GOLD     │  4 tabelas analíticas
│  4 tabelas  │  Prontas para consumo em dashboards
└─────────────┘
```

---

## Tabelas Gold

| Tabela | Registros | Descrição |
|--------|-----------|-----------|
| `preco_por_estado_ano` | 28.391 | Evolução de preços por estado, mês e produto |
| `ranking_estados` | 2.676 | Ranking de estados por preço (Window RANK) |
| `variacao_anual` | 104 | Variação Year-over-Year por produto (Window LAG) |
| `resumo_brasil` | 104 | Panorama nacional de preços |

---

## Estrutura do projeto

```
fuel-price-pipeline-br/
├── config.py                 # Constantes e helper get_spark() centralizados
├── databricks.yml            # Databricks Asset Bundle — Job sequencial completo
├── 00_setup.py               # Catalog, schemas, volumes (Unity Catalog)
├── 01_ingest_bronze.py       # Ingestão do TSV para Delta Bronze
├── 02_transform_silver.py    # Limpeza, tipagem e enriquecimento
├── 03_build_gold.py          # 4 tabelas analíticas com Window Functions
├── 04_quality_checks.py      # 22 checks automatizados ✅ 100%
├── 05_visualizacoes.py       # 6 gráficos com Matplotlib/Seaborn
├── dashboard.py              # Notebook de dashboard Databricks SQL
└── analyze_tables.py         # Inspeção e exploração das tabelas Delta
```

---

## Databricks Job

O pipeline completo está configurado como **Databricks Job** via Asset Bundle (`databricks.yml`), com 5 tasks encadeadas em sequência:

```
ingest_bronze → transform_silver → build_gold → quality_checks → visualizacoes
```

Para fazer o deploy:

```bash
databricks bundle deploy
databricks bundle run fuel_price_full_pipeline
```

---

## Quality Checks

**22/22 checks passando — 100%**

| Camada | Checks | Cobertura |
|--------|--------|-----------|
| Bronze | 5 | Volume, colunas, nulos, auditoria |
| Silver | 10 | Tipos, nulos críticos, duplicatas, range 2004–2021 |
| Gold | 7 | Integridade ranking, YoY, cobertura nacional ≥20 estados |

---

## Principais insights

- **GLP:** maior alta absoluta — de R$ 32 para R$ 85 em 17 anos (+165%)
- **2021:** maior alta YoY da gasolina da série histórica (+20,6%)
- **Acre:** estado mais caro consistentemente em toda a série
- **Amapá:** estado mais barato em 2021 (diferença de R$ 1,33/l vs Acre)
- **A partir de 2016:** aceleração generalizada dos preços em todos os produtos
- **GLP:** margem de revenda muito superior (~R$ 17/botijão em 2018)

---

## Stack técnica

| Tecnologia | Uso |
|------------|-----|
| **Databricks Free Edition** | Ambiente de execução (Serverless AWS) |
| **Unity Catalog** | Governança de dados e controle de acesso |
| **Delta Lake** | Formato de armazenamento com ACID transactions |
| **PySpark** | Processamento distribuído |
| **Window Functions** | RANK e LAG para análises temporais |
| **Databricks Asset Bundles** | Orquestração do pipeline como código |
| **Matplotlib / Seaborn** | Visualizações com tema dark |
| **Databricks SQL** | Dashboard com 6 queries e KPIs |

---

## Como reproduzir

### Pré-requisitos
- Conta no [Databricks Free Edition](https://www.databricks.com/try-databricks)
- Dataset: [Gas Prices in Brazil — Kaggle](https://www.kaggle.com/datasets/matheusfreitag/gas-prices-in-brazil)
- Databricks CLI instalado e configurado

### Passo a passo

```bash
# 1. Clone o repositório
git clone https://github.com/hiazevedo/fuel-price-pipeline-br.git
cd fuel-price-pipeline-br

# 2. Faça upload do arquivo 2004-2021.tsv para o Volume:
# /Volumes/fuel_pipeline/bronze/raw_files/

# 3. Deploy via Asset Bundle
databricks bundle deploy

# 4. Execute o job completo
databricks bundle run fuel_price_full_pipeline
```

Ou execute os notebooks manualmente na ordem:

```
00_setup.py             # Cria catalog e volumes
01_ingest_bronze.py     # Ingere o TSV
02_transform_silver.py  # Transforma os dados
03_build_gold.py        # Cria tabelas analíticas
04_quality_checks.py    # Valida pipeline (22/22)
05_visualizacoes.py     # Gera visualizações
```

### Unity Catalog

```
Catalog : fuel_pipeline
Schemas : bronze | silver | gold
Volume  : /Volumes/fuel_pipeline/bronze/raw_files
```

---

## Visualizações

| # | Gráfico | Tipo |
|---|---------|------|
| 1 | Evolução do preço médio nacional por combustível | Line chart |
| 2 | Variação YoY da gasolina (verde/vermelho) | Bar chart condicional |
| 3 | Preço da gasolina por estado × ano | Heatmap |
| 4 | Top 5 estados mais caros vs mais baratos (2021) | Barh com média |
| 5 | Revenda vs distribuição por região (2020) | Grouped bar |
| 6 | Margem média por produto | Fill between |

---

## Decisões técnicas

**Por que tratar sentinelas -99999 da ANP?**
A ANP usa o valor `-99999` para indicar ausência de dados em campos de margem e distribuição. Sem esse tratamento, análises de média e variação seriam completamente distorcidas (5.517 registros afetados).

**Por que usar 2020 no gráfico de revenda vs distribuição?**
Em 2021, todos os campos de preço de distribuição estão nulos na base da ANP — provavelmente por atraso no repasse dos dados. O gráfico usa 2020 para garantir integridade analítica.

---

## Portfólio

Este projeto faz parte do [Databricks Data Engineering Portfolio](https://github.com/hiazevedo/databricks-portfolio), uma série de projetos práticos cobrindo o ciclo completo de engenharia de dados com Databricks.

| # | Projeto | Tema |
|---|---------|------|
| 1 | **fuel-price-pipeline-br** ← você está aqui | Batch · Medallion · ANP |
| 2 | [earthquake-streaming-pipeline](https://github.com/hiazevedo/earthquake-streaming-pipeline) | Streaming · Auto Loader · USGS |
| 3 | [earthquake-ml-pipeline](https://github.com/hiazevedo/earthquake-ml-pipeline) | ML · MLflow · Spark ML |
| 4 | [weather-dlt-pipeline](https://github.com/hiazevedo/weather-dlt-pipeline) | DLT · Workflows · Open-Meteo |
| 5 | [weather-ml-rain-forecast](https://github.com/hiazevedo/weather-ml-rain-forecast) | ML Avançado · Previsão de Chuva |
