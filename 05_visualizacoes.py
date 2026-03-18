# Databricks notebook source
# MAGIC %md
# MAGIC # Análises visuais com SparkSQL + Matplotlib/Seaborn

# COMMAND ----------

import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import seaborn as sns
import pandas as pd
import numpy as np
from config import get_spark

spark = get_spark()

# COMMAND ----------

# Estilo global dos gráficos
plt.rcParams.update({
    "figure.facecolor":  "#0d1117",
    "axes.facecolor":    "#161b22",
    "axes.edgecolor":    "#30363d",
    "axes.labelcolor":   "#c9d1d9",
    "axes.titlecolor":   "#ffffff",
    "xtick.color":       "#8b949e",
    "ytick.color":       "#8b949e",
    "text.color":        "#c9d1d9",
    "grid.color":        "#21262d",
    "grid.linestyle":    "--",
    "grid.alpha":        0.5,
    "font.family":       "monospace",
})

ACCENT_COLORS = ["#58a6ff", "#3fb950", "#f78166", "#d2a8ff",
                 "#ffa657", "#79c0ff", "#56d364", "#ff7b72"]

print("Ambiente configurado\n")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Evolução do preço médio nacional por combustível (2004–2021)

# COMMAND ----------

df_g1 = spark.sql("""
    SELECT ano, produto, preco_medio_nacional
    FROM fuel_pipeline.gold.resumo_brasil
    ORDER BY produto, ano
""").toPandas()

produtos = df_g1["produto"].unique()

fig, ax = plt.subplots(figsize=(14, 6))

for i, produto in enumerate(produtos):
    d = df_g1[df_g1["produto"] == produto]
    ax.plot(d["ano"], d["preco_medio_nacional"],
            marker="o", linewidth=2.5, markersize=5,
            color=ACCENT_COLORS[i % len(ACCENT_COLORS)],
            label=produto)

ax.set_title("Evolução do Preço Médio Nacional por Combustível (2004–2021)",
             fontsize=14, fontweight="bold", pad=15)
ax.set_xlabel("Ano", fontsize=11)
ax.set_ylabel("Preço Médio (R$/l ou R$/13kg)", fontsize=11)
ax.legend(loc="upper left", framealpha=0.2, fontsize=9)
ax.yaxis.set_major_formatter(mticker.FormatStrFormatter("R$ %.2f"))
ax.grid(True)
plt.tight_layout()
plt.show()
print("Gráfico gerado\n")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Variação % anual da Gasolina Comum (YoY)

# COMMAND ----------

df_g2 = spark.sql("""
    SELECT ano, variacao_pct
    FROM fuel_pipeline.gold.variacao_anual
    WHERE produto = 'GASOLINA COMUM'
      AND variacao_pct IS NOT NULL
    ORDER BY ano
""").toPandas()

colors_bar = ["#3fb950" if v >= 0 else "#f78166" for v in df_g2["variacao_pct"]]

fig, ax = plt.subplots(figsize=(14, 5))
bars = ax.bar(df_g2["ano"], df_g2["variacao_pct"],
              color=colors_bar, edgecolor="#0d1117", linewidth=0.5, width=0.6)

for bar, val in zip(bars, df_g2["variacao_pct"]):
    offset = 0.3 if val >= 0 else -1.2
    ax.text(bar.get_x() + bar.get_width() / 2,
            bar.get_height() + offset,
            f"{val:+.1f}%", ha="center", va="bottom", fontsize=8,
            color="#ffffff")

ax.axhline(0, color="#8b949e", linewidth=1)
ax.set_title("Variação Anual do Preço da Gasolina Comum — YoY (%)",
             fontsize=14, fontweight="bold", pad=15)
ax.set_xlabel("Ano", fontsize=11)
ax.set_ylabel("Variação (%)", fontsize=11)
ax.yaxis.set_major_formatter(mticker.FormatStrFormatter("%.1f%%"))
ax.grid(True, axis="y")
plt.tight_layout()
plt.show()
print("Gráfico gerado\n")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Mapa de calor: preço médio da Gasolina por Estado e Ano

# COMMAND ----------

df_g3 = spark.sql("""
    SELECT ano, estado,
           ROUND(AVG(preco_medio_revenda), 3) AS preco_medio
    FROM fuel_pipeline.gold.preco_por_estado_ano
    WHERE produto = 'GASOLINA COMUM'
    GROUP BY ano, estado
    ORDER BY estado, ano
""").toPandas()

pivot = df_g3.pivot(index="estado", columns="ano", values="preco_medio")

fig, ax = plt.subplots(figsize=(18, 10))
sns.heatmap(
    pivot,
    cmap="YlOrRd",
    linewidths=0.3,
    linecolor="#0d1117",
    annot=False,
    fmt=".2f",
    ax=ax,
    cbar_kws={"label": "Preço Médio (R$/l)"}
)
ax.set_title("Mapa de Calor — Preço Médio da Gasolina Comum por Estado e Ano",
             fontsize=14, fontweight="bold", pad=15)
ax.set_xlabel("Ano", fontsize=11)
ax.set_ylabel("Estado", fontsize=11)
ax.tick_params(axis="x", rotation=45)
ax.tick_params(axis="y", rotation=0)
plt.tight_layout()
plt.show()
print("Gráfico gerado\n")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Top 10 estados mais caros vs mais baratos (Gasolina, 2021)

# COMMAND ----------

df_g4 = spark.sql("""
    SELECT estado, regiao, preco_medio_revenda, ranking_preco
    FROM fuel_pipeline.gold.ranking_estados
    WHERE produto = 'GASOLINA COMUM' AND ano = 2021
    ORDER BY ranking_preco
""").toPandas()

top5_caros   = df_g4.head(5)
top5_baratos = df_g4.tail(5).sort_values("preco_medio_revenda")
df_g4_plot   = pd.concat([top5_caros, top5_baratos])

colors_rank = (
    ["#f78166"] * 5 +   # caros = vermelho
    ["#3fb950"] * 5     # baratos = verde
)

fig, ax = plt.subplots(figsize=(12, 6))
bars = ax.barh(df_g4_plot["estado"], df_g4_plot["preco_medio_revenda"],
               color=colors_rank, edgecolor="#0d1117", linewidth=0.5)

for bar, val in zip(bars, df_g4_plot["preco_medio_revenda"]):
    ax.text(bar.get_width() + 0.01, bar.get_y() + bar.get_height() / 2,
            f"R$ {val:.3f}", va="center", fontsize=9, color="#ffffff")

ax.axvline(df_g4_plot["preco_medio_revenda"].mean(),
           color="#ffa657", linestyle="--", linewidth=1.5, label="Média Nacional")
ax.set_title("Top 5 Estados Mais Caros vs Mais Baratos — Gasolina Comum (2021)",
             fontsize=13, fontweight="bold", pad=15)
ax.set_xlabel("Preço Médio Revenda (R$/l)", fontsize=11)
ax.legend(fontsize=9, framealpha=0.2)
ax.grid(True, axis="x")
ax.invert_yaxis()
plt.tight_layout()
plt.show()
print("Gráfico gerado\n")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Comparativo revenda vs distribuição por região (2021)

# COMMAND ----------

df_g5 = spark.sql("""
    SELECT regiao,
           ROUND(AVG(preco_medio_revenda),      3) AS revenda,
           ROUND(AVG(preco_medio_distribuicao), 3) AS distribuicao
    FROM fuel_pipeline.silver.anp_combustiveis
    WHERE produto = 'GASOLINA COMUM' AND ano = 2021
    GROUP BY regiao
    ORDER BY revenda DESC
""").toPandas()

x     = np.arange(len(df_g5["regiao"]))
width = 0.35

fig, ax = plt.subplots(figsize=(12, 6))
b1 = ax.bar(x - width/2, df_g5["revenda"],      width, label="Revenda",      color="#58a6ff", edgecolor="#0d1117")
b2 = ax.bar(x + width/2, df_g5["distribuicao"], width, label="Distribuição", color="#d2a8ff", edgecolor="#0d1117")

for bar in [*b1, *b2]:
    ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 0.02,
            f"R$ {bar.get_height():.2f}", ha="center", va="bottom", fontsize=8)

ax.set_title("Preço Médio Revenda vs Distribuição por Região — Gasolina Comum (2021)",
             fontsize=13, fontweight="bold", pad=15)
ax.set_xlabel("Região", fontsize=11)
ax.set_ylabel("Preço (R$/l)", fontsize=11)
ax.set_xticks(x)
ax.set_xticklabels(df_g5["regiao"], fontsize=10)
ax.legend(fontsize=10, framealpha=0.2)
ax.grid(True, axis="y")
plt.tight_layout()
plt.show()
print("Gráfico gerado\n")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Margem média de revenda por produto ao longo dos anos

# COMMAND ----------

df_g6 = spark.sql("""
    SELECT ano, produto, margem_media_nacional
    FROM fuel_pipeline.gold.resumo_brasil
    WHERE produto IN ('GASOLINA COMUM', 'ETANOL HIDRATADO', 'GLP')
    ORDER BY produto, ano
""").toPandas()

fig, ax = plt.subplots(figsize=(14, 5))

for i, produto in enumerate(df_g6["produto"].unique()):
    d = df_g6[df_g6["produto"] == produto]
    ax.fill_between(d["ano"], d["margem_media_nacional"],
                    alpha=0.15, color=ACCENT_COLORS[i])
    ax.plot(d["ano"], d["margem_media_nacional"],
            marker="o", linewidth=2, markersize=4,
            color=ACCENT_COLORS[i], label=produto)

ax.set_title("Margem Média de Revenda por Produto (2004–2021)",
             fontsize=14, fontweight="bold", pad=15)
ax.set_xlabel("Ano", fontsize=11)
ax.set_ylabel("Margem Média (R$)", fontsize=11)
ax.legend(fontsize=10, framealpha=0.2)
ax.grid(True)
plt.tight_layout()
plt.show()
print("Gráfico gerado\n")