# Databricks notebook source
# MAGIC %md
# MAGIC # 01 - Bronze (Auto Loader)
# MAGIC Carga incremental do Volume `chegada`: uma linha por arquivo, com o JSON bruto. O checkpoint garante que cada arquivo entra uma vez só.

# COMMAND ----------

from pyspark.sql import functions as F

ESQUEMA = "workspace.caminho_livre"
BASE = "/Volumes/workspace/caminho_livre/chegada"

# campos de primeiro nivel; os blocos aninhados ficam como texto e sao tratados na Prata
FONTES = {
    "osm": "version DOUBLE, generator STRING, osm3s STRING, elements STRING",
    "relatos": "atualizado_em STRING, itens STRING",
    "clima": "latitude DOUBLE, longitude DOUBLE, timezone STRING, hourly_units STRING, hourly STRING",
}

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {ESQUEMA}.bronze_controle_cargas (
  executado_em TIMESTAMP, tabela STRING, linhas_antes BIGINT, linhas_depois BIGINT, arquivos_novos BIGINT
) COMMENT 'Historico das cargas da Bronze'
""")


def contar(tabela: str) -> int:
    if not spark.catalog.tableExists(tabela):
        return 0
    return spark.table(tabela).count()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Carga

# COMMAND ----------

resultado = []
for fonte, campos in FONTES.items():
    tabela = f"{ESQUEMA}.bronze_{fonte}"
    antes = contar(tabela)

    (spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("multiLine", "true")
        .schema(campos)
        .load(f"{BASE}/{fonte}")
        .withColumn("arquivo_origem", F.col("_metadata.file_path"))
        .withColumn("data_referencia",
                    F.to_date(F.regexp_extract("arquivo_origem", r"(\d{4}-\d{2}-\d{2})\.json$", 1)))
        .withColumn("carregado_em", F.current_timestamp())
     .writeStream
        .option("checkpointLocation", f"{BASE}/_controle/checkpoint_{fonte}")
        .trigger(availableNow=True)
        .toTable(tabela)
        .awaitTermination())

    depois = contar(tabela)
    resultado.append((tabela.split(".")[-1], antes, depois, depois - antes))

spark.createDataFrame(resultado, "tabela STRING, linhas_antes BIGINT, linhas_depois BIGINT, arquivos_novos BIGINT") \
    .withColumn("executado_em", F.current_timestamp()) \
    .select("executado_em", "tabela", "linhas_antes", "linhas_depois", "arquivos_novos") \
    .write.mode("append").saveAsTable(f"{ESQUEMA}.bronze_controle_cargas")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Histórico das cargas

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT dense_rank() OVER (ORDER BY executado_em) AS execucao,
# MAGIC        date_format(from_utc_timestamp(executado_em, 'America/Sao_Paulo'), 'dd/MM HH:mm:ss') AS quando,
# MAGIC        tabela, linhas_antes, linhas_depois, arquivos_novos
# MAGIC FROM workspace.caminho_livre.bronze_controle_cargas
# MAGIC ORDER BY executado_em, tabela;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Arquivos carregados

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 'bronze_osm' AS tabela, data_referencia, regexp_extract(arquivo_origem, '[^/]+$', 0) AS arquivo, count(*) AS vezes_carregado
# MAGIC FROM workspace.caminho_livre.bronze_osm GROUP BY ALL
# MAGIC UNION ALL
# MAGIC SELECT 'bronze_relatos', data_referencia, regexp_extract(arquivo_origem, '[^/]+$', 0), count(*)
# MAGIC FROM workspace.caminho_livre.bronze_relatos GROUP BY ALL
# MAGIC UNION ALL
# MAGIC SELECT 'bronze_clima', data_referencia, regexp_extract(arquivo_origem, '[^/]+$', 0), count(*)
# MAGIC FROM workspace.caminho_livre.bronze_clima GROUP BY ALL
# MAGIC ORDER BY tabela, data_referencia;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Conferência

# COMMAND ----------

vazios = {
    "bronze_osm": spark.table(f"{ESQUEMA}.bronze_osm").filter("elements IS NULL").count(),
    "bronze_relatos": spark.table(f"{ESQUEMA}.bronze_relatos").filter("itens IS NULL").count(),
    "bronze_clima": spark.table(f"{ESQUEMA}.bronze_clima").filter("hourly IS NULL").count(),
}
assert not any(vazios.values()), f"Arquivos sem o bloco de registros na Bronze: {vazios}"
print("Bronze conferida: todos os arquivos trazem o bloco de registros.", vazios)
