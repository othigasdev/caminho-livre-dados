# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · Bronze: carga incremental com Auto Loader
# MAGIC Lê **só os arquivos novos** de cada pasta do Volume `chegada` e grava nas tabelas Bronze, uma linha por arquivo,
# MAGIC com o JSON bruto preservado em texto. O *checkpoint* do Auto Loader guarda quais arquivos já entraram:
# MAGIC por isso o 2º lote só acrescenta o que é novo, e rodar de novo não duplica nada.
# MAGIC
# MAGIC Rode este notebook **três vezes** para gerar a evidência do requisito 05:
# MAGIC 1. depois de enviar o 1º lote;
# MAGIC 2. de novo, sem enviar nada (2ª execução: nenhum arquivo novo);
# MAGIC 3. depois de enviar o 2º lote.
# MAGIC
# MAGIC A última célula mostra o histórico com as contagens antes e depois de cada execução.

# COMMAND ----------

from pyspark.sql import functions as F

ESQUEMA = "workspace.caminho_livre"
BASE = "/Volumes/workspace/caminho_livre/chegada"

# Campos de primeiro nível de cada fonte. Os blocos aninhados ficam como texto JSON (dado bruto):
# quem define tipos e explode as listas é a Prata.
FONTES = {
    "osm": "version DOUBLE, generator STRING, osm3s STRING, elements STRING",
    "relatos": "atualizado_em STRING, itens STRING",
    "clima": "latitude DOUBLE, longitude DOUBLE, timezone STRING, hourly_units STRING, hourly STRING",
}

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {ESQUEMA}.bronze_controle_cargas (
  executado_em TIMESTAMP, tabela STRING, linhas_antes BIGINT, linhas_depois BIGINT, arquivos_novos BIGINT
) COMMENT 'Historico das cargas da Bronze: contagem antes e depois de cada execucao'
""")


def contar(tabela: str) -> int:
    if not spark.catalog.tableExists(tabela):
        return 0
    return spark.table(tabela).count()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Carga: uma tabela Bronze por fonte

# COMMAND ----------

resultado = []
for fonte, campos in FONTES.items():
    tabela = f"{ESQUEMA}.bronze_{fonte}"
    antes = contar(tabela)

    (spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("multiLine", "true")              # cada arquivo é um único documento JSON
        .schema(campos)
        .load(f"{BASE}/{fonte}")
        .withColumn("arquivo_origem", F.col("_metadata.file_path"))
        .withColumn("data_referencia",
                    F.to_date(F.regexp_extract("arquivo_origem", r"(\d{4}-\d{2}-\d{2})\.json$", 1)))
        .withColumn("carregado_em", F.current_timestamp())
     .writeStream
        .option("checkpointLocation", f"{BASE}/_controle/checkpoint_{fonte}")
        .trigger(availableNow=True)               # processa o que há de novo e encerra
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
# MAGIC ## Evidência: contagens antes e depois de cada execução
# MAGIC Cada execução deste notebook acrescenta três linhas (uma por tabela). Na 2ª execução sem arquivos novos,
# MAGIC `arquivos_novos` é 0 e `linhas_depois` é igual a `linhas_antes`.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT dense_rank() OVER (ORDER BY executado_em) AS execucao,
# MAGIC        date_format(from_utc_timestamp(executado_em, 'America/Sao_Paulo'), 'dd/MM HH:mm:ss') AS quando,
# MAGIC        tabela, linhas_antes, linhas_depois, arquivos_novos
# MAGIC FROM workspace.caminho_livre.bronze_controle_cargas
# MAGIC ORDER BY executado_em, tabela;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Arquivos carregados e prova de que não há duplicata
# MAGIC Cada arquivo do Volume aparece exatamente uma vez na Bronze (`vezes_carregado = 1`).

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
# MAGIC ## Conferência do dado bruto
# MAGIC Nenhum arquivo pode ter entrado sem o bloco de registros (arquivo truncado ou resposta de erro da API).

# COMMAND ----------

vazios = {
    "bronze_osm": spark.table(f"{ESQUEMA}.bronze_osm").filter("elements IS NULL").count(),
    "bronze_relatos": spark.table(f"{ESQUEMA}.bronze_relatos").filter("itens IS NULL").count(),
    "bronze_clima": spark.table(f"{ESQUEMA}.bronze_clima").filter("hourly IS NULL").count(),
}
assert not any(vazios.values()), f"Arquivos sem o bloco de registros na Bronze: {vazios}"
print("Bronze conferida: todos os arquivos trazem o bloco de registros.", vazios)
