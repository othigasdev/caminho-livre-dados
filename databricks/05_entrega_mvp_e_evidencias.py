# Databricks notebook source
# MAGIC %md
# MAGIC # 05 · Entrega ao MVP e evidências do relatório
# MAGIC Grava dois arquivos no Volume `entrega`:
# MAGIC
# MAGIC | Arquivo | Para quê |
# MAGIC |---|---|
# MAGIC | `ouro_painel_celula.json` | **Contrato com o produto.** É a tabela `ouro_painel_celula` no formato que o MVP lê (`contrato/ouro_painel_celula.schema.json`). Vai para `app/dados/` no repositório do MVP e é servido em `GET /api/painel`. |
# MAGIC | `evidencias.json` | Contagens e resultados reais desta execução, usados por `relatorio/gerar_relatorio.py`. |
# MAGIC
# MAGIC O Databricks Free Edition não aceita conexão de fora, então a entrega é por arquivo, do mesmo jeito que a chegada.
# MAGIC Baixe os dois pelo Catalog Explorer (Volume `entrega` > arquivo > *Download*).

# COMMAND ----------

import json
import os
from datetime import datetime, timezone
from decimal import Decimal

ESQUEMA = "workspace.caminho_livre"
SAIDA = "/Volumes/workspace/caminho_livre/entrega"
os.makedirs(SAIDA, exist_ok=True)


def linhas(sql: str) -> list[dict]:
    """Resultado da consulta como lista de dicionários (decimais viram número, para o JSON)."""
    return [{coluna: float(valor) if isinstance(valor, Decimal) else valor
             for coluna, valor in linha.asDict(recursive=True).items()} for linha in spark.sql(sql).collect()]


def gravar(nome: str, conteudo: dict) -> None:
    with open(f"{SAIDA}/{nome}", "w", encoding="utf-8") as arquivo:
        json.dump(conteudo, arquivo, ensure_ascii=False, indent=1, default=str)
    print(f"Gravado: {SAIDA}/{nome}")


agora = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Contrato com o MVP: `ouro_painel_celula.json`
# MAGIC Só as quadrículas com algo a mostrar (as `sem_travessia` e sem relato ficam de fora).

# COMMAND ----------

celulas = linhas(f"""
  SELECT id_celula, lat_min, lat_max, lon_min, lon_max, travessias, pontos_rebaixados, pontos_piso_tatil,
         travessias_sem_dado, relatos_total, relatos_abertos, relatos_abertos_demonstracao, nivel_atencao
  FROM {ESQUEMA}.ouro_painel_celula
  WHERE nivel_atencao <> 'sem_travessia' OR relatos_total > 0
  ORDER BY id_celula""")
referencia = linhas(f"SELECT max(data_referencia) AS d FROM {ESQUEMA}.ouro_painel_celula")[0]["d"]

painel = {
    "tabela_origem": f"{ESQUEMA}.ouro_painel_celula",
    "gerado_em": agora,
    "data_referencia": str(referencia),
    "fontes": ["OpenStreetMap (ODbL)", "Relatos do Caminho Livre", "Open-Meteo (CC BY 4.0)"],
    "celulas": celulas,
}
gravar("ouro_painel_celula.json", painel)
print(f"{len(celulas)} quadrículas publicadas, dados de {referencia}.")
display(spark.sql(f"""
  SELECT id_celula, nivel_atencao, travessias, pontos_rebaixados, pontos_piso_tatil, relatos_abertos
  FROM {ESQUEMA}.ouro_painel_celula
  WHERE nivel_atencao <> 'sem_travessia' OR relatos_total > 0
  ORDER BY CASE nivel_atencao WHEN 'alta' THEN 1 WHEN 'media' THEN 2 ELSE 3 END, id_celula"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Evidências para o relatório: `evidencias.json`

# COMMAND ----------

TABELAS = ["bronze_osm", "bronze_relatos", "bronze_clima", "prata_osm_elemento", "prata_relato", "prata_clima_hora",
           "prata_rejeitados", "fato_relato", "fato_elemento_via", "dim_celula", "dim_data", "dim_categoria",
           "dim_status", "dim_classe_elemento", "ouro_painel_celula"]
CONSULTAS = {
    "q1": "vw_q1_barreiras_abertas",
    "q2": "vw_q2_travessias_sem_rebaixamento",
    "q3": "vw_q3_cobertura_do_mapeamento",
    "q4": "vw_q4_impacto_fila_do_gestor",
    "q5": "vw_q5_relatos_por_dia_e_chuva",
}


def merges(tabela: str) -> list[dict]:
    return [{"tabela": tabela, "versao": h["version"], "quando": str(h["timestamp"]),
             "inseridas": int(h["operationMetrics"].get("numTargetRowsInserted", 0)),
             "atualizadas": int(h["operationMetrics"].get("numTargetRowsUpdated", 0))}
            for h in spark.sql(f"DESCRIBE HISTORY {ESQUEMA}.{tabela}").filter("operation = 'MERGE'")
                          .orderBy("version").collect()]


evidencias = {
    "gerado_em": agora,
    "contagens": {t: spark.table(f"{ESQUEMA}.{t}").count() for t in TABELAS},
    "bronze_controle": linhas(f"""
        SELECT dense_rank() OVER (ORDER BY executado_em) AS execucao,
               date_format(from_utc_timestamp(executado_em, 'America/Sao_Paulo'), 'dd/MM/yyyy HH:mm:ss') AS quando,
               tabela, linhas_antes, linhas_depois, arquivos_novos
        FROM {ESQUEMA}.bronze_controle_cargas ORDER BY executado_em, tabela"""),
    "bronze_arquivos": linhas(f"""
        SELECT 'osm' AS fonte, CAST(data_referencia AS STRING) AS dia, count(*) AS vezes FROM {ESQUEMA}.bronze_osm GROUP BY ALL
        UNION ALL SELECT 'relatos', CAST(data_referencia AS STRING), count(*) FROM {ESQUEMA}.bronze_relatos GROUP BY ALL
        UNION ALL SELECT 'clima', CAST(data_referencia AS STRING), count(*) FROM {ESQUEMA}.bronze_clima GROUP BY ALL
        ORDER BY fonte, dia"""),
    # regras de qualidade da execução mais recente da Prata
    "qualidade": linhas(f"""
        SELECT fonte, regra, descricao, total_avaliado, total_barrado FROM {ESQUEMA}.prata_qualidade
        WHERE executado_em = (SELECT max(executado_em) FROM {ESQUEMA}.prata_qualidade) ORDER BY regra, fonte"""),
    "merges": merges("prata_osm_elemento") + merges("prata_relato") + merges("prata_clima_hora"),
    "consultas": {q: linhas(f"SELECT * FROM {ESQUEMA}.{view}") for q, view in CONSULTAS.items()},
    "painel": {
        "data_referencia": str(referencia),
        "celulas_publicadas": len(celulas),
        "por_nivel": linhas(f"""
            SELECT nivel_atencao, count(*) AS quadriculas, sum(travessias) AS travessias,
                   sum(pontos_rebaixados) AS pontos_rebaixados, sum(relatos_abertos) AS relatos_abertos
            FROM {ESQUEMA}.ouro_painel_celula GROUP BY nivel_atencao ORDER BY nivel_atencao"""),
    },
    "lgpd": {
        "colunas_fato_relato": spark.table(f"{ESQUEMA}.fato_relato").columns,
        "exemplo_hash": linhas(f"SELECT concat(left(id_relato_hash, 16), '…') AS h FROM {ESQUEMA}.fato_relato LIMIT 1"),
    },
}
gravar("evidencias.json", evidencias)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Resumo desta execução

# COMMAND ----------

display(spark.createDataFrame([(t, n) for t, n in evidencias["contagens"].items()], "tabela STRING, linhas BIGINT"))
