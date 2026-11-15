# Databricks notebook source
# MAGIC %md
# MAGIC # 02 · Prata: tipos, nulos, regras de qualidade e MERGE
# MAGIC Para cada fonte, este notebook:
# MAGIC 1. abre o JSON bruto da Bronze e dá **tipo** a cada campo;
# MAGIC 2. **trata nulos e inválidos** (padroniza domínios; o que falta no OpenStreetMap vira `nao_mapeado`, nunca "não existe");
# MAGIC 3. aplica as **regras de qualidade** e conta quantos registros cada uma barrou;
# MAGIC 4. faz **MERGE pela chave**, para que o mesmo registro vindo em vários lotes exista uma única vez.
# MAGIC
# MAGIC | Regra | Fonte | O que barra |
# MAGIC |---|---|---|
# MAGIC | Q1 | OSM | elemento sem coordenada ou com o centro fora do território do piloto |
# MAGIC | Q2 | OSM | via privada, interna (dentro de edifício) ou de classe não reconhecida |
# MAGIC | Q3 | Relatos | relato sem id/data, com categoria ou status fora do domínio, ou fora do piloto |
# MAGIC | Q4 | Clima | hora sem medição, chuva negativa ou temperatura fora de -5 °C a 45 °C |
# MAGIC | Q5 | Todas | registro repetido entre lotes (mesma chave): fica só a versão mais recente |
# MAGIC
# MAGIC Os barrados por Q1 a Q4 não são apagados: vão para `prata_rejeitados`, com a regra que os barrou.
# MAGIC O notebook pode ser rodado quantas vezes for preciso: o MERGE não duplica nada.

# COMMAND ----------

# MAGIC %sql
# MAGIC USE workspace.caminho_livre;
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS prata_osm_elemento (
# MAGIC   tipo_osm STRING COMMENT 'node ou way', osm_id BIGINT,
# MAGIC   latitude DOUBLE, longitude DOUBLE,
# MAGIC   classe STRING COMMENT 'travessia, meio_fio, faixa_travessia, calcada ou via_pedestre',
# MAGIC   rebaixamento STRING COMMENT 'acessivel, barreira, outro ou nao_mapeado (tag kerb)',
# MAGIC   piso_tatil STRING COMMENT 'sim, nao, parcial ou nao_mapeado (tag tactile_paving)',
# MAGIC   tipo_travessia STRING COMMENT 'semaforizada, faixa_sem_semaforo, sem_faixa, outro ou nao_mapeado',
# MAGIC   sinal_sonoro STRING COMMENT 'sim, nao ou nao_mapeado (tag traffic_signals:sound)',
# MAGIC   cadeira_de_rodas STRING COMMENT 'sim, limitado, nao ou nao_mapeado (tag wheelchair)',
# MAGIC   superficie STRING,
# MAGIC   data_referencia DATE COMMENT 'dia do lote mais recente em que o elemento apareceu',
# MAGIC   atualizado_na_prata TIMESTAMP
# MAGIC ) COMMENT 'Elementos de pedestre do OpenStreetMap no piloto. Chave: (tipo_osm, osm_id)';
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS prata_relato (
# MAGIC   id_relato STRING, criado_em TIMESTAMP, atualizado_em TIMESTAMP,
# MAGIC   latitude DOUBLE COMMENT 'ja arredondada pelo MVP (~11 m)', longitude DOUBLE,
# MAGIC   categoria STRING, status STRING, fonte STRING COMMENT 'cidadao ou sintetico (demonstracao)',
# MAGIC   descricao_mascarada STRING COMMENT 'texto livre com e-mail, telefone e CPF trocados por [removido]',
# MAGIC   data_referencia DATE, atualizado_na_prata TIMESTAMP
# MAGIC ) COMMENT 'Relatos de barreiras vindos do MVP. Chave: id_relato';
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS prata_clima_hora (
# MAGIC   data_hora TIMESTAMP COMMENT 'hora local de Sao Paulo', data DATE,
# MAGIC   temperatura_c DOUBLE, precipitacao_mm DOUBLE,
# MAGIC   data_referencia DATE, atualizado_na_prata TIMESTAMP
# MAGIC ) COMMENT 'Clima hora a hora no centro do piloto (Open-Meteo). Chave: data_hora';
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS prata_qualidade (
# MAGIC   executado_em TIMESTAMP, fonte STRING, regra STRING, descricao STRING,
# MAGIC   total_avaliado BIGINT, total_barrado BIGINT
# MAGIC ) COMMENT 'Resultado das regras de qualidade a cada execucao da Prata';

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. OpenStreetMap
# MAGIC Cada arquivo traz a lista `elements`. A tag de cada elemento é um mapa de chave e valor que muda de elemento para elemento,
# MAGIC por isso é lida como `MAP<STRING, STRING>` e cada atributo de acessibilidade vira uma coluna de domínio fechado.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW osm_tipado AS
# MAGIC WITH explodido AS (
# MAGIC   SELECT e.type AS tipo_osm, e.id AS osm_id,
# MAGIC          coalesce(e.lat, e.center.lat) AS latitude,      -- nó tem lat/lon; via tem o centro
# MAGIC          coalesce(e.lon, e.center.lon) AS longitude,
# MAGIC          e.tags AS tags, b.data_referencia, b.arquivo_origem
# MAGIC   FROM workspace.caminho_livre.bronze_osm b
# MAGIC   LATERAL VIEW explode(from_json(b.elements,
# MAGIC     'ARRAY<STRUCT<type: STRING, id: BIGINT, lat: DOUBLE, lon: DOUBLE, center: STRUCT<lat: DOUBLE, lon: DOUBLE>, tags: MAP<STRING, STRING>>>'
# MAGIC   )) t AS e
# MAGIC ),
# MAGIC etiquetas AS (
# MAGIC   SELECT *,
# MAGIC          lower(trim(try_element_at(tags, 'highway')))               AS t_highway,
# MAGIC          lower(trim(try_element_at(tags, 'footway')))               AS t_footway,
# MAGIC          lower(trim(try_element_at(tags, 'kerb')))                  AS t_kerb,
# MAGIC          lower(trim(try_element_at(tags, 'tactile_paving')))        AS t_tatil,
# MAGIC          lower(trim(try_element_at(tags, 'crossing')))              AS t_crossing,
# MAGIC          lower(trim(try_element_at(tags, 'crossing:signals')))      AS t_semaforo,
# MAGIC          lower(trim(try_element_at(tags, 'traffic_signals:sound'))) AS t_som,
# MAGIC          lower(trim(try_element_at(tags, 'wheelchair')))            AS t_cadeira,
# MAGIC          lower(trim(try_element_at(tags, 'access')))                AS t_acesso
# MAGIC   FROM explodido
# MAGIC )
# MAGIC SELECT
# MAGIC   tipo_osm, osm_id, round(latitude, 7) AS latitude, round(longitude, 7) AS longitude,
# MAGIC   CASE WHEN tipo_osm = 'node' AND t_highway = 'crossing' THEN 'travessia'
# MAGIC        WHEN tipo_osm = 'node' AND t_kerb IS NOT NULL      THEN 'meio_fio'
# MAGIC        WHEN tipo_osm = 'way'  AND t_footway = 'crossing'  THEN 'faixa_travessia'
# MAGIC        WHEN tipo_osm = 'way'  AND t_footway = 'sidewalk'  THEN 'calcada'
# MAGIC        WHEN tipo_osm = 'way'  AND t_highway IN ('footway', 'pedestrian') THEN 'via_pedestre'
# MAGIC        ELSE 'outro' END AS classe,
# MAGIC   CASE WHEN t_kerb IS NULL THEN 'nao_mapeado'
# MAGIC        WHEN t_kerb IN ('lowered', 'flush', 'no') THEN 'acessivel'      -- rebaixado, nivelado ou sem degrau
# MAGIC        WHEN t_kerb IN ('raised', 'rolled', 'regular', 'yes') THEN 'barreira'
# MAGIC        ELSE 'outro' END AS rebaixamento,
# MAGIC   CASE WHEN t_tatil IS NULL THEN 'nao_mapeado'
# MAGIC        WHEN t_tatil = 'yes' THEN 'sim'
# MAGIC        WHEN t_tatil = 'no'  THEN 'nao'
# MAGIC        ELSE 'parcial' END AS piso_tatil,
# MAGIC   CASE WHEN t_crossing = 'traffic_signals' OR t_semaforo = 'yes' THEN 'semaforizada'
# MAGIC        WHEN t_crossing IN ('marked', 'zebra', 'uncontrolled')    THEN 'faixa_sem_semaforo'
# MAGIC        WHEN t_crossing IN ('unmarked', 'no')                     THEN 'sem_faixa'
# MAGIC        WHEN t_crossing IS NULL THEN 'nao_mapeado'
# MAGIC        ELSE 'outro' END AS tipo_travessia,
# MAGIC   CASE WHEN t_som IS NULL THEN 'nao_mapeado' WHEN t_som = 'yes' THEN 'sim' ELSE 'nao' END AS sinal_sonoro,
# MAGIC   CASE WHEN t_cadeira IS NULL THEN 'nao_mapeado' WHEN t_cadeira = 'yes' THEN 'sim'
# MAGIC        WHEN t_cadeira = 'limited' THEN 'limitado' ELSE 'nao' END AS cadeira_de_rodas,
# MAGIC   coalesce(lower(trim(try_element_at(tags, 'surface'))), 'nao_mapeado') AS superficie,
# MAGIC   (coalesce(t_acesso, '') IN ('private', 'no', 'customers')
# MAGIC     OR coalesce(lower(try_element_at(tags, 'indoor')), 'no') <> 'no'
# MAGIC     OR coalesce(lower(try_element_at(tags, 'tunnel')), '') = 'building_passage'
# MAGIC     OR coalesce(try_element_at(tags, 'level'), '0') <> '0') AS via_restrita,
# MAGIC   data_referencia, arquivo_origem
# MAGIC FROM etiquetas;
# MAGIC
# MAGIC CREATE OR REPLACE TEMP VIEW osm_avaliado AS
# MAGIC SELECT *,
# MAGIC   CASE WHEN osm_id IS NULL OR latitude IS NULL OR longitude IS NULL
# MAGIC          OR latitude  NOT BETWEEN -23.5700 AND -23.5590
# MAGIC          OR longitude NOT BETWEEN -46.6600 AND -46.6450 THEN 'Q1_osm_fora_do_piloto'
# MAGIC        WHEN via_restrita OR classe = 'outro'            THEN 'Q2_osm_via_privada_ou_interna'
# MAGIC   END AS regra_barrada
# MAGIC FROM osm_tipado;
# MAGIC
# MAGIC -- Q5: o mesmo elemento vem em todos os lotes; fica a versão do lote mais recente
# MAGIC CREATE OR REPLACE TEMP VIEW lote_prata_osm AS
# MAGIC SELECT tipo_osm, osm_id, latitude, longitude, classe, rebaixamento, piso_tatil, tipo_travessia,
# MAGIC        sinal_sonoro, cadeira_de_rodas, superficie, data_referencia,
# MAGIC        current_timestamp() AS atualizado_na_prata
# MAGIC FROM osm_avaliado
# MAGIC WHERE regra_barrada IS NULL
# MAGIC QUALIFY row_number() OVER (PARTITION BY tipo_osm, osm_id ORDER BY data_referencia DESC, arquivo_origem DESC) = 1;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Relatos do MVP
# MAGIC O MVP já arredonda a coordenada e limpa a descrição antes de gravar. A Prata repete a máscara de e-mail, CPF e telefone
# MAGIC (defesa em profundidade: o lakehouse não depende de a origem ter acertado).

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW relatos_tipado AS
# MAGIC SELECT
# MAGIC   r.id AS id_relato,
# MAGIC   try_cast(r.criado_em AS TIMESTAMP) AS criado_em,
# MAGIC   coalesce(try_cast(r.atualizado_em AS TIMESTAMP), try_cast(r.criado_em AS TIMESTAMP)) AS atualizado_em,
# MAGIC   r.latitude, r.longitude,
# MAGIC   lower(trim(r.categoria)) AS categoria,
# MAGIC   lower(trim(r.status))    AS status,
# MAGIC   coalesce(lower(trim(r.fonte)), 'cidadao') AS fonte,
# MAGIC   nullif(trim(regexp_replace(regexp_replace(regexp_replace(r.descricao_sanitizada,
# MAGIC       r'[\w.+-]+@[\w-]+\.[\w.]+', '[removido]'),          -- e-mail
# MAGIC       r'\d{3}\.?\d{3}\.?\d{3}-?\d{2}', '[removido]'),      -- CPF
# MAGIC       r'\+?\d[\d\s().-]{7,}\d', '[removido]')), '')        -- telefone
# MAGIC     AS descricao_mascarada,
# MAGIC   b.data_referencia, b.arquivo_origem
# MAGIC FROM workspace.caminho_livre.bronze_relatos b
# MAGIC LATERAL VIEW explode(from_json(b.itens,
# MAGIC   'ARRAY<STRUCT<id: STRING, criado_em: STRING, atualizado_em: STRING, latitude: DOUBLE, longitude: DOUBLE, categoria: STRING, descricao_sanitizada: STRING, status: STRING, fonte: STRING>>'
# MAGIC )) t AS r;
# MAGIC
# MAGIC CREATE OR REPLACE TEMP VIEW relatos_avaliado AS
# MAGIC SELECT *,
# MAGIC   CASE WHEN id_relato IS NULL OR criado_em IS NULL
# MAGIC          OR categoria IS NULL OR categoria NOT IN ('calcada_obstruida', 'rampa_bloqueada', 'travessia_barreira')
# MAGIC          OR status IS NULL    OR status NOT IN ('pendente', 'em_analise', 'resolvido', 'descartado')
# MAGIC          OR latitude IS NULL  OR latitude  NOT BETWEEN -23.5700 AND -23.5590
# MAGIC          OR longitude IS NULL OR longitude NOT BETWEEN -46.6600 AND -46.6450 THEN 'Q3_relato_invalido'
# MAGIC   END AS regra_barrada
# MAGIC FROM relatos_tipado;
# MAGIC
# MAGIC -- Q5: o relato vem em todo lote (a API devolve a fila inteira); fica o estado mais recente
# MAGIC CREATE OR REPLACE TEMP VIEW lote_prata_relato AS
# MAGIC SELECT id_relato, criado_em, atualizado_em, latitude, longitude, categoria, status, fonte,
# MAGIC        descricao_mascarada, data_referencia, current_timestamp() AS atualizado_na_prata
# MAGIC FROM relatos_avaliado
# MAGIC WHERE regra_barrada IS NULL
# MAGIC QUALIFY row_number() OVER (PARTITION BY id_relato ORDER BY data_referencia DESC, atualizado_em DESC) = 1;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Clima
# MAGIC O Open-Meteo devolve listas paralelas (`time`, `temperature_2m`, `precipitation`). O `arrays_zip` junta as três
# MAGIC e o `explode` gera uma linha por hora.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW clima_tipado AS
# MAGIC WITH listas AS (
# MAGIC   SELECT h.time AS horas, h.temperature_2m AS temperaturas, h.precipitation AS chuvas, data_referencia, arquivo_origem
# MAGIC   FROM (SELECT from_json(hourly, 'STRUCT<time: ARRAY<STRING>, temperature_2m: ARRAY<DOUBLE>, precipitation: ARRAY<DOUBLE>>') AS h,
# MAGIC                data_referencia, arquivo_origem
# MAGIC         FROM workspace.caminho_livre.bronze_clima)
# MAGIC )
# MAGIC SELECT
# MAGIC   coalesce(try_cast(z.horas AS TIMESTAMP), try_cast(concat(z.horas, ':00') AS TIMESTAMP)) AS data_hora,
# MAGIC   z.temperaturas AS temperatura_c,
# MAGIC   z.chuvas       AS precipitacao_mm,
# MAGIC   data_referencia, arquivo_origem
# MAGIC FROM listas
# MAGIC LATERAL VIEW explode(arrays_zip(horas, temperaturas, chuvas)) t AS z;
# MAGIC
# MAGIC CREATE OR REPLACE TEMP VIEW clima_avaliado AS
# MAGIC SELECT *,
# MAGIC   CASE WHEN data_hora IS NULL
# MAGIC          OR precipitacao_mm IS NULL OR precipitacao_mm < 0
# MAGIC          OR temperatura_c IS NULL   OR temperatura_c NOT BETWEEN -5 AND 45 THEN 'Q4_clima_medida_invalida'
# MAGIC   END AS regra_barrada
# MAGIC FROM clima_tipado;
# MAGIC
# MAGIC CREATE OR REPLACE TEMP VIEW lote_prata_clima AS
# MAGIC SELECT data_hora, CAST(data_hora AS DATE) AS data, temperatura_c, precipitacao_mm, data_referencia,
# MAGIC        current_timestamp() AS atualizado_na_prata
# MAGIC FROM clima_avaliado
# MAGIC WHERE regra_barrada IS NULL
# MAGIC QUALIFY row_number() OVER (PARTITION BY data_hora ORDER BY data_referencia DESC, arquivo_origem DESC) = 1;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Regras de qualidade: total barrado por regra
# MAGIC A contagem é gravada em `prata_qualidade` a cada execução. A tabela abaixo mostra a execução atual.

# COMMAND ----------

REGRAS = [
    # (fonte, regra, descrição, view avaliada, view do lote já sem duplicatas)
    ("osm", "Q1_osm_fora_do_piloto", "Elemento sem coordenada ou com o centro fora do território do piloto", "osm_avaliado", None),
    ("osm", "Q2_osm_via_privada_ou_interna", "Via privada, interna ou de classe não reconhecida", "osm_avaliado", None),
    ("relatos", "Q3_relato_invalido", "Relato sem id/data, com domínio inválido ou fora do piloto", "relatos_avaliado", None),
    ("clima", "Q4_clima_medida_invalida", "Hora sem medição, chuva negativa ou temperatura fora de -5 a 45 °C", "clima_avaliado", None),
    ("osm", "Q5_duplicado_entre_lotes", "Mesma chave em mais de um lote: fica a versão mais recente", "osm_avaliado", "lote_prata_osm"),
    ("relatos", "Q5_duplicado_entre_lotes", "Mesma chave em mais de um lote: fica a versão mais recente", "relatos_avaliado", "lote_prata_relato"),
    ("clima", "Q5_duplicado_entre_lotes", "Mesma chave em mais de um lote: fica a versão mais recente", "clima_avaliado", "lote_prata_clima"),
]

linhas = []
for fonte, regra, descricao, avaliada, lote in REGRAS:
    if lote is None:  # regra de validade: avaliada sobre tudo o que veio da Bronze
        avaliado = spark.table(avaliada).count()
        barrado = spark.table(avaliada).filter(f"regra_barrada = '{regra}'").count()
    else:             # duplicidade: avaliada só sobre os registros válidos
        avaliado = spark.table(avaliada).filter("regra_barrada IS NULL").count()
        barrado = avaliado - spark.table(lote).count()
    linhas.append((fonte, regra, descricao, avaliado, barrado))

qualidade = spark.createDataFrame(
    linhas, "fonte STRING, regra STRING, descricao STRING, total_avaliado BIGINT, total_barrado BIGINT")
qualidade.selectExpr("current_timestamp() AS executado_em", "*") \
    .write.mode("append").saveAsTable("workspace.caminho_livre.prata_qualidade")
display(qualidade.orderBy("regra", "fonte"))

# COMMAND ----------

# MAGIC %md
# MAGIC Os registros barrados pelas regras de validade ficam em quarentena, com a regra e o arquivo de origem, para auditoria.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE workspace.caminho_livre.prata_rejeitados
# MAGIC COMMENT 'Quarentena: registros barrados pelas regras Q1 a Q4 (recriada a cada execucao a partir da Bronze)' AS
# MAGIC SELECT 'osm' AS fonte, regra_barrada AS regra, concat(tipo_osm, '/', osm_id) AS chave,
# MAGIC        concat('classe=', classe, ' lat=', latitude, ' lon=', longitude) AS detalhe, data_referencia, arquivo_origem
# MAGIC FROM osm_avaliado WHERE regra_barrada IS NOT NULL
# MAGIC UNION ALL
# MAGIC SELECT 'relatos', regra_barrada, id_relato,
# MAGIC        concat('categoria=', coalesce(categoria, 'nulo'), ' status=', coalesce(status, 'nulo')), data_referencia, arquivo_origem
# MAGIC FROM relatos_avaliado WHERE regra_barrada IS NOT NULL
# MAGIC UNION ALL
# MAGIC SELECT 'clima', regra_barrada, CAST(data_hora AS STRING),
# MAGIC        concat('chuva=', coalesce(CAST(precipitacao_mm AS STRING), 'nulo'), ' temp=', coalesce(CAST(temperatura_c AS STRING), 'nulo')),
# MAGIC        data_referencia, arquivo_origem
# MAGIC FROM clima_avaliado WHERE regra_barrada IS NOT NULL;
# MAGIC
# MAGIC SELECT fonte, regra, count(*) AS registros_em_quarentena FROM workspace.caminho_livre.prata_rejeitados GROUP BY ALL ORDER BY regra;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. MERGE pela chave
# MAGIC Chave nova entra (`INSERT`). Chave que já existe só é atualizada se o lote for **mais recente** que o que está gravado.
# MAGIC Por isso, rodar o notebook de novo sem lote novo resulta em 0 linhas inseridas e 0 atualizadas.

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO workspace.caminho_livre.prata_osm_elemento AS alvo
# MAGIC USING lote_prata_osm AS novo
# MAGIC ON alvo.tipo_osm = novo.tipo_osm AND alvo.osm_id = novo.osm_id
# MAGIC WHEN MATCHED AND novo.data_referencia > alvo.data_referencia THEN UPDATE SET *
# MAGIC WHEN NOT MATCHED THEN INSERT *;

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO workspace.caminho_livre.prata_relato AS alvo
# MAGIC USING lote_prata_relato AS novo
# MAGIC ON alvo.id_relato = novo.id_relato
# MAGIC WHEN MATCHED AND novo.data_referencia > alvo.data_referencia THEN UPDATE SET *
# MAGIC WHEN NOT MATCHED THEN INSERT *;

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO workspace.caminho_livre.prata_clima_hora AS alvo
# MAGIC USING lote_prata_clima AS novo
# MAGIC ON alvo.data_hora = novo.data_hora
# MAGIC WHEN MATCHED AND novo.data_referencia > alvo.data_referencia THEN UPDATE SET *
# MAGIC WHEN NOT MATCHED THEN INSERT *;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Conferência da Prata
# MAGIC Total de linhas, total de chaves distintas (têm de ser iguais) e o histórico de MERGE gravado pelo Delta.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 'prata_osm_elemento' AS tabela, count(*) AS linhas, count(DISTINCT tipo_osm, osm_id) AS chaves_distintas FROM workspace.caminho_livre.prata_osm_elemento
# MAGIC UNION ALL SELECT 'prata_relato', count(*), count(DISTINCT id_relato) FROM workspace.caminho_livre.prata_relato
# MAGIC UNION ALL SELECT 'prata_clima_hora', count(*), count(DISTINCT data_hora) FROM workspace.caminho_livre.prata_clima_hora;

# COMMAND ----------

from functools import reduce
from pyspark.sql import functions as F

# Histórico do Delta: cada MERGE fica registrado com o número de linhas inseridas e atualizadas
historicos = [
    spark.sql(f"DESCRIBE HISTORY workspace.caminho_livre.{tabela}")
        .filter("operation = 'MERGE'")
        .select(F.lit(tabela).alias("tabela"), F.col("version").alias("versao"),
                F.date_format(F.from_utc_timestamp("timestamp", "America/Sao_Paulo"), "dd/MM HH:mm").alias("quando"),
                F.col("operationMetrics")["numTargetRowsInserted"].cast("bigint").alias("inseridas"),
                F.col("operationMetrics")["numTargetRowsUpdated"].cast("bigint").alias("atualizadas"))
    for tabela in ("prata_osm_elemento", "prata_relato", "prata_clima_hora")
]
display(reduce(lambda a, b: a.unionByName(b), historicos).orderBy("tabela", "versao"))
