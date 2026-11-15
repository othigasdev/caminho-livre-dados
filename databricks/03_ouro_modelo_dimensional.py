# Databricks notebook source
# MAGIC %md
# MAGIC # 03 - Ouro
# MAGIC
# MAGIC | Tabela | Tipo | Grão |
# MAGIC |---|---|---|
# MAGIC | `fato_relato` | fato | relato de barreira |
# MAGIC | `fato_elemento_via` | fato | elemento de pedestre do OpenStreetMap |
# MAGIC | `dim_celula` | dimensão | quadrícula de ~110 m × 100 m |
# MAGIC | `dim_data` | dimensão | dia, com chuva e temperatura |
# MAGIC | `dim_categoria` | dimensão | categoria da barreira |
# MAGIC | `dim_status` | dimensão | status do relato |
# MAGIC | `dim_classe_elemento` | dimensão | classe do elemento OSM |
# MAGIC | `ouro_painel_celula` | agregada | situação de cada quadrícula (usada pelo MVP) |
# MAGIC
# MAGIC LGPD na `fato_relato`: id em hash SHA-256 com sal (informado no widget), sem coordenada, sem descrição e horário reduzido a dia e hora.

# COMMAND ----------

dbutils.widgets.text("salt_hash", "", "Sal do hash")
SAL = dbutils.widgets.get("salt_hash").strip()
assert len(SAL) >= 16, "Preencha o widget salt_hash (16 ou mais caracteres)."

spark.createDataFrame([(SAL,)], "sal STRING").createOrReplaceTempView("segredo_sal")
spark.sql("USE workspace.caminho_livre")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Dimensões

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Quadrícula do piloto: 11 linhas x 15 colunas de 0,001 grau (o mesmo PILOT_BBOX do MVP).
# MAGIC CREATE OR REPLACE TABLE dim_celula
# MAGIC COMMENT 'Quadricula de ~110 m x 100 m do territorio do piloto' AS
# MAGIC SELECT CAST(l * 15 + c + 1 AS INT) AS sk_celula,
# MAGIC        concat('L', lpad(CAST(l AS STRING), 2, '0'), 'C', lpad(CAST(c AS STRING), 2, '0')) AS id_celula,
# MAGIC        l AS linha, c AS coluna,
# MAGIC        CAST((-235700 + l * 10) / 10000.0 AS DOUBLE)       AS lat_min,
# MAGIC        CAST((-235700 + (l + 1) * 10) / 10000.0 AS DOUBLE) AS lat_max,
# MAGIC        CAST((-466600 + c * 10) / 10000.0 AS DOUBLE)       AS lon_min,
# MAGIC        CAST((-466600 + (c + 1) * 10) / 10000.0 AS DOUBLE) AS lon_max
# MAGIC FROM (SELECT explode(sequence(0, 10)) AS l) CROSS JOIN (SELECT explode(sequence(0, 14)) AS c);
# MAGIC
# MAGIC -- Em qual quadrícula cai uma coordenada
# MAGIC CREATE OR REPLACE FUNCTION sk_celula_de(latitude DOUBLE, longitude DOUBLE)
# MAGIC RETURNS INT
# MAGIC COMMENT 'Chave da quadricula (dim_celula) que contem a coordenada'
# MAGIC RETURN CAST(least(greatest((CAST(round(latitude * 10000) AS INT) + 235700) div 10, 0), 10) * 15
# MAGIC           + least(greatest((CAST(round(longitude * 10000) AS INT) + 466600) div 10, 0), 14) + 1 AS INT);
# MAGIC
# MAGIC CREATE OR REPLACE TABLE dim_categoria COMMENT 'Categoria da barreira relatada no MVP' AS
# MAGIC SELECT * FROM VALUES
# MAGIC   (1, 'calcada_obstruida',  'Calçada obstruída'),
# MAGIC   (2, 'rampa_bloqueada',    'Rampa bloqueada ou ausente'),
# MAGIC   (3, 'travessia_barreira', 'Travessia com barreira')
# MAGIC AS t(sk_categoria, categoria, rotulo);
# MAGIC
# MAGIC CREATE OR REPLACE TABLE dim_status COMMENT 'Status do relato na fila do gestor' AS
# MAGIC SELECT * FROM VALUES
# MAGIC   (1, 'pendente',   'Pendente',   true,  false),
# MAGIC   (2, 'em_analise', 'Em análise', true,  true),
# MAGIC   (3, 'resolvido',  'Resolvido',  false, true),
# MAGIC   (4, 'descartado', 'Descartado', false, true)
# MAGIC AS t(sk_status, status, rotulo, aberto, revisado);
# MAGIC
# MAGIC CREATE OR REPLACE TABLE dim_classe_elemento COMMENT 'Classe do elemento de pedestre do OpenStreetMap' AS
# MAGIC SELECT * FROM VALUES
# MAGIC   (1, 'travessia',       'Travessia de pedestres (ponto)'),
# MAGIC   (2, 'meio_fio',        'Meio-fio mapeado (ponto)'),
# MAGIC   (3, 'faixa_travessia', 'Faixa de travessia (trecho)'),
# MAGIC   (4, 'calcada',         'Calçada (trecho)'),
# MAGIC   (5, 'via_pedestre',    'Outra via de pedestre (trecho)')
# MAGIC AS t(sk_classe, classe, rotulo);

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Um dia por linha: todo dia com clima coletado ou com relato criado (data no fuso de São Paulo)
# MAGIC CREATE OR REPLACE TABLE dim_data
# MAGIC COMMENT 'Calendario com a chuva e a temperatura do dia (Open-Meteo)' AS
# MAGIC WITH dias AS (
# MAGIC   SELECT CAST(from_utc_timestamp(criado_em, 'America/Sao_Paulo') AS DATE) AS data FROM prata_relato
# MAGIC   UNION
# MAGIC   SELECT data FROM prata_clima_hora
# MAGIC ),
# MAGIC clima AS (
# MAGIC   SELECT data, round(sum(precipitacao_mm), 1) AS chuva_mm, count_if(precipitacao_mm >= 0.1) AS horas_com_chuva,
# MAGIC          round(avg(temperatura_c), 1) AS temperatura_media_c
# MAGIC   FROM prata_clima_hora GROUP BY data
# MAGIC )
# MAGIC SELECT CAST(date_format(d.data, 'yyyyMMdd') AS INT) AS sk_data, d.data,
# MAGIC        element_at(array('domingo', 'segunda', 'terça', 'quarta', 'quinta', 'sexta', 'sábado'), dayofweek(d.data)) AS dia_semana,
# MAGIC        dayofweek(d.data) IN (1, 7) AS fim_de_semana,
# MAGIC        c.chuva_mm, c.horas_com_chuva, c.temperatura_media_c,
# MAGIC        CASE WHEN c.data IS NULL THEN 'sem_dado_de_clima'
# MAGIC             WHEN c.chuva_mm >= 1 THEN 'com_chuva'          -- 1 mm ou mais no dia
# MAGIC             ELSE 'sem_chuva' END AS condicao_chuva
# MAGIC FROM dias d LEFT JOIN clima c ON c.data = d.data;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Fatos

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE fato_relato
# MAGIC COMMENT 'Fato: um relato de barreira. LGPD: id em hash com sal, sem coordenada e sem texto livre' AS
# MAGIC SELECT
# MAGIC   sha2(concat(s.sal, r.id_relato), 256)               AS id_relato_hash,
# MAGIC   sk_celula_de(r.latitude, r.longitude)               AS sk_celula,
# MAGIC   cat.sk_categoria, st.sk_status,
# MAGIC   CAST(date_format(from_utc_timestamp(r.criado_em, 'America/Sao_Paulo'), 'yyyyMMdd') AS INT) AS sk_data_criacao,
# MAGIC   hour(from_utc_timestamp(r.criado_em, 'America/Sao_Paulo'))                                 AS hora_criacao,
# MAGIC   r.fonte,
# MAGIC   r.descricao_mascarada IS NOT NULL                   AS tem_descricao,
# MAGIC   CASE WHEN st.revisado THEN round((unix_timestamp(r.atualizado_em) - unix_timestamp(r.criado_em)) / 3600.0, 2) END AS horas_ate_revisao,
# MAGIC   CAST(st.aberto AS INT)               AS fl_aberto,
# MAGIC   CAST(st.revisado AS INT)             AS fl_revisado,
# MAGIC   CAST(r.status = 'resolvido' AS INT)  AS fl_resolvido,
# MAGIC   1                                    AS qtd_relatos
# MAGIC FROM prata_relato r
# MAGIC CROSS JOIN segredo_sal s
# MAGIC JOIN dim_categoria cat ON cat.categoria = r.categoria
# MAGIC JOIN dim_status st     ON st.status = r.status;
# MAGIC
# MAGIC CREATE OR REPLACE TABLE fato_elemento_via
# MAGIC COMMENT 'Fato: um elemento de pedestre do OpenStreetMap, com os atributos de acessibilidade mapeados' AS
# MAGIC SELECT
# MAGIC   e.tipo_osm, e.osm_id,
# MAGIC   sk_celula_de(e.latitude, e.longitude) AS sk_celula,
# MAGIC   cl.sk_classe,
# MAGIC   e.tipo_travessia, e.rebaixamento, e.piso_tatil, e.sinal_sonoro,
# MAGIC   CAST(e.rebaixamento <> 'nao_mapeado' AS INT) AS fl_rebaixamento_mapeado,
# MAGIC   CAST(e.rebaixamento = 'acessivel' AS INT)    AS fl_rebaixamento_acessivel,
# MAGIC   CAST(e.piso_tatil <> 'nao_mapeado' AS INT)   AS fl_piso_tatil_mapeado,
# MAGIC   CAST(e.piso_tatil = 'sim' AS INT)            AS fl_piso_tatil_sim,
# MAGIC   CAST(e.sinal_sonoro = 'sim' AS INT)          AS fl_sinal_sonoro_sim,
# MAGIC   1                                            AS qtd_elementos
# MAGIC FROM prata_osm_elemento e
# MAGIC JOIN dim_classe_elemento cl ON cl.classe = e.classe;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Painel do MVP: `ouro_painel_celula`
# MAGIC - `alta`: 2+ relatos abertos, ou relato aberto sem rebaixamento mapeado
# MAGIC - `media`: 1 relato aberto, ou travessia sem rebaixamento mapeado
# MAGIC - `baixa`: travessia com rebaixamento mapeado e sem relato aberto
# MAGIC - `sem_travessia`: nada a avaliar

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ouro_painel_celula
# MAGIC COMMENT 'Situacao de cada quadricula, usada pelo MVP em /api/painel' AS
# MAGIC WITH infra AS (
# MAGIC   SELECT f.sk_celula,
# MAGIC          sum(CASE WHEN cl.classe = 'travessia' THEN 1 ELSE 0 END)                                              AS travessias,
# MAGIC          sum(CASE WHEN cl.classe IN ('travessia', 'meio_fio') THEN f.fl_rebaixamento_acessivel ELSE 0 END)    AS pontos_rebaixados,
# MAGIC          sum(f.fl_piso_tatil_sim)                                                                              AS pontos_piso_tatil,
# MAGIC          sum(CASE WHEN cl.classe = 'travessia' AND f.fl_rebaixamento_mapeado = 0 AND f.fl_piso_tatil_mapeado = 0
# MAGIC                   THEN 1 ELSE 0 END)                                                                           AS travessias_sem_dado
# MAGIC   FROM fato_elemento_via f JOIN dim_classe_elemento cl ON cl.sk_classe = f.sk_classe
# MAGIC   GROUP BY f.sk_celula
# MAGIC ),
# MAGIC relatos AS (
# MAGIC   SELECT sk_celula, sum(qtd_relatos) AS relatos_total, sum(fl_aberto) AS relatos_abertos,
# MAGIC          sum(CASE WHEN fonte = 'sintetico' THEN fl_aberto ELSE 0 END) AS relatos_abertos_demonstracao
# MAGIC   FROM fato_relato GROUP BY sk_celula
# MAGIC ),
# MAGIC junto AS (
# MAGIC   SELECT c.id_celula, c.lat_min, c.lat_max, c.lon_min, c.lon_max,
# MAGIC          coalesce(i.travessias, 0) AS travessias, coalesce(i.pontos_rebaixados, 0) AS pontos_rebaixados,
# MAGIC          coalesce(i.pontos_piso_tatil, 0) AS pontos_piso_tatil, coalesce(i.travessias_sem_dado, 0) AS travessias_sem_dado,
# MAGIC          coalesce(r.relatos_total, 0) AS relatos_total, coalesce(r.relatos_abertos, 0) AS relatos_abertos,
# MAGIC          coalesce(r.relatos_abertos_demonstracao, 0) AS relatos_abertos_demonstracao
# MAGIC   FROM dim_celula c
# MAGIC   LEFT JOIN infra i   ON i.sk_celula = c.sk_celula
# MAGIC   LEFT JOIN relatos r ON r.sk_celula = c.sk_celula
# MAGIC )
# MAGIC SELECT *,
# MAGIC   CASE WHEN relatos_abertos >= 2 OR (relatos_abertos >= 1 AND pontos_rebaixados = 0) THEN 'alta'
# MAGIC        WHEN relatos_abertos = 1 OR (travessias > 0 AND pontos_rebaixados = 0)        THEN 'media'
# MAGIC        WHEN travessias > 0                                                          THEN 'baixa'
# MAGIC        ELSE 'sem_travessia' END AS nivel_atencao,
# MAGIC   (SELECT max(data_referencia) FROM prata_osm_elemento) AS data_referencia
# MAGIC FROM junto;
# MAGIC
# MAGIC SELECT nivel_atencao, count(*) AS quadriculas, sum(travessias) AS travessias, sum(pontos_rebaixados) AS pontos_rebaixados,
# MAGIC        sum(relatos_abertos) AS relatos_abertos
# MAGIC FROM ouro_painel_celula GROUP BY nivel_atencao ORDER BY nivel_atencao;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Conferência de LGPD

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT concat(left(id_relato_hash, 16), '…') AS id_relato_hash, sk_celula, sk_categoria, sk_status, sk_data_criacao,
# MAGIC        hora_criacao, fonte, tem_descricao, horas_ate_revisao, fl_aberto, fl_resolvido
# MAGIC FROM fato_relato ORDER BY sk_data_criacao, hora_criacao;

# COMMAND ----------

colunas = set(spark.table("workspace.caminho_livre.fato_relato").columns)
proibidas = {"id_relato", "latitude", "longitude", "descricao_mascarada", "descricao_sanitizada", "criado_em", "atualizado_em"}
assert not (colunas & proibidas), f"Dado pessoal na Ouro: {colunas & proibidas}"
originais = spark.sql("""
  SELECT count(*) AS n FROM workspace.caminho_livre.fato_relato f
  JOIN workspace.caminho_livre.prata_relato r ON f.id_relato_hash = r.id_relato""").first()["n"]
assert originais == 0, "Há id de relato sem hash na Ouro"
print("LGPD conferida: fato_relato sem id original, sem coordenada e sem texto livre.")
print("Colunas:", ", ".join(sorted(colunas)))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Conferência do modelo

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 'fato_relato' AS tabela, 'fato' AS papel, count(*) AS linhas FROM fato_relato
# MAGIC UNION ALL SELECT 'fato_elemento_via', 'fato', count(*) FROM fato_elemento_via
# MAGIC UNION ALL SELECT 'dim_celula', 'dimensão', count(*) FROM dim_celula
# MAGIC UNION ALL SELECT 'dim_data', 'dimensão', count(*) FROM dim_data
# MAGIC UNION ALL SELECT 'dim_categoria', 'dimensão', count(*) FROM dim_categoria
# MAGIC UNION ALL SELECT 'dim_status', 'dimensão', count(*) FROM dim_status
# MAGIC UNION ALL SELECT 'dim_classe_elemento', 'dimensão', count(*) FROM dim_classe_elemento
# MAGIC UNION ALL SELECT 'ouro_painel_celula', 'agregada para o MVP', count(*) FROM ouro_painel_celula;
