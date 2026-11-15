-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 04 - Consultas
-- MAGIC
-- MAGIC | # | Pergunta |
-- MAGIC |---|---|
-- MAGIC | Q1 | Onde há barreiras relatadas que continuam abertas? |
-- MAGIC | Q2 | Em quais quadras há travessia sem nenhum rebaixamento mapeado? |
-- MAGIC | Q3 | Quanto do território já tem dado de acessibilidade mapeado? |
-- MAGIC | Q4 | Os relatos estão sendo revisados e resolvidos? Em quanto tempo? (impacto) |
-- MAGIC | Q5 | Em que dias os relatos chegam, e a chuva muda isso? |

-- COMMAND ----------

USE workspace.caminho_livre;

-- COMMAND ----------

-- Q1: Onde há barreiras relatadas que continuam abertas?
CREATE OR REPLACE VIEW vw_q1_barreiras_abertas AS
SELECT c.id_celula                                        AS quadricula,
       cat.rotulo                                         AS categoria,
       count(*)                                           AS relatos_abertos,
       sum(CASE WHEN f.fonte = 'sintetico' THEN 1 ELSE 0 END) AS de_demonstracao,
       min(d.data)                                        AS aberto_desde,
       datediff(current_date(), min(d.data))              AS dias_do_mais_antigo
FROM fato_relato f
JOIN dim_status s      ON s.sk_status = f.sk_status
JOIN dim_celula c      ON c.sk_celula = f.sk_celula
JOIN dim_categoria cat ON cat.sk_categoria = f.sk_categoria
JOIN dim_data d        ON d.sk_data = f.sk_data_criacao
WHERE s.aberto
GROUP BY c.id_celula, cat.rotulo
ORDER BY relatos_abertos DESC, aberto_desde, quadricula;

SELECT * FROM vw_q1_barreiras_abertas;

-- COMMAND ----------

-- Q2: Em quais quadras há travessia sem nenhum rebaixamento mapeado?
CREATE OR REPLACE VIEW vw_q2_travessias_sem_rebaixamento AS
WITH por_quadricula AS (
  SELECT c.id_celula AS quadricula,
         sum(CASE WHEN cl.classe = 'travessia' THEN 1 ELSE 0 END)                                           AS travessias,
         sum(CASE WHEN cl.classe IN ('travessia', 'meio_fio') THEN f.fl_rebaixamento_acessivel ELSE 0 END) AS pontos_rebaixados,
         sum(f.fl_piso_tatil_sim)                                                                           AS pontos_piso_tatil
  FROM fato_elemento_via f
  JOIN dim_celula c           ON c.sk_celula = f.sk_celula
  JOIN dim_classe_elemento cl ON cl.sk_classe = f.sk_classe
  GROUP BY c.id_celula
  HAVING sum(CASE WHEN cl.classe = 'travessia' THEN 1 ELSE 0 END) > 0
)
SELECT quadricula, travessias, pontos_rebaixados, pontos_piso_tatil,
       count(*) OVER ()                                             AS quadriculas_sem_rebaixamento,
       (SELECT count(*) FROM por_quadricula)                        AS quadriculas_com_travessia
FROM por_quadricula
WHERE pontos_rebaixados = 0
ORDER BY travessias DESC, quadricula
LIMIT 15;

SELECT * FROM vw_q2_travessias_sem_rebaixamento;

-- COMMAND ----------

-- Q3: Quanto do território já tem dado de acessibilidade mapeado?
CREATE OR REPLACE VIEW vw_q3_cobertura_do_mapeamento AS
SELECT cl.rotulo                                                        AS classe_do_elemento,
       sum(f.qtd_elementos)                                             AS elementos,
       sum(f.fl_rebaixamento_mapeado)                                   AS com_rebaixamento_informado,
       round(100.0 * sum(f.fl_rebaixamento_mapeado) / sum(f.qtd_elementos), 1) AS pct_rebaixamento_informado,
       sum(f.fl_rebaixamento_acessivel)                                 AS rebaixamento_acessivel,
       sum(f.fl_piso_tatil_mapeado)                                     AS com_piso_tatil_informado,
       round(100.0 * sum(f.fl_piso_tatil_mapeado) / sum(f.qtd_elementos), 1)   AS pct_piso_tatil_informado,
       sum(f.fl_piso_tatil_sim)                                         AS com_piso_tatil
FROM fato_elemento_via f
JOIN dim_classe_elemento cl ON cl.sk_classe = f.sk_classe
GROUP BY cl.sk_classe, cl.rotulo
ORDER BY cl.sk_classe;

SELECT * FROM vw_q3_cobertura_do_mapeamento;

-- COMMAND ----------

-- Q4 (IMPACTO): Os relatos estão sendo revisados e resolvidos? Em quanto tempo?
CREATE OR REPLACE VIEW vw_q4_impacto_fila_do_gestor AS
WITH por_origem AS (
  SELECT f.fonte, grouping(f.fonte) AS linha_de_total,
         sum(f.qtd_relatos)                 AS recebidos,
         sum(f.fl_revisado)                 AS revisados,
         sum(f.fl_resolvido)                AS resolvidos,
         sum(CASE WHEN s.status = 'descartado' THEN 1 ELSE 0 END) AS descartados,
         sum(f.fl_aberto)                   AS ainda_abertos,
         round(avg(f.horas_ate_revisao), 1) AS horas_medias_ate_revisao
  FROM fato_relato f
  JOIN dim_status s ON s.sk_status = f.sk_status
  GROUP BY ROLLUP (f.fonte)
)
SELECT CASE WHEN linha_de_total = 1 THEN 'TOTAL' ELSE fonte END AS origem_do_relato,
       recebidos, revisados, resolvidos, descartados, ainda_abertos,
       round(100.0 * revisados / recebidos, 1)  AS taxa_de_revisao_pct,
       round(100.0 * resolvidos / recebidos, 1) AS taxa_de_resolucao_pct,
       horas_medias_ate_revisao
FROM por_origem
ORDER BY linha_de_total, origem_do_relato;

SELECT * FROM vw_q4_impacto_fila_do_gestor;

-- COMMAND ----------

-- Q5: Em que dias os relatos chegam, e a chuva muda isso?
CREATE OR REPLACE VIEW vw_q5_relatos_por_dia_e_chuva AS
WITH por_dia AS (
  SELECT d.data, d.condicao_chuva, d.fim_de_semana, d.chuva_mm, count(f.id_relato_hash) AS relatos
  FROM dim_data d
  LEFT JOIN fato_relato f ON f.sk_data_criacao = d.sk_data
  GROUP BY d.data, d.condicao_chuva, d.fim_de_semana, d.chuva_mm
)
SELECT condicao_chuva,
       CASE WHEN fim_de_semana THEN 'fim de semana' ELSE 'dia útil' END AS tipo_de_dia,
       count(*)                          AS dias_observados,
       round(avg(chuva_mm), 1)           AS chuva_media_mm,
       sum(relatos)                      AS relatos_criados,
       round(sum(relatos) / count(*), 2) AS relatos_por_dia
FROM por_dia
GROUP BY condicao_chuva, CASE WHEN fim_de_semana THEN 'fim de semana' ELSE 'dia útil' END
ORDER BY condicao_chuva, tipo_de_dia;

SELECT * FROM vw_q5_relatos_por_dia_e_chuva;
