-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 00 · Preparar o ambiente (rodar uma vez)
-- MAGIC Cria o esquema do projeto e os dois Volumes:
-- MAGIC - `chegada`: recebe, pela interface, os arquivos que o Airflow gravou na pasta `chegada/` (um lote por vez);
-- MAGIC - `entrega`: recebe o que a camada Ouro publica para o MVP e as evidências do relatório.
-- MAGIC
-- MAGIC Depois de rodar, crie no Volume `chegada` as pastas `osm`, `relatos` e `clima` e envie o **1º lote**.

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS workspace.caminho_livre
COMMENT 'Caminho Livre (GS FIAP 2026.2): barreiras de acessibilidade em calcadas e travessias';

-- COMMAND ----------

CREATE VOLUME IF NOT EXISTS workspace.caminho_livre.chegada
COMMENT 'Pasta de pouso: arquivos brutos coletados pelo Airflow (osm/, relatos/, clima/)';

-- COMMAND ----------

CREATE VOLUME IF NOT EXISTS workspace.caminho_livre.entrega
COMMENT 'Saidas da Ouro para o MVP (contrato JSON) e evidencias do relatorio';

-- COMMAND ----------

SHOW VOLUMES IN workspace.caminho_livre;
