-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 00 - Ambiente
-- MAGIC Esquema `caminho_livre` e volumes `chegada` (arquivos do Airflow) e `entrega` (exportação para o MVP).

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS workspace.caminho_livre
COMMENT 'Caminho Livre: barreiras de acessibilidade em calcadas e travessias';

-- COMMAND ----------

CREATE VOLUME IF NOT EXISTS workspace.caminho_livre.chegada
COMMENT 'Arquivos brutos coletados pelo Airflow (osm/, relatos/, clima/)';

-- COMMAND ----------

CREATE VOLUME IF NOT EXISTS workspace.caminho_livre.entrega
COMMENT 'Arquivos exportados da Ouro para o MVP';

-- COMMAND ----------

SHOW VOLUMES IN workspace.caminho_livre;
