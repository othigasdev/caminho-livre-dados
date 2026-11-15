# Prints do relatório

Salve cada captura nesta pasta com o nome exato da tabela (`.png` ou `.jpg`). Capture em tela cheia e com zoom que deixe
o texto legível. O `gerar_relatorio.py` põe a legenda e a numeração sozinho e avisa o que estiver faltando.

| Arquivo | O que capturar |
|---|---|
| `01-fontes.png` | Navegador com as respostas das fontes: `https://caminholivre-gs.azurewebsites.net/api/reports` e um arquivo de clima ou de OSM aberto (pode ser lado a lado) |
| `02-chegada.png` | Explorador de arquivos ou terminal (`ls -R chegada`) mostrando os arquivos com a data no nome |
| `03-variables.png` | Airflow, **Admin > Variables**, com `ntfy_topic_secret` mascarada |
| `03-conferir-log.png` | Airflow, log da tarefa `conferir_chegada` de uma execução (linhas `OK ...json: N bytes, N registros`) |
| `04-volume.png` | Databricks, **Catalog > workspace > caminho_livre > chegada**, com os arquivos dos dois lotes (abra uma das pastas) |
| `05-bronze-controle.png` | Notebook 01, célula "Evidência: contagens antes e depois", depois da 3ª execução |
| `05-bronze-arquivos.png` | Notebook 01, célula "Arquivos carregados e prova de que não há duplicata" |
| `06-prata-qualidade.png` | Notebook 02, tabela das regras de qualidade (seção 4) |
| `06-prata-merge.png` | Notebook 02, última célula (histórico de MERGE), depois do 2º lote |
| `07-ouro-modelo.png` | Catalog Explorer com as tabelas `fato_*`, `dim_*` e `ouro_painel_celula` |
| `08-lgpd-ouro.png` | Notebook 03, célula "Evidência de LGPD na Ouro" (hash, sem coordenada) |
| `08-mvp-tela.png` | MVP publicado, com o mapa e a seção "Situação por quadra" |
| `09-repositorio.png` | Página do repositório no GitHub, com as pastas e o README |
| `airflow-graph.png` | Airflow, DAG `caminho_livre_coleta`, visão **Graph** de uma execução concluída |
| `airflow-grid.png` | Airflow, visão **Grid**, com várias execuções verdes |
| `q1.png` a `q5.png` | Notebook 04, resultado de cada consulta |
