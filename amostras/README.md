# Amostras

Um recorte pequeno (5 registros) de cada fonte, gravado pela tarefa `atualizar_amostras` do DAG a cada coleta:

| Arquivo | Fonte |
|---|---|
| `osm_amostra.json` | OpenStreetMap, via Overpass API (© colaboradores do OpenStreetMap, ODbL) |
| `relatos_amostra.json` | API de relatos do MVP (coordenada aproximada e descrição já sanitizada na origem) |
| `clima_amostra.json` | Open-Meteo (CC BY 4.0) |

Os arquivos aparecem aqui depois da primeira execução do DAG. Não há dado pessoal: a consulta ao OpenStreetMap não pede
o autor do mapeamento, e a API de relatos não tem conta de usuário.
