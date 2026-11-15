"""Monta o relatório da disciplina (HTML pronto para "Salvar como PDF"), na ordem pedida no enunciado.

Entradas, todas reais:
  relatorio/evidencias.json   baixado do Volume `entrega` (gerado pelo notebook 05)
  relatorio/prints/*.png      capturas de tela com os nomes listados em PRINTS
  databricks/04_consultas_mvp.sql

Uso:  python relatorio/gerar_relatorio.py
      abra relatorio/relatorio-big-data.html no navegador > Imprimir > Salvar como PDF

Só usa a biblioteca padrão. O que estiver faltando aparece em vermelho no HTML e é listado no terminal.
"""
from __future__ import annotations

import base64
import html
import json
import re
from pathlib import Path

RAIZ = Path(__file__).resolve().parent
REPO = RAIZ.parent

# ---- Ajuste aqui se os endereços forem outros ----
REPO_DADOS = "https://github.com/othigasdev/caminho-livre-dados"
REPO_MVP = "https://github.com/santos-nikolas/caminho-livre"
URL_MVP = "https://caminholivre-gs.azurewebsites.net"
PROFESSORES = "Profs. Rogério, Thiago, Thiago e Milton"
INTEGRANTES = [
    ("Guilherme Rocha Bianchini", "RM97974"),
    ("Nikolas Rodrigues Moura dos Santos", "RM551566"),
    ("Thiago Jardim de Oliveira", "RM551624"),
    ("Rodrigo Brasileiro", "RM98952"),
]

# nome do arquivo em relatorio/prints/ (sem extensão) -> legenda
PRINTS = {
    "01-fontes": "Respostas reais das fontes no navegador: API de relatos do MVP, Open-Meteo e Overpass (OpenStreetMap).",
    "02-chegada": "Pasta chegada/ com um arquivo por fonte e por coleta, com a data de referência no nome.",
    "03-variables": "Airflow, Admin > Variables: a URL do tópico de notificação fica na Variable ntfy_topic_secret, com o valor mascarado.",
    "03-conferir-log": "Log da tarefa conferir_chegada: os três arquivos do dia conferidos, com tamanho e número de registros.",
    "04-volume": "Volume chegada no Databricks (Catalog Explorer) com os arquivos enviados pela interface, em dois lotes.",
    "05-bronze-controle": "Notebook 01: histórico das cargas da Bronze, com as linhas antes e depois de cada execução.",
    "05-bronze-arquivos": "Notebook 01: cada arquivo do Volume aparece uma única vez na Bronze (vezes_carregado = 1).",
    "06-prata-qualidade": "Notebook 02: regras de qualidade com o total avaliado e o total barrado por regra.",
    "06-prata-merge": "Notebook 02: histórico do Delta com as linhas inseridas e atualizadas em cada MERGE.",
    "07-ouro-modelo": "Catalog Explorer: esquema caminho_livre com as tabelas fato, as dimensões e a tabela do MVP.",
    "08-lgpd-ouro": "Notebook 03: fato_relato com o identificador em hash, sem coordenada e sem texto livre.",
    "08-mvp-tela": "MVP publicado: seção \"Situação por quadra\" e quadras no mapa, com os dados de ouro_painel_celula.",
    "09-repositorio": "Repositório público no GitHub com dags/, databricks/, amostras/ e README.md.",
    "airflow-graph": "Airflow, visão Graph do DAG caminho_livre_coleta: três coletas em paralelo, conferência e notificação.",
    "airflow-grid": "Airflow, visão Grid: execuções diárias concluídas com sucesso.",
    "q1": "Resultado da consulta Q1 no Databricks.",
    "q2": "Resultado da consulta Q2 no Databricks.",
    "q3": "Resultado da consulta Q3 no Databricks.",
    "q4": "Resultado da consulta Q4 no Databricks.",
    "q5": "Resultado da consulta Q5 no Databricks.",
}

PERGUNTAS = [
    ("Q1", "Onde há barreiras relatadas que continuam abertas?", "cidadão", "lista e mapa de relatos; quadras em atenção alta"),
    ("Q2", "Em quais quadras há travessia sem nenhum rebaixamento mapeado?", "cidadão e gestor", "quadras em atenção média no mapa"),
    ("Q3", "Quanto do território já tem dado de acessibilidade mapeado?", "gestor", "aviso de que \"sem dado\" não é \"sem rampa\""),
    ("Q4", "Os relatos estão sendo revisados e resolvidos? Em quanto tempo? (impacto)", "gestor e sociedade", "status de cada relato"),
    ("Q5", "Em que dias os relatos chegam, e a chuva muda isso?", "gestor", "planejamento da vistoria"),
]

pendencias: list[str] = []
numero_figura = 0
e = html.escape


def fmt(valor) -> str:
    """Número no padrão brasileiro; None vira travessão."""
    if valor is None:
        return "—"
    if isinstance(valor, bool):
        return "sim" if valor else "não"
    if isinstance(valor, int):
        return f"{valor:,}".replace(",", ".")
    if isinstance(valor, float):
        if valor == int(valor):
            return fmt(int(valor))
        return f"{valor:,.2f}".rstrip("0").replace(",", "X").replace(".", ",").replace("X", ".")
    return e(str(valor))


def pendente(texto: str) -> str:
    pendencias.append(texto)
    return f'<p class="pendente">PENDENTE: {e(texto)}</p>'


def figura(nome: str) -> str:
    global numero_figura
    numero_figura += 1
    legenda = PRINTS[nome]
    for extensao in ("png", "jpg", "jpeg"):
        arquivo = RAIZ / "prints" / f"{nome}.{extensao}"
        if arquivo.is_file():
            tipo = "jpeg" if extensao != "png" else "png"
            dados = base64.b64encode(arquivo.read_bytes()).decode()
            return (f'<figure><img src="data:image/{tipo};base64,{dados}" alt="{e(legenda)}">'
                    f"<figcaption>Figura {numero_figura}: {e(legenda)}</figcaption></figure>")
    return pendente(f"print relatorio/prints/{nome}.png ({legenda})")


def tabela(linhas: list[dict], colunas: list[str] | None = None, rotulos: dict | None = None) -> str:
    if not linhas:
        return '<p class="vazio">A consulta não devolveu linhas.</p>'
    colunas = colunas or list(linhas[0])
    rotulos = rotulos or {}
    cab = "".join(f"<th>{e(rotulos.get(c, c.replace('_', ' ')))}</th>" for c in colunas)
    corpo = "".join("<tr>" + "".join(
        f'<td class="{"num" if isinstance(l.get(c), (int, float)) and not isinstance(l.get(c), bool) else ""}">{fmt(l.get(c))}</td>'
        for c in colunas) + "</tr>" for l in linhas)
    return f"<table><thead><tr>{cab}</tr></thead><tbody>{corpo}</tbody></table>"


def sql_das_consultas() -> dict[str, str]:
    texto = (REPO / "databricks" / "04_consultas_mvp.sql").read_text(encoding="utf-8")
    consultas = {}
    for celula in texto.split("-- COMMAND ----------"):
        celula = celula.strip()
        achou = re.match(r"-- (Q\d)", celula)
        if achou:
            consultas[achou.group(1).lower()] = re.sub(r"\n\s*SELECT \* FROM vw_\w+;\s*$", "", celula)
    return consultas


# ---------------------------------------------------------------- interpretações (calculadas dos resultados reais)
def interpretar_q1(l: list[dict]) -> str:
    if not l:
        return "Não há relato aberto: toda a fila já foi revisada e encerrada."
    total, demo = sum(x["relatos_abertos"] for x in l), sum(x["de_demonstracao"] for x in l)
    quadras = len({x["quadricula"] for x in l})
    topo = l[0]
    antigo = max(x["dias_do_mais_antigo"] or 0 for x in l)
    return (f"Há {fmt(total)} relato(s) aberto(s) em {fmt(quadras)} quadrícula(s). A que mais concentra é a {e(topo['quadricula'])}, "
            f"com {fmt(topo['relatos_abertos'])} de \"{e(topo['categoria'])}\". O relato aberto mais antigo espera há {fmt(antigo)} dia(s). "
            f"{fmt(demo)} dos abertos são dados sintéticos de demonstração, que o MVP identifica na tela. "
            "É a resposta que o cidadão vê no mapa: onde a barreira foi relatada e ainda não foi tratada.")


def interpretar_q2(l: list[dict]) -> str:
    if not l:
        return ("Toda quadrícula com travessia tem ao menos um rebaixamento mapeado no OpenStreetMap, "
                "então nenhuma entra na lista de vistoria por falta de dado.")
    sem, total = l[0]["quadriculas_sem_rebaixamento"], l[0]["quadriculas_com_travessia"]
    return (f"Das {fmt(total)} quadrículas do piloto com travessia de pedestres, {fmt(sem)} ({fmt(round(100 * sem / total, 1))}%) "
            f"não têm nenhum rebaixamento mapeado. A lista começa pela {e(l[0]['quadricula'])}, com {fmt(l[0]['travessias'])} travessia(s). "
            "Isso não prova que a rampa não existe: mostra onde o dado aberto está em branco, que é onde a vistoria do gestor "
            "(ou o mapeamento colaborativo) deve começar. No MVP essas quadras aparecem com atenção média.")


def interpretar_q3(l: list[dict]) -> str:
    if not l:
        return "Sem elementos do OpenStreetMap na Ouro."
    total = sum(x["elementos"] for x in l)
    trav = next((x for x in l if str(x["classe_do_elemento"]).startswith("Travessia")), None)
    texto = f"O piloto tem {fmt(total)} elementos de pedestre mapeados no OpenStreetMap. "
    if trav:
        texto += (f"Das {fmt(trav['elementos'])} travessias, {fmt(trav['pct_rebaixamento_informado'])}% informam se há rebaixamento "
                  f"e {fmt(trav['pct_piso_tatil_informado'])}% informam se há piso tátil; "
                  f"{fmt(trav['com_piso_tatil'])} têm piso tátil confirmado. ")
    return texto + ("O que falta é lacuna de mapeamento, não ausência de equipamento. Por isso a Prata grava \"nao_mapeado\" "
                    "em vez de \"não\", e o MVP avisa o cidadão dessa diferença.")


def interpretar_q4(l: list[dict]) -> str:
    total = next((x for x in l if x["origem_do_relato"] == "TOTAL"), None)
    if not total:
        return "Ainda não há relatos na Ouro."
    cidadao = next((x for x in l if x["origem_do_relato"] == "cidadao"), None)
    horas = total["horas_medias_ate_revisao"]
    texto = (f"Indicador de impacto do produto. Dos {fmt(total['recebidos'])} relatos recebidos, {fmt(total['revisados'])} "
             f"({fmt(total['taxa_de_revisao_pct'])}%) já foram revisados pelo gestor e {fmt(total['resolvidos'])} "
             f"({fmt(total['taxa_de_resolucao_pct'])}%) foram resolvidos; {fmt(total['ainda_abertos'])} seguem abertos. ")
    texto += (f"O tempo médio entre o registro e a revisão é de {fmt(horas)} h. " if horas is not None
              else "Ainda não há relato revisado para medir o tempo até a revisão. ")
    if cidadao:
        texto += (f"Considerando só os relatos de cidadãos (sem os de demonstração), são {fmt(cidadao['recebidos'])} recebidos "
                  f"e {fmt(cidadao['resolvidos'])} resolvidos. ")
    return texto + ("São indicadores de processo de um piloto recém-publicado, com poucos relatos: medem se a fila anda, "
                    "não provam a melhoria física da calçada, que depende de confirmação no local.")


def interpretar_q5(l: list[dict]) -> str:
    if not l:
        return "Sem dias observados."
    por = {}
    for x in l:
        d = por.setdefault(x["condicao_chuva"], {"dias": 0, "relatos": 0})
        d["dias"] += x["dias_observados"]
        d["relatos"] += x["relatos_criados"]
    partes = [f"{fmt(v['dias'])} dia(s) {k.replace('_', ' ')} com {fmt(v['relatos'])} relato(s)" for k, v in sorted(por.items())]
    texto = "Foram observados " + "; ".join(partes) + ". "
    if por.get("com_chuva", {}).get("dias") and por.get("sem_chuva", {}).get("dias"):
        c = por["com_chuva"]["relatos"] / por["com_chuva"]["dias"]
        s = por["sem_chuva"]["relatos"] / por["sem_chuva"]["dias"]
        texto += f"Isso dá {fmt(round(c, 2))} relato(s) por dia com chuva contra {fmt(round(s, 2))} por dia sem chuva. "
    else:
        texto += "Ainda não há dias com chuva e sem chuva suficientes para comparar. "
    return texto + ("Com tão poucos dias, a diferença não permite conclusão: a consulta fica pronta para, com o piloto em uso, "
                    "mostrar ao gestor se a demanda sobe em dia de chuva e em que dia da semana reforçar a vistoria.")


INTERPRETAR = {"q1": interpretar_q1, "q2": interpretar_q2, "q3": interpretar_q3, "q4": interpretar_q4, "q5": interpretar_q5}

# ---------------------------------------------------------------- diagrama do fluxo de dados (SVG)
def diagrama() -> str:
    def caixa(x, y, w, h, titulo, linhas, classe=""):
        t = f'<rect x="{x}" y="{y}" width="{w}" height="{h}" rx="6" class="cx {classe}"/>'
        t += f'<text x="{x + w / 2}" y="{y + 18}" class="tt">{e(titulo)}</text>'
        for i, linha in enumerate(linhas):
            t += f'<text x="{x + w / 2}" y="{y + 35 + i * 14}" class="tx">{e(linha)}</text>'
        return t

    def seta(x1, y1, x2, y2, rotulo=""):
        t = f'<line x1="{x1}" y1="{y1}" x2="{x2}" y2="{y2}" class="st" marker-end="url(#p)"/>'
        if rotulo:
            t += f'<text x="{(x1 + x2) / 2}" y="{(y1 + y2) / 2 - 5}" class="rt">{e(rotulo)}</text>'
        return t

    s = ('<svg viewBox="0 0 960 400" xmlns="http://www.w3.org/2000/svg" role="img" '
         'aria-label="Fluxo de dados: fontes, Airflow, pasta chegada, Volume, Bronze, Prata, Ouro e MVP">'
         '<defs><marker id="p" markerWidth="8" markerHeight="8" refX="7" refY="4" orient="auto">'
         '<path d="M0,0 L8,4 L0,8 z" fill="#333"/></marker></defs>')
    # linha 1: coleta
    s += caixa(10, 10, 190, 56, "OpenStreetMap (Overpass)", ["travessias, meios-fios, calçadas", "dado aberto · ODbL"], "fonte")
    s += caixa(10, 76, 190, 56, "API do MVP /api/reports", ["relatos de barreiras", "fonte do próprio produto"], "fonte")
    s += caixa(10, 142, 190, 56, "Open-Meteo", ["chuva e temperatura por hora", "API pública · sem chave"], "fonte")
    s += caixa(255, 30, 265, 150, "Airflow 3 · DAG caminho_livre_coleta", [
        "agendado: todo dia às 06h", "", "coletar_osm ║ coletar_relatos ║ coletar_clima",
        "(3 tarefas em paralelo)", "▼", "conferir_chegada", "▼", "atualizar_amostras · notificar (ntfy)"], "air")
    s += caixa(580, 60, 170, 90, "chegada/", ["pasta local", "osm_AAAA-MM-DD.json", "relatos_AAAA-MM-DD.json", "clima_AAAA-MM-DD.json"])
    s += caixa(800, 60, 150, 90, "Volume chegada", ["Databricks", "Unity Catalog", "osm/ relatos/ clima/"], "dbx")
    for y in (38, 104, 170):
        s += seta(200, y, 253, 104 if y == 104 else (80 if y == 38 else 130))
    s += seta(520, 105, 578, 105)
    s += seta(750, 105, 798, 105, "upload")
    s += '<text x="775" y="165" class="rt">manual, em lotes</text>'
    # linha 2: lakehouse e entrega
    s += caixa(800, 250, 150, 110, "Bronze", ["dado bruto", "Auto Loader", "só arquivos novos", "bronze_osm / _relatos", "/ _clima"], "bronze")
    s += caixa(600, 250, 150, 110, "Prata", ["tipos e domínios", "5 regras de qualidade", "quarentena", "MERGE pela chave"], "prata")
    s += caixa(370, 250, 180, 110, "Ouro", ["fato_relato · fato_elemento_via", "dim_celula · dim_data · …", "LGPD: hash, sem coordenada", "ouro_painel_celula"], "ouro")
    s += caixa(190, 250, 130, 110, "Contrato JSON", ["ouro_painel_celula", ".json", "Volume entrega", "→ app/dados/"])
    s += caixa(10, 250, 130, 110, "MVP", ["GET /api/painel", "tela do cidadão:", "Situação por quadra", "(Azure App Service)"], "mvp")
    s += seta(875, 150, 875, 248)
    s += seta(800, 305, 752, 305)
    s += seta(600, 305, 552, 305)
    s += seta(370, 305, 322, 305)
    s += seta(190, 305, 142, 305, "deploy")
    s += '<text x="480" y="385" class="rt">As cinco consultas SQL da seção 8 leem as tabelas fato e dimensão da Ouro.</text>'
    return s + "</svg>"


CSS = """
@page { size: A4; margin: 14mm 13mm; }
* { box-sizing: border-box; }
body { font: 10.5pt/1.45 "Segoe UI", system-ui, Arial, sans-serif; color: #1a1a1a; max-width: 190mm; margin: 0 auto; padding: 8px; }
.capa { border-left: 6px solid #0b5cad; padding-left: 14px; margin-bottom: 14px; }
.capa p { margin: 0; color: #555; } .capa h1 { margin: 2px 0; color: #0b5cad; font-size: 22pt; }
h2 { font-size: 14pt; border-bottom: 2px solid #0b5cad; padding-bottom: 3px; margin: 22px 0 8px; break-after: avoid; }
h3 { font-size: 11.5pt; margin: 16px 0 4px; break-after: avoid; } p { margin: 5px 0; }
table { border-collapse: collapse; width: 100%; margin: 6px 0 10px; font-size: 9.5pt; }
th, td { border: 1px solid #d0d7de; padding: 4px 7px; text-align: left; vertical-align: top; }
th { background: #f1f5f9; } th, td { overflow-wrap: break-word; } td.num { text-align: right; font-variant-numeric: tabular-nums; }
code, pre { font-family: Consolas, "Courier New", monospace; font-size: 8.6pt; background: #f1f5f9; }
code { padding: 0 3px; } pre { padding: 8px; white-space: pre-wrap; border-radius: 4px; margin: 6px 0; }
figure { margin: 10px 0 14px; text-align: center; break-inside: avoid; }
figure img { max-width: 100%; max-height: 215mm; border: 1px solid #d0d7de; }
figcaption { color: #555; font-size: 9pt; margin-top: 3px; }
.pendente { color: #b00020; border: 2px dashed #b00020; padding: 10px; font-weight: 600; background: #fff5f5; }
.vazio { color: #555; font-style: italic; } .interp { background: #f6faf6; border-left: 4px solid #2b8a3e; padding: 6px 10px; }
.req { break-inside: auto; } svg { width: 100%; height: auto; border: 1px solid #d0d7de; border-radius: 4px; }
svg .cx { fill: #fff; stroke: #333; } svg .fonte { fill: #eef4fb; } svg .air { fill: #e8f5f0; } svg .dbx { fill: #fff4e6; }
svg .bronze { fill: #f3e2d0; } svg .prata { fill: #eceff1; } svg .ouro { fill: #fff3bf; } svg .mvp { fill: #ffe3ec; }
svg text { font-family: "Segoe UI", Arial, sans-serif; text-anchor: middle; } svg .tt { font-size: 12.5px; font-weight: 700; }
svg .tx { font-size: 11px; } svg .rt { font-size: 10.5px; fill: #555; font-style: italic; } svg .st { stroke: #333; stroke-width: 1.3; }
"""


def montar(ev: dict | None) -> str:
    sql = sql_das_consultas()
    cont = (ev or {}).get("contagens", {})
    p: list[str] = []
    add = p.append

    def dado(funcao, o_que: str) -> str:
        """Trecho que depende do evidencias.json."""
        if ev is None:
            return pendente(f"{o_que} (falta relatorio/evidencias.json)")
        return funcao()

    add('<div class="capa"><p>FIAP · Global Solution 2026.2 · CitizenTech · Engenharia de Software 4ES</p>'
        "<h1>Caminho Livre: componente Dados</h1>"
        f"<p>Big Data Architecture &amp; Data Integration · {e(PROFESSORES)}</p></div>")
    add("<table><tr><th>Repositório desta entrega (público)</th>"
        f'<td><a href="{REPO_DADOS}">{REPO_DADOS}</a></td></tr>'
        f'<tr><th>MVP publicado</th><td><a href="{URL_MVP}">{URL_MVP}</a></td></tr>'
        f'<tr><th>Repositório do MVP</th><td><a href="{REPO_MVP}">{REPO_MVP}</a></td></tr></table>')

    # 1
    add("<h2>1. Integrantes</h2>")
    add(tabela([{"Nome completo": n, "RM": rm} for n, rm in INTEGRANTES]))

    # 2
    add("<h2>2. O produto em uma frase e as perguntas que o MVP responde</h2>")
    add("<p><strong>Caminho Livre</strong> é um mapa colaborativo em que pedestres com deficiência ou mobilidade reduzida, idosos e "
        "cuidadores registram barreiras de acessibilidade em calçadas e travessias do entorno da FIAP Paulista e acompanham o "
        "status, enquanto o gestor recebe uma fila auditável para revisar, priorizar e encerrar cada relato.</p>")
    add("<p>O componente Dados entrega ao produto a <strong>situação de cada quadra</strong>: junta os relatos com a malha de travessias "
        "e rebaixamentos do OpenStreetMap e com o clima, e publica a tabela <code>ouro_painel_celula</code>, que o MVP mostra na tela.</p>")
    add(tabela([{"#": a, "Pergunta": b, "Quem pergunta": c, "Onde aparece no MVP": d} for a, b, c, d in PERGUNTAS]))

    # 3
    add("<h2>3. Diagrama do fluxo de dados, da fonte ao MVP</h2>")
    add(diagrama())
    add("<p>O Databricks Free Edition não se conecta ao Airflow nem aceita conexão de fora. Por isso a entrada é o envio dos arquivos da "
        "pasta <code>chegada/</code> ao Volume pela interface, em lotes, e a saída para o MVP é um arquivo JSON com formato fixado em "
        "contrato (<code>contrato/ouro_painel_celula.schema.json</code>), publicado pelo pipeline de deploy do MVP.</p>")

    # 4
    add("<h2>4. Tabela de fontes</h2>")
    add(tabela([
        {"Fonte": "OpenStreetMap, via Overpass API", "Origem": "Externa, dado aberto (ODbL). overpass-api.de",
         "Formato": "JSON: lista elements, com tags de chave e valor variáveis", "Frequência de coleta": "Diária, 06h (retrato do dia)",
         "Conteúdo": "Travessias, meios-fios e calçadas do piloto", "Chave de API": "Não exige"},
        {"Fonte": "Relatos do Caminho Livre", "Origem": f"Interna: API pública do MVP, {URL_MVP}/api/reports",
         "Formato": "JSON: lista itens", "Frequência de coleta": "Diária, 06h (fila completa)",
         "Conteúdo": "Barreiras relatadas, com categoria, status e coordenada aproximada", "Chave de API": "Não exige"},
        {"Fonte": "Open-Meteo", "Origem": "Externa, API pública (CC BY 4.0). api.open-meteo.com",
         "Formato": "JSON: listas paralelas por hora", "Frequência de coleta": "Diária, 06h (24 horas do dia anterior)",
         "Conteúdo": "Chuva e temperatura no centro do piloto", "Chave de API": "Não exige"},
    ]))
    add("<p>O território é o mesmo retângulo do MVP (latitude -23,5700 a -23,5590; longitude -46,6600 a -46,6450). "
        "Os relatos marcados como <code>sintetico</code> são os dados de demonstração do MVP, identificados na tela e nas consultas.</p>")

    # 5
    add("<h2>5. Requisitos 01 a 09</h2>")

    add('<div class="req"><h3>Requisito 01: duas ou mais fontes, ao menos uma externa</h3>'
        "<p>São três fontes do problema: duas externas e abertas (OpenStreetMap e Open-Meteo) e uma do próprio produto (relatos). "
        "O OpenStreetMap diz onde estão as travessias e o que se sabe sobre rebaixamento e piso tátil; os relatos dizem onde o cidadão "
        "encontrou barreira; o clima dá o contexto do dia. Uma amostra de cada uma está em <code>amostras/</code>.</p>"
        + figura("01-fontes") + "</div>")

    add('<div class="req"><h3>Requisito 02: DAG agendado gravando em chegada/</h3>'
        "<p>O DAG <code>caminho_livre_coleta</code> (<code>dags/caminho_livre_coleta.py</code>) roda todo dia às 06h "
        "(<code>schedule=\"0 6 * * *\"</code>, fuso de São Paulo, <code>catchup=True</code>) e grava a resposta bruta de cada fonte em "
        "<code>chegada/&lt;fonte&gt;/&lt;fonte&gt;_AAAA-MM-DD.json</code>: um arquivo por coleta, com o dia de referência no nome. "
        "Reexecutar o mesmo dia sobrescreve o mesmo arquivo, com troca atômica.</p>"
        + figura("02-chegada") + "</div>")

    add('<div class="req"><h3>Requisito 03: paralelismo, conferência dos arquivos e chaves fora do código</h3>'
        "<p><code>coletar_osm</code>, <code>coletar_relatos</code> e <code>coletar_clima</code> rodam em paralelo (Figura da visão Graph, "
        "seção 6). Em seguida, <code>conferir_chegada</code> barra a execução se algum arquivo do dia não chegou, está vazio, não é JSON "
        "ou veio sem registros. Nenhuma das fontes exige chave. O único segredo do DAG é a URL do tópico de notificação (ntfy.sh, o mesmo "
        "canal dos alertas de produção), que fica na Variable <code>ntfy_topic_secret</code>; os endereços das APIs também são "
        "Variables. Não há chave, senha nem URL secreta no repositório.</p>"
        + figura("03-variables") + figura("03-conferir-log") + "</div>")

    def lotes() -> str:
        dias: dict[str, list[str]] = {}
        for linha in ev["bronze_arquivos"]:
            dias.setdefault(linha["fonte"], []).append(linha["dia"])
        return tabela([{"Fonte": f, "Arquivos no Volume": len(d), "Dias de coleta": ", ".join(d)} for f, d in sorted(dias.items())])

    add('<div class="req"><h3>Requisito 04: dois ou mais lotes enviados ao Volume</h3>'
        "<p>Os arquivos de <code>chegada/</code> foram enviados pela interface ao Volume "
        "<code>/Volumes/workspace/caminho_livre/chegada</code>, nas pastas <code>osm/</code>, <code>relatos/</code> e <code>clima/</code>, "
        "em lotes de dias de coleta diferentes. A tabela lista os dias que estão na Bronze.</p>"
        + dado(lotes, "dias de coleta por fonte") + figura("04-volume") + "</div>")

    add('<div class="req"><h3>Requisito 05: Bronze incremental e sem duplicação</h3>'
        "<p>O notebook <code>01_bronze_auto_loader</code> usa Auto Loader (<code>cloudFiles</code>) com "
        "<code>trigger(availableNow=True)</code> e um <em>checkpoint</em> por fonte. O checkpoint guarda os arquivos já lidos: no 2º lote "
        "só os arquivos novos entram, e uma nova execução sem arquivos novos não acrescenta nenhuma linha. A Bronze guarda uma linha por "
        "arquivo, com o JSON bruto, o arquivo de origem e o momento da carga. As contagens estão na seção 7.</p>"
        + figura("05-bronze-controle") + "</div>")

    def qualidade() -> str:
        t = tabela(ev["qualidade"], ["regra", "fonte", "descricao", "total_avaliado", "total_barrado"],
                   {"regra": "Regra", "fonte": "Fonte", "descricao": "O que barra", "total_avaliado": "Avaliados", "total_barrado": "Barrados"})
        m = tabela(ev["merges"], ["tabela", "versao", "quando", "inseridas", "atualizadas"],
                   {"tabela": "Tabela", "versao": "Versão Delta", "quando": "Quando (UTC)", "inseridas": "Inseridas", "atualizadas": "Atualizadas"})
        prata = tabela([{"Tabela": t_, "Linhas": cont.get(t_)} for t_ in
                        ("prata_osm_elemento", "prata_relato", "prata_clima_hora", "prata_rejeitados")])
        return ("<p>Total barrado por regra na execução mais recente:</p>" + t
                + "<p>Histórico de MERGE gravado pelo Delta:</p>" + m + "<p>Linhas na Prata:</p>" + prata)

    add('<div class="req"><h3>Requisito 06: Prata com tipos, nulos e inválidos tratados, MERGE e regras de qualidade</h3>'
        "<p>O notebook <code>02_prata_tratamento_merge</code> abre o JSON bruto com <code>from_json</code> e esquema explícito "
        "(as tags do OpenStreetMap como <code>MAP</code>; as listas do Open-Meteo com <code>arrays_zip</code> e <code>explode</code>), "
        "converte datas e números com <code>try_cast</code> e padroniza os domínios. Nulo tem tratamento com significado: atributo que "
        "não está no OpenStreetMap vira <code>nao_mapeado</code>, nunca \"não\". Cinco regras de qualidade contam o que barram; os "
        "barrados por validade vão para a quarentena <code>prata_rejeitados</code>. O MERGE usa a chave de cada tabela "
        "(<code>tipo_osm + osm_id</code>, <code>id_relato</code>, <code>data_hora</code>) e só atualiza quando o lote é mais recente.</p>"
        + dado(qualidade, "regras de qualidade, MERGE e contagens da Prata")
        + figura("06-prata-qualidade") + figura("06-prata-merge") + "</div>")

    def ouro() -> str:
        papeis = [("fato_relato", "Fato", "um relato de barreira"), ("fato_elemento_via", "Fato", "um elemento de pedestre do OpenStreetMap"),
                  ("dim_celula", "Dimensão", "quadrícula de ~110 m × 100 m (compartilhada pelas duas fatos)"),
                  ("dim_data", "Dimensão", "dia, com chuva e temperatura"), ("dim_categoria", "Dimensão", "categoria da barreira"),
                  ("dim_status", "Dimensão", "status na fila do gestor"), ("dim_classe_elemento", "Dimensão", "classe do elemento OSM"),
                  ("ouro_painel_celula", "Agregada para o MVP", "situação de cada quadrícula")]
        return tabela([{"Tabela": t_, "Papel": pp, "Grão": g, "Linhas": cont.get(t_)} for t_, pp, g in papeis])

    add('<div class="req"><h3>Requisito 07: Ouro com fato e dimensões, e cinco consultas SQL</h3>'
        "<p>O notebook <code>03_ouro_modelo_dimensional</code> monta um modelo estrela com duas fatos e cinco dimensões. "
        "A <code>dim_celula</code> é compartilhada: é ela que permite cruzar o que o cidadão relatou com o que está mapeado na mesma quadra. "
        "As cinco consultas (<code>04_consultas_mvp.sql</code>) estão na seção 8, com resultado e interpretação; a Q4 é a de impacto.</p>"
        + dado(ouro, "contagens da Ouro") + figura("07-ouro-modelo") + "</div>")

    def painel() -> str:
        pn = ev["painel"]
        return (f"<p>Na exportação usada pelo MVP (dados de {e(pn['data_referencia'])}), {fmt(pn['celulas_publicadas'])} quadrículas "
                "foram publicadas:</p>" + tabela(pn["por_nivel"], None, {
                    "nivel_atencao": "Nível de atenção", "quadriculas": "Quadrículas", "travessias": "Travessias",
                    "pontos_rebaixados": "Rebaixamentos mapeados", "relatos_abertos": "Relatos abertos"})
                + "<p>Colunas da <code>fato_relato</code>: <code>" + e(", ".join(ev["lgpd"]["colunas_fato_relato"])) + "</code>.</p>")

    add('<div class="req"><h3>Requisito 08: dados pessoais protegidos na Ouro e uso pelo MVP</h3>'
        "<p>O relato é o único dado ligado a uma pessoa: indica onde alguém esteve e pode trazer texto livre.</p>"
        + tabela([
            {"Dado": "Identificador do relato", "Proteção na Ouro": "Hash SHA-256 com sal. O sal é informado em widget e não fica no repositório."},
            {"Dado": "Coordenada do relato", "Proteção na Ouro": "Não entra. Fica só a quadrícula (~100 m). Na origem o MVP já arredonda para ~11 m."},
            {"Dado": "Descrição livre", "Proteção na Ouro": "Não entra; fica só o indicador tem_descricao. Na Prata, e-mail, CPF e telefone são mascarados de novo."},
            {"Dado": "Horário", "Proteção na Ouro": "Reduzido a dia e hora cheia."},
            {"Dado": "Autor do mapeamento no OpenStreetMap", "Proteção na Ouro": "Nunca é coletado: a consulta à Overpass não pede metadados de usuário."},
        ])
        + "<p><strong>Contrato com o produto.</strong> O MVP usa a tabela <code>workspace.caminho_livre.ouro_painel_celula</code>, agregada por "
        "quadrícula e sem relato individual. O notebook 05 a exporta como <code>ouro_painel_celula.json</code>; o arquivo entra no "
        "repositório do MVP em <code>app/dados/</code>, passa pelo pipeline de deploy e é servido em <code>GET /api/painel</code>. "
        "A página mostra esses dados na seção \"Situação por quadra\" e como quadras coloridas no mapa. A regra do nível de atenção é "
        "fixa e explicada na tela, e a decisão de vistoria continua com o gestor.</p>"
        + dado(painel, "resumo do painel exportado e colunas da fato_relato")
        + figura("08-lgpd-ouro") + figura("08-mvp-tela") + "</div>")

    add('<div class="req"><h3>Requisito 09: repositório público com README</h3>'
        f'<p>O repositório <a href="{REPO_DADOS}">{REPO_DADOS}</a> traz <code>dags/</code>, <code>databricks/</code>, '
        "<code>amostras/</code>, o contrato com o MVP e o <code>README.md</code> com integrantes, arquitetura e o passo a passo para "
        "reproduzir o pipeline. Não há chave, senha nem dado pessoal real: a pasta <code>chegada/</code> e o arquivo de Variables "
        "estão no <code>.gitignore</code>.</p>" + figura("09-repositorio") + "</div>")

    # 6
    add("<h2>6. Prints do Airflow: visão Graph e Grid com execuções concluídas</h2>")
    add(figura("airflow-graph") + figura("airflow-grid"))

    # 7
    def bronze() -> str:
        ex: dict[int, dict] = {}
        for linha in ev["bronze_controle"]:
            d = ex.setdefault(linha["execucao"], {"quando": linha["quando"], "antes": 0, "depois": 0, "novos": 0})
            d["antes"] += linha["linhas_antes"]
            d["depois"] += linha["linhas_depois"]
            d["novos"] += linha["arquivos_novos"]
        resumo, lote = [], 0
        for n, d in sorted(ex.items()):
            if d["novos"] > 0:
                lote += 1
                papel = f"Carga do {lote}º lote"
            else:
                papel = "Nova execução sem arquivos novos"
            resumo.append({"Execução": n, "Quando": d["quando"], "O que foi": papel, "Linhas antes": d["antes"],
                           "Linhas depois": d["depois"], "Arquivos novos": d["novos"]})
        tem_reexecucao = any(r["Arquivos novos"] == 0 for r in resumo)
        avisos = ""
        if lote < 2:
            avisos += pendente("a Bronze ainda não registra a carga de um 2º lote (rode o notebook 01 depois de enviar o 2º lote)")
        if not tem_reexecucao:
            avisos += pendente("a Bronze ainda não registra uma 2ª execução sem arquivos novos (rode o notebook 01 duas vezes seguidas)")
        t = (avisos + "<p>Soma das três tabelas Bronze (uma linha por arquivo) em cada execução do notebook 01:</p>" + tabela(resumo)
             + "<p>Detalhe por tabela:</p>" + tabela(ev["bronze_controle"], None, {
                 "execucao": "Execução", "quando": "Quando", "tabela": "Tabela", "linhas_antes": "Linhas antes",
                 "linhas_depois": "Linhas depois", "arquivos_novos": "Arquivos novos"}))
        t += ("<p>Na carga de cada lote, só os arquivos novos entram: as linhas que já existiam continuam lá e não são relidas. "
              "Na execução sem arquivos novos, \"antes\" e \"depois\" são iguais: rodar de novo não duplica nada.</p>")
        return t

    add("<h2>7. Contagens da Bronze antes e depois do 2º lote e da 2ª execução</h2>")
    add(dado(bronze, "contagens da Bronze por execução"))
    add(figura("05-bronze-arquivos"))

    # 8
    add("<h2>8. As cinco consultas SQL, com resultado e interpretação</h2>")
    add("<p>As consultas leem as tabelas fato e dimensão da Ouro e ficam salvas como <em>views</em>. "
        "Os resultados abaixo são os da execução exportada em <code>evidencias.json</code>.</p>")
    for codigo, pergunta, _quem, _onde in PERGUNTAS:
        q = codigo.lower()
        add(f"<h3>{codigo}. {e(pergunta)}</h3><pre>{e(sql.get(q, 'consulta não encontrada em 04_consultas_mvp.sql'))}</pre>")
        add(dado(lambda q=q: "<p><strong>Resultado:</strong></p>" + tabela(ev["consultas"][q])
                 + f'<p class="interp"><strong>Interpretação:</strong> {INTERPRETAR[q](ev["consultas"][q])}</p>',
                 f"resultado e interpretação da {codigo}"))
        add(figura(q))

    # 9
    add("<h2>9. Conclusão técnica e link do repositório</h2>")
    add("<p>O componente Dados fecha o caminho da fonte até a tela do cidadão. A coleta é orquestrada e conferida no Airflow, com as três "
        "fontes em paralelo e nenhum segredo no código. A Bronze é incremental por construção: o checkpoint do Auto Loader garante que "
        "cada arquivo entra uma única vez. A Prata dá tipo e domínio aos dados, registra quanto cada regra de qualidade barrou e mantém "
        "uma linha por chave com MERGE. A Ouro organiza tudo em um modelo estrela, protege o que é pessoal e publica, por contrato, a "
        "tabela que o MVP mostra em \"Situação por quadra\".</p>"
        "<p><strong>Limites conhecidos.</strong> (1) O piloto tem poucos relatos, e parte deles é de demonstração: os indicadores de "
        "impacto medem o processo, não a melhoria física. (2) O OpenStreetMap tem lacunas: \"não mapeado\" não é \"não existe\", e a tela "
        "diz isso. (3) A API de relatos só expõe a última mudança de status, então o tempo até a revisão é uma aproximação. (4) O Free "
        "Edition obriga o envio e a entrega manuais por arquivo. <strong>Próximos passos:</strong> expor o histórico de status na API, "
        "trocar o envio manual por ingestão direta em um workspace pago e levar a Ouro ao MVP por uma tabela servida, em vez de arquivo.</p>")
    add(f'<p><strong>Repositório:</strong> <a href="{REPO_DADOS}">{REPO_DADOS}</a></p>')

    return ("<!doctype html><html lang='pt-BR'><head><meta charset='utf-8'>"
            f"<title>Caminho Livre: componente Dados (Big Data)</title><style>{CSS}</style></head><body>"
            + "\n".join(p) + "</body></html>")


if __name__ == "__main__":
    caminho_ev = RAIZ / "evidencias.json"
    evidencias = json.loads(caminho_ev.read_text(encoding="utf-8")) if caminho_ev.is_file() else None
    saida = RAIZ / "relatorio-big-data.html"
    saida.write_text(montar(evidencias), encoding="utf-8")
    print(f"Gerado: {saida}")
    if pendencias:
        print(f"\nFALTAM {len(pendencias)} ITENS (aparecem em vermelho no HTML):")
        for item in pendencias:
            print("  -", item)
    else:
        print("Completo. Abra no navegador > Imprimir > Salvar como PDF (A4, com gráficos de plano de fundo).")
