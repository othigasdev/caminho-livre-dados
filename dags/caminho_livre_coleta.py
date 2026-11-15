from __future__ import annotations

import json
import os
from datetime import timedelta
from pathlib import Path

import pendulum
import requests
from airflow.sdk import Variable, dag, task

FUSO = "America/Sao_Paulo"
CHEGADA = Path(os.getenv("CHEGADA_DIR", "/opt/airflow/chegada"))
AMOSTRAS = Path(os.getenv("AMOSTRAS_DIR", "/opt/airflow/amostras"))

BBOX = (-23.5700, -46.6600, -23.5590, -46.6450)
CENTRO = (round((BBOX[0] + BBOX[2]) / 2, 4), round((BBOX[1] + BBOX[3]) / 2, 4))

MVP_PADRAO = "https://caminholivre-gs.azurewebsites.net"
OVERPASS_PADRAO = "https://overpass-api.de/api/interpreter"
OPEN_METEO = "https://api.open-meteo.com/v1/forecast"
AGENTE = {"User-Agent": "caminho-livre-gs-fiap/1.0 (projeto academico FIAP)"}

CAMPO_REGISTROS = {"osm": "elements", "relatos": "itens", "clima": "hourly"}

OVERPASS_QUERY = """
[out:json][timeout:90];
(
  node["highway"="crossing"]({s},{w},{n},{e});
  node["kerb"]({s},{w},{n},{e});
  way["highway"="footway"]({s},{w},{n},{e});
  way["highway"="pedestrian"]({s},{w},{n},{e});
);
out center;
""".strip()


def dia_referencia(contexto: dict) -> str:
    base = contexto.get("logical_date") or contexto["dag_run"].run_after
    return pendulum.instance(base).in_timezone(FUSO).subtract(days=1).to_date_string()


def caminho(fonte: str, dia: str) -> Path:
    return CHEGADA / fonte / f"{fonte}_{dia}.json"


def gravar(fonte: str, dia: str, texto: str) -> str:
    json.loads(texto) 
    destino = caminho(fonte, dia)
    destino.parent.mkdir(parents=True, exist_ok=True)
    temporario = destino.with_suffix(".tmp")
    temporario.write_text(texto, encoding="utf-8")
    temporario.replace(destino)
    return str(destino)


def notificar(titulo: str, mensagem: str, etiqueta: str) -> None:
    url = Variable.get("ntfy_topic_secret", default=None)
    if not url:
        print(f"[sem ntfy_topic_secret] {titulo}: {mensagem}")
        return
    requests.post(url, data=mensagem.encode("utf-8"), timeout=10,
                  headers={"Title": titulo, "Tags": etiqueta}).raise_for_status()


def avisar_falha(contexto: dict) -> None:
    ti = contexto["task_instance"]
    notificar("Caminho Livre: coleta FALHOU",
              f"DAG {ti.dag_id}, tarefa {ti.task_id}, execucao {contexto.get('run_id')}", "rotating_light")


@dag(
    dag_id="caminho_livre_coleta",
    description="Coleta diaria: OpenStreetMap, relatos do MVP e clima -> pasta chegada/",
    schedule="0 6 * * *",  
    start_date=pendulum.datetime(2026, 11, 10, tz=FUSO),
    catchup=True,
    max_active_runs=1,
    default_args={
        "retries": 3,
        "retry_delay": timedelta(minutes=2),
        "retry_exponential_backoff": True,
        "on_failure_callback": avisar_falha,
    },
    tags=["caminho-livre", "global-solution", "coleta"],
)
def caminho_livre_coleta():

    @task
    def coletar_osm(**contexto) -> str:
        s, w, n, e = BBOX
        resposta = requests.post(
            Variable.get("overpass_url", default=OVERPASS_PADRAO),
            data={"data": OVERPASS_QUERY.format(s=s, w=w, n=n, e=e)}, headers=AGENTE, timeout=120)
        resposta.raise_for_status()
        return gravar("osm", dia_referencia(contexto), resposta.text)

    @task
    def coletar_relatos(**contexto) -> str:
        base = Variable.get("caminholivre_api_url", default=MVP_PADRAO).rstrip("/")
        resposta = requests.get(f"{base}/api/reports", headers=AGENTE, timeout=60)
        resposta.raise_for_status()
        return gravar("relatos", dia_referencia(contexto), resposta.text)

    @task
    def coletar_clima(**contexto) -> str:
        dia = dia_referencia(contexto)
        resposta = requests.get(OPEN_METEO, headers=AGENTE, timeout=30, params={
            "latitude": CENTRO[0], "longitude": CENTRO[1], "start_date": dia, "end_date": dia,
            "hourly": "temperature_2m,precipitation", "timezone": FUSO})
        resposta.raise_for_status()
        return gravar("clima", dia, resposta.text)

    @task
    def conferir_chegada(**contexto) -> dict:
        dia = dia_referencia(contexto)
        resumo, problemas = {}, []
        for fonte, campo in CAMPO_REGISTROS.items():
            arquivo = caminho(fonte, dia)
            if not arquivo.is_file() or arquivo.stat().st_size == 0:
                problemas.append(f"{arquivo.name}: nao chegou ou esta vazio")
                continue
            registros = json.loads(arquivo.read_text(encoding="utf-8")).get(campo)
            total = len(registros["time"]) if fonte == "clima" and registros else len(registros or [])
            if total == 0:
                problemas.append(f"{arquivo.name}: sem registros em '{campo}'")
                continue
            resumo[fonte] = {"arquivo": arquivo.name, "bytes": arquivo.stat().st_size, "registros": total}
            print(f"OK {arquivo.name}: {arquivo.stat().st_size} bytes, {total} registros")
        if problemas:
            raise ValueError("Conferencia da chegada falhou: " + "; ".join(problemas))
        return {"dia": dia, "fontes": resumo}

    @task
    def atualizar_amostras(conferencia: dict) -> None:
        AMOSTRAS.mkdir(parents=True, exist_ok=True)
        for fonte, campo in CAMPO_REGISTROS.items():
            dados = json.loads(caminho(fonte, conferencia["dia"]).read_text(encoding="utf-8"))
            if fonte == "clima":
                dados[campo] = {chave: valores[:5] for chave, valores in dados[campo].items()}
            else:
                dados[campo] = dados[campo][:5]
            (AMOSTRAS / f"{fonte}_amostra.json").write_text(
                json.dumps(dados, ensure_ascii=False, indent=2), encoding="utf-8")

    @task
    def notificar_sucesso(conferencia: dict) -> None:
        linhas = [f"{f}: {d['registros']} registros ({d['bytes']} bytes)" for f, d in conferencia["fontes"].items()]
        notificar(f"Caminho Livre: coleta de {conferencia['dia']} ok", "\n".join(linhas), "white_check_mark")

    conferencia = conferir_chegada()
    [coletar_osm(), coletar_relatos(), coletar_clima()] >> conferencia
    atualizar_amostras(conferencia)
    notificar_sucesso(conferencia)


caminho_livre_coleta()
