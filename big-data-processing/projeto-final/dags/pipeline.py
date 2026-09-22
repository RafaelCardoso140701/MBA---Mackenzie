"""
DAG do pipeline Medallion - ShopBrasil / DataFlow Analytics.

Encadeamento:
    aguardar_dados (sensor)
        -> ingestao_bronze (spark)
        -> transformacao_silver (spark)
        -> quality_checks (spark)
        -> agregacao_gold (spark)
        -> notificar_conclusao (python)

O quality_checks e um portao: se a taxa de reprovacao estourar o limite, o
job sai com codigo 1, a task falha e a Gold nao roda. Dado ruim nao vira
metrica de negocio.
"""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.sensors.filesystem import FileSensor

logger = logging.getLogger(__name__)

BASE = "/opt/airflow"
DATA = f"{BASE}/data"
ARQUIVO_GATILHO = f"{DATA}/raw/aula_02/vendas_2023_completo.parquet"

CONF_SPARK = {
    "spark.driver.memory": "2g",
    "spark.sql.shuffle.partitions": "8",
    "spark.sql.session.timeZone": "America/Sao_Paulo",
}

default_args = {
    "owner": "dataflow-analytics",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
    "depends_on_past": False,
}


def notificar_conclusao(**contexto) -> str:
    """Le o relatorio de qualidade e publica o resumo da execucao no log."""
    caminho = f"{DATA}/reports/quality_report.json"

    if not os.path.exists(caminho):
        raise FileNotFoundError(f"Relatorio de qualidade nao encontrado em {caminho}")

    with open(caminho, encoding="utf-8") as arquivo:
        relatorio = json.load(arquivo)

    falhas = [c for c in relatorio["checks"] if c["status"] == "FALHOU"]

    linhas = [
        "",
        "=" * 66,
        "PIPELINE CONCLUIDO - ShopBrasil",
        "=" * 66,
        f"Execucao          : {contexto['ds']}",
        f"Registros na Silver: {relatorio['registros_avaliados']:,}",
        f"Aprovados          : {relatorio['registros_aprovados']:,}",
        f"Em quarentena      : {relatorio['registros_reprovados']:,} "
        f"({relatorio['taxa_reprovacao'] * 100:.2f}%)",
        f"Checks com falha   : {len(falhas)} de {len(relatorio['checks'])}",
        "-" * 66,
    ]

    for check in relatorio["checks"]:
        linhas.append(
            f"  [{check['status']:6}] {check['nome']:<32} "
            f"{check['registros_com_falha']:>10,} falhas"
        )

    linhas += [
        "-" * 66,
        "Tabelas Gold disponiveis:",
        "  - faturamento_por_estado",
        "  - vendas_mensais",
        "  - faturamento_por_segmento",
        "=" * 66,
    ]

    mensagem = "\n".join(linhas)
    logger.info(mensagem)

    # Artefato consultavel fora do Airflow
    os.makedirs(f"{DATA}/reports", exist_ok=True)
    with open(f"{DATA}/reports/ultima_execucao.txt", "w", encoding="utf-8") as arquivo:
        arquivo.write(mensagem)

    return f"quarentena={relatorio['registros_reprovados']}"


with DAG(
    dag_id="pipeline_medallion_shopbrasil",
    description="Bronze -> Silver -> Quality -> Gold para o dashboard executivo",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule=None,          # disparo manual: e uma demo ao vivo
    catchup=False,
    max_active_runs=1,
    tags=["mba", "medallion", "pyspark", "shopbrasil"],
) as dag:

    aguardar_dados = FileSensor(
        task_id="aguardar_dados",
        filepath=ARQUIVO_GATILHO,
        fs_conn_id="fs_default",
        poke_interval=10,
        timeout=120,
        mode="poke",
        doc_md="Confirma que a fonte de vendas chegou antes de acionar o Spark.",
    )

    ingestao_bronze = SparkSubmitOperator(
        task_id="ingestao_bronze",
        application=f"{BASE}/spark_jobs/ingestao.py",
        conn_id="spark_local",
        application_args=["--fonte", "all"],
        conf=CONF_SPARK,
        name="bronze-ingestao",
        verbose=False,
        doc_md="Le 4 fontes em 3 formatos e grava Parquet com metadados de origem.",
    )

    transformacao_silver = SparkSubmitOperator(
        task_id="transformacao_silver",
        application=f"{BASE}/spark_jobs/transformacao.py",
        conn_id="spark_local",
        application_args=["--entidade", "all"],
        conf=CONF_SPARK,
        name="silver-transformacao",
        verbose=False,
        doc_md="Normaliza schema, trata nulos e deduplica.",
    )

    quality_checks = SparkSubmitOperator(
        task_id="quality_checks",
        application=f"{BASE}/quality/checks.py",
        conn_id="spark_local",
        application_args=["--limite-reprovacao", "0.5"],
        conf=CONF_SPARK,
        name="quality-checks",
        verbose=False,
        doc_md="6 validacoes customizadas + quarentena. Falha o pipeline acima do limite.",
    )

    agregacao_gold = SparkSubmitOperator(
        task_id="agregacao_gold",
        application=f"{BASE}/spark_jobs/agregacao.py",
        conn_id="spark_local",
        conf=CONF_SPARK,
        name="gold-agregacao",
        verbose=False,
        doc_md="3 tabelas de negocio a partir apenas dos registros aprovados.",
    )

    notificar = PythonOperator(
        task_id="notificar_conclusao",
        python_callable=notificar_conclusao,
        doc_md="Consolida o relatorio de qualidade e registra o resumo da execucao.",
    )

    (
        aguardar_dados
        >> ingestao_bronze
        >> transformacao_silver
        >> quality_checks
        >> agregacao_gold
        >> notificar
    )
