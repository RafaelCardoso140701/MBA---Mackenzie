"""
Camada Gold - tabelas agregadas para o dashboard executivo.

Le apenas os registros aprovados pelos checks de qualidade. Nenhum dado em
quarentena entra em metrica de negocio.

Tabelas produzidas:
    faturamento_por_estado  - receita, ticket medio e ranking por UF
    vendas_mensais          - serie temporal com variacao mes a mes
    faturamento_por_segmento- cruzamento vendas x cadastro de clientes

Uso:
    spark-submit agregacao.py
"""

import argparse
import logging
import sys

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-7s | gold | %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger("gold")

# Apenas pedidos efetivados entram no faturamento
STATUS_FATURAVEIS = ["confirmed", "shipped", "delivered"]


def criar_spark(app_name: str = "gold-agregacao") -> SparkSession:
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.session.timeZone", "America/Sao_Paulo")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )


def faturamento_por_estado(vendas: DataFrame) -> DataFrame:
    agregado = vendas.groupBy("shipping_state").agg(
        F.round(F.sum("total_amount"), 2).alias("receita_total"),
        F.countDistinct("order_id").alias("qtd_pedidos"),
        F.countDistinct("customer_id").alias("clientes_unicos"),
        F.sum("quantity").alias("itens_vendidos"),
        F.round(F.avg("total_amount"), 2).alias("ticket_medio"),
    )

    total_nacional = agregado.agg(F.sum("receita_total")).collect()[0][0] or 1

    return (
        agregado.withColumn(
            "participacao_pct",
            F.round(F.col("receita_total") / F.lit(total_nacional) * 100, 2),
        )
        .withColumn(
            "ranking",
            F.row_number().over(Window.orderBy(F.col("receita_total").desc())),
        )
        .orderBy("ranking")
    )


def vendas_mensais(vendas: DataFrame) -> DataFrame:
    agregado = vendas.groupBy("ano_mes").agg(
        F.round(F.sum("total_amount"), 2).alias("receita_total"),
        F.countDistinct("order_id").alias("qtd_pedidos"),
        F.countDistinct("customer_id").alias("clientes_unicos"),
        F.sum("quantity").alias("itens_vendidos"),
        F.round(F.avg("total_amount"), 2).alias("ticket_medio"),
    )

    janela = Window.orderBy("ano_mes")

    return (
        agregado.withColumn("receita_mes_anterior", F.lag("receita_total").over(janela))
        .withColumn(
            "variacao_mom_pct",
            F.round(
                (F.col("receita_total") - F.col("receita_mes_anterior"))
                / F.col("receita_mes_anterior")
                * 100,
                2,
            ),
        )
        .withColumn(
            "receita_acumulada",
            F.round(F.sum("receita_total").over(janela.rowsBetween(Window.unboundedPreceding, 0)), 2),
        )
        .orderBy("ano_mes")
    )


def faturamento_por_segmento(vendas: DataFrame, clientes: DataFrame) -> DataFrame:
    """Join distribuido entre o fato e a dimensao de clientes."""
    enriquecido = vendas.join(
        clientes.select("customer_id", "segment", F.col("state").alias("estado_cliente")),
        on="customer_id",
        how="left",
    )

    return (
        enriquecido.withColumn("segment", F.coalesce(F.col("segment"), F.lit("Nao cadastrado")))
        .groupBy("segment")
        .agg(
            F.round(F.sum("total_amount"), 2).alias("receita_total"),
            F.countDistinct("order_id").alias("qtd_pedidos"),
            F.countDistinct("customer_id").alias("clientes_unicos"),
            F.round(F.avg("total_amount"), 2).alias("ticket_medio"),
        )
        .orderBy(F.col("receita_total").desc())
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Agregacoes da camada Gold")
    parser.add_argument("--silver-dir", default="/opt/airflow/data/silver")
    parser.add_argument("--gold-dir", default="/opt/airflow/data/gold")
    args = parser.parse_args()

    spark = criar_spark()
    spark.sparkContext.setLogLevel("WARN")

    try:
        vendas = spark.read.parquet(f"{args.silver_dir}/vendas_aprovada")
        clientes = spark.read.parquet(f"{args.silver_dir}/clientes")

        faturaveis = vendas.filter(F.col("status").isin(STATUS_FATURAVEIS))
        logger.info(
            "Aprovados: %s | faturaveis (%s): %s",
            f"{vendas.count():,}",
            ", ".join(STATUS_FATURAVEIS),
            f"{faturaveis.count():,}",
        )

        tabelas = {
            "faturamento_por_estado": faturamento_por_estado(faturaveis),
            "vendas_mensais": vendas_mensais(faturaveis),
            "faturamento_por_segmento": faturamento_por_segmento(faturaveis, clientes),
        }

        # data_processamento serve de chave de particao: mesmo padrao das demais
        # camadas, mantendo a escrita dinamica e idempotente em toda a Gold.
        for nome, df in tabelas.items():
            df = df.withColumn("data_processamento", F.current_date())
            df.write.mode("overwrite").partitionBy("data_processamento").parquet(
                f"{args.gold_dir}/{nome}"
            )
            logger.info("Gold gravada: %s/%s", args.gold_dir, nome)
            logger.info("Previa de %s:", nome)
            df.show(12, truncate=False)

    finally:
        spark.stop()

    logger.info("=" * 60)
    logger.info("GOLD CONCLUIDA - 3 tabelas agregadas")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()
