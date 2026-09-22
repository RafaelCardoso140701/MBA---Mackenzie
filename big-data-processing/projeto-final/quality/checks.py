"""
Framework de qualidade de dados - implementacao propria.

O enunciado proibe frameworks prontos (Great Expectations, Soda), entao as
validacoes sao construidas com PySpark puro.

Cada regra e uma expressao booleana que vale True quando o registro FALHA.
Um registro pode falhar em varias regras: todas sao acumuladas na coluna
_quality_errors, o que permite auditar o motivo exato da quarentena.

Saidas:
    silver/vendas_aprovada        -> segue para a Gold
    quarantine/vendas_reprovada   -> isolado, com o motivo de cada falha
    reports/quality_report.json   -> metricas consolidadas da execucao

Uso:
    spark-submit checks.py --limite-reprovacao 0.5
"""

import argparse
import json
import logging
import os
import sys
from datetime import datetime

from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql import functions as F

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-7s | quality | %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger("quality")

UFS_VALIDAS = [
    "AC", "AL", "AP", "AM", "BA", "CE", "DF", "ES", "GO", "MA", "MT", "MS",
    "MG", "PA", "PB", "PR", "PE", "PI", "RJ", "RN", "RS", "RO", "RR", "SC",
    "SP", "SE", "TO",
]

STATUS_VALIDOS = ["pending", "confirmed", "shipped", "delivered", "cancelled", "returned"]

METODOS_PAGAMENTO_VALIDOS = ["credit_card", "debit_card", "pix", "boleto", "voucher"]

DATA_MINIMA = "2023-01-01"
DATA_MAXIMA = "2023-12-31"


def criar_spark(app_name: str = "quality-checks") -> SparkSession:
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.session.timeZone", "America/Sao_Paulo")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )


# ---------------------------------------------------------------------------
# Definicao das regras
# ---------------------------------------------------------------------------
def montar_regras() -> list:
    """Retorna (nome, dimensao, descricao, condicao_de_falha)."""
    return [
        (
            "completude_chaves",
            "completude",
            "order_id, customer_id, order_date e total_amount sao obrigatorios",
            F.col("order_id").isNull()
            | F.col("customer_id").isNull()
            | F.col("order_date").isNull()
            | F.col("total_amount").isNull(),
        ),
        (
            "unicidade_order_id",
            "unicidade",
            "order_id deve identificar um unico pedido",
            F.col("_ocorrencias_order_id") > 1,
        ),
        (
            "validade_valores",
            "validade",
            "quantity e total_amount devem ser positivos",
            (F.col("quantity").isNull())
            | (F.col("quantity") <= 0)
            | (F.col("total_amount").isNotNull() & (F.col("total_amount") <= 0)),
        ),
        (
            "validade_dominio_uf_status",
            "validade",
            "shipping_state deve ser UF brasileira e status deve existir no dominio",
            (~F.col("shipping_state").isin(UFS_VALIDAS))
            | F.col("shipping_state").isNull()
            | (~F.col("status").isin(STATUS_VALIDOS))
            | F.col("status").isNull()
            | (~F.col("payment_method").isin(METODOS_PAGAMENTO_VALIDOS))
            | F.col("payment_method").isNull(),
        ),
        (
            "validade_periodo",
            "validade",
            f"order_date deve estar entre {DATA_MINIMA} e {DATA_MAXIMA}",
            F.col("order_date").isNull()
            | (F.col("order_date") < F.lit(DATA_MINIMA).cast("date"))
            | (F.col("order_date") > F.lit(DATA_MAXIMA).cast("date")),
        ),
        (
            "consistencia_total",
            "consistencia",
            "total_amount deve bater com quantity * unit_price (tolerancia de 1 centavo)",
            F.col("quantity").isNotNull()
            & F.col("unit_price").isNotNull()
            & F.col("total_amount").isNotNull()
            & (
                F.abs(F.col("total_amount") - (F.col("quantity") * F.col("unit_price")))
                > F.lit(0.01)
            ),
        ),
    ]


def aplicar_regras(df: DataFrame, regras: list) -> DataFrame:
    """Anexa a coluna _quality_errors com os nomes das regras violadas."""
    # A unicidade precisa de contexto global, entao e calculada antes.
    janela = Window.partitionBy("order_id")
    df = df.withColumn("_ocorrencias_order_id", F.count(F.lit(1)).over(janela))

    marcadores = [
        F.when(condicao, F.lit(nome)) for nome, _, _, condicao in regras
    ]
    return df.withColumn("_quality_errors", F.array_compact(F.array(*marcadores)))


def montar_relatorio(df_marcado: DataFrame, regras: list, total: int) -> dict:
    """Uma unica passada agregada, em vez de um count por regra."""
    agregacoes = [
        F.sum(F.when(condicao, 1).otherwise(0)).alias(nome)
        for nome, _, _, condicao in regras
    ]
    agregacoes.append(
        F.sum(F.when(F.size("_quality_errors") > 0, 1).otherwise(0)).alias("_reprovados")
    )
    linha = df_marcado.agg(*agregacoes).collect()[0].asDict()

    reprovados = int(linha.pop("_reprovados") or 0)
    aprovados = total - reprovados

    return {
        "executado_em": datetime.now().isoformat(timespec="seconds"),
        "registros_avaliados": total,
        "registros_aprovados": aprovados,
        "registros_reprovados": reprovados,
        "taxa_reprovacao": round(reprovados / total, 4) if total else 0.0,
        "checks": [
            {
                "nome": nome,
                "dimensao": dimensao,
                "descricao": descricao,
                "registros_com_falha": int(linha.get(nome) or 0),
                "status": "PASSOU" if int(linha.get(nome) or 0) == 0 else "FALHOU",
            }
            for nome, dimensao, descricao, _ in regras
        ],
    }


def imprimir_relatorio(rel: dict) -> None:
    logger.info("=" * 74)
    logger.info("RELATORIO DE QUALIDADE")
    logger.info("-" * 74)
    logger.info("%-34s %12s %10s %8s", "CHECK", "DIMENSAO", "FALHAS", "STATUS")
    for c in rel["checks"]:
        logger.info(
            "%-34s %12s %10s %8s",
            c["nome"],
            c["dimensao"],
            f"{c['registros_com_falha']:,}",
            c["status"],
        )
    logger.info("-" * 74)
    logger.info("Avaliados : %s", f"{rel['registros_avaliados']:,}")
    logger.info("Aprovados : %s", f"{rel['registros_aprovados']:,}")
    logger.info("Quarentena: %s (%.2f%%)", f"{rel['registros_reprovados']:,}", rel["taxa_reprovacao"] * 100)
    logger.info("=" * 74)


def main() -> None:
    parser = argparse.ArgumentParser(description="Checks de qualidade e quarentena")
    parser.add_argument("--silver-dir", default="/opt/airflow/data/silver")
    parser.add_argument("--quarantine-dir", default="/opt/airflow/data/quarantine")
    parser.add_argument("--report-dir", default="/opt/airflow/data/reports")
    parser.add_argument(
        "--limite-reprovacao",
        type=float,
        default=0.5,
        help="Acima desta fracao de reprovacao o job falha e trava o pipeline",
    )
    args = parser.parse_args()

    spark = criar_spark()
    spark.sparkContext.setLogLevel("WARN")

    try:
        df = spark.read.parquet(f"{args.silver_dir}/vendas")
        total = df.count()
        logger.info("Avaliando %s registros da Silver", f"{total:,}")

        regras = montar_regras()
        df_marcado = aplicar_regras(df, regras).cache()

        relatorio = montar_relatorio(df_marcado, regras, total)

        aprovados = df_marcado.filter(F.size("_quality_errors") == 0).drop(
            "_quality_errors", "_ocorrencias_order_id"
        )
        reprovados = df_marcado.filter(F.size("_quality_errors") > 0).withColumn(
            "_quarantined_at", F.current_timestamp()
        )

        aprovados.write.mode("overwrite").partitionBy("ano_mes").parquet(f"{args.silver_dir}/vendas_aprovada")
        reprovados.write.mode("overwrite").partitionBy("_source").parquet(f"{args.quarantine_dir}/vendas_reprovada")

        logger.info("Aprovados gravados em %s/vendas_aprovada", args.silver_dir)
        logger.info("Quarentena gravada em %s/vendas_reprovada", args.quarantine_dir)

        # Amostra da quarentena - ajuda muito na demonstracao ao vivo
        logger.info("Amostra da quarentena:")
        reprovados.select(
            "order_id", "quantity", "total_amount", "order_date",
            "shipping_state", "status", "_quality_errors",
        ).show(10, truncate=False)

        imprimir_relatorio(relatorio)

        os.makedirs(args.report_dir, exist_ok=True)
        caminho = os.path.join(args.report_dir, "quality_report.json")
        with open(caminho, "w", encoding="utf-8") as arquivo:
            json.dump(relatorio, arquivo, indent=2, ensure_ascii=False)
        logger.info("Relatorio salvo em %s", caminho)

        df_marcado.unpersist()

        # Quality gate: acima do limite, o pipeline para aqui.
        if relatorio["taxa_reprovacao"] > args.limite_reprovacao:
            logger.error(
                "Taxa de reprovacao %.2f%% acima do limite de %.2f%%. Pipeline interrompido.",
                relatorio["taxa_reprovacao"] * 100,
                args.limite_reprovacao * 100,
            )
            sys.exit(1)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
