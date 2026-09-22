"""
Camada Silver - normalizacao, limpeza e deduplicacao.

Unifica as duas fontes de vendas (Parquet tipado + CSV cru) em um schema
canonico, padroniza valores textuais, trata nulos e remove duplicatas exatas.

Importante: a Silver NAO descarta registro ruim. Ela normaliza. A separacao
entre valido e invalido e responsabilidade do checks.py, para que a quarentena
fique auditavel.

Uso:
    spark-submit transformacao.py --entidade all
"""

import argparse
import logging
import sys

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-7s | silver | %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger("silver")

# Valores que representam ausencia de dado mas chegam como texto
NULOS_TEXTUAIS = ["", "NULL", "null", "Null", "N/A", "n/a", "NaN", "nan", "None", "-"]

COLUNAS_NEGOCIO_VENDAS = [
    "order_id",
    "customer_id",
    "product_id",
    "quantity",
    "unit_price",
    "total_amount",
    "order_date",
    "payment_method",
    "shipping_city",
    "shipping_state",
    "status",
    "partner_source",
]

COLUNAS_METADADOS = ["_source", "_source_format", "_ingestion_ts"]


def criar_spark(app_name: str = "silver-transformacao") -> SparkSession:
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.session.timeZone", "America/Sao_Paulo")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )


def limpar_strings(df: DataFrame, colunas: list) -> DataFrame:
    """Trim + conversao de nulos textuais em NULL de verdade."""
    for coluna in colunas:
        df = df.withColumn(
            coluna,
            F.when(F.trim(F.col(coluna)).isin(NULOS_TEXTUAIS), None).otherwise(
                F.trim(F.col(coluna))
            ),
        )
    return df


# ---------------------------------------------------------------------------
# VENDAS
# ---------------------------------------------------------------------------
def normalizar_vendas(df: DataFrame) -> DataFrame:
    """Aplica o schema canonico.

    O CSV chega inteiramente como string; o Parquet chega tipado. Os casts
    abaixo produzem NULL quando o valor e inconversivel - de proposito, para
    que o registro sobreviva ate a quarentena em vez de derrubar o job.
    """
    texto = [
        "order_id",
        "customer_id",
        "product_id",
        "payment_method",
        "shipping_city",
        "shipping_state",
        "status",
        "partner_source",
    ]
    df = limpar_strings(df, texto)

    df = (
        df.withColumn("quantity", F.col("quantity").cast("int"))
        .withColumn("unit_price", F.col("unit_price").cast("double"))
        .withColumn("total_amount", F.col("total_amount").cast("double"))
        .withColumn("order_date", F.to_date(F.col("order_date")))
    )

    # Padronizacao de dominio textual
    df = (
        df.withColumn("shipping_state", F.upper(F.col("shipping_state")))
        .withColumn("status", F.lower(F.col("status")))
        .withColumn("payment_method", F.lower(F.col("payment_method")))
        .withColumn("shipping_city", F.initcap(F.col("shipping_city")))
    )

    # Tratamento de nulo derivavel: total_amount reconstruido a partir dos
    # componentes quando ambos existem. Marcamos para rastreabilidade.
    pode_recalcular = (
        F.col("total_amount").isNull()
        & F.col("quantity").isNotNull()
        & F.col("unit_price").isNotNull()
    )
    df = df.withColumn("_total_recalculado", pode_recalcular).withColumn(
        "total_amount",
        F.when(pode_recalcular, F.round(F.col("quantity") * F.col("unit_price"), 2))
        .otherwise(F.col("total_amount")),
    )

    # Colunas derivadas usadas pela Gold
    df = df.withColumn("ano_mes", F.date_format(F.col("order_date"), "yyyy-MM"))

    return df.select(
        *COLUNAS_NEGOCIO_VENDAS,
        "ano_mes",
        "_total_recalculado",
        *COLUNAS_METADADOS,
    )


def processar_vendas(spark: SparkSession, bronze: str, silver: str) -> int:
    master = spark.read.parquet(f"{bronze}/vendas_master")
    parceiros = spark.read.parquet(f"{bronze}/vendas_parceiros")

    logger.info("vendas_master: %s | vendas_parceiros: %s", master.count(), parceiros.count())

    df = normalizar_vendas(master).unionByName(normalizar_vendas(parceiros))
    antes = df.count()

    # Deduplicacao: remove linhas integralmente identicas nas colunas de
    # negocio. Divergencias de order_id com conteudo diferente NAO sao
    # removidas aqui - viram falha no check de unicidade.
    df = df.dropDuplicates(COLUNAS_NEGOCIO_VENDAS)
    depois = df.count()
    logger.info("Deduplicacao: %s -> %s (%s removidos)", antes, depois, antes - depois)

    df.write.mode("overwrite").partitionBy("ano_mes").parquet(f"{silver}/vendas")
    logger.info("Silver gravada em %s/vendas", silver)
    return depois


# ---------------------------------------------------------------------------
# CLIENTES
# ---------------------------------------------------------------------------
def processar_clientes(spark: SparkSession, bronze: str, silver: str) -> int:
    df = spark.read.parquet(f"{bronze}/clientes")

    df = limpar_strings(df, ["customer_id", "customer_name", "email", "phone", "city", "state", "segment"])
    df = (
        df.withColumn("state", F.upper(F.col("state")))
        .withColumn("city", F.initcap(F.col("city")))
        .withColumn("email", F.lower(F.col("email")))
        .withColumn("segment", F.initcap(F.col("segment")))
        .withColumn("registration_date", F.to_date(F.col("registration_date")))
    )

    # Um cliente = um registro. Em caso de duplicata, vence o cadastro mais recente.
    janela = Window.partitionBy("customer_id").orderBy(F.col("registration_date").desc_nulls_last())
    antes = df.count()
    df = df.withColumn("_rn", F.row_number().over(janela)).filter(F.col("_rn") == 1).drop("_rn")
    depois = df.count()
    logger.info("Clientes: %s -> %s apos deduplicacao", antes, depois)

    df.select(
        "customer_id",
        "customer_name",
        "email",
        "city",
        "state",
        "segment",
        "registration_date",
        *COLUNAS_METADADOS,
    ).write.mode("overwrite").partitionBy("state").parquet(f"{silver}/clientes")

    logger.info("Silver gravada em %s/clientes", silver)
    return depois


# ---------------------------------------------------------------------------
# CATEGORIAS
# ---------------------------------------------------------------------------
def processar_categorias(spark: SparkSession, bronze: str, silver: str) -> int:
    """Achata a arvore JSON (categoria -> subcategorias) em tabela tabular."""
    df = spark.read.parquet(f"{bronze}/categorias")

    df = (
        df.select(F.explode("categorias").alias("cat"), *COLUNAS_METADADOS)
        .select(
            F.col("cat.category_id").alias("category_id"),
            F.col("cat.category_name").alias("category_name"),
            F.explode("cat.subcategories").alias("sub"),
            *COLUNAS_METADADOS,
        )
        .select(
            "category_id",
            "category_name",
            F.col("sub.subcategory_id").alias("subcategory_id"),
            F.col("sub.subcategory_name").alias("subcategory_name"),
            *COLUNAS_METADADOS,
        )
    )

    total = df.count()
    df.write.mode("overwrite").partitionBy("category_id").parquet(f"{silver}/categorias")
    logger.info("Silver gravada em %s/categorias (%s linhas)", silver, total)
    return total


def main() -> None:
    parser = argparse.ArgumentParser(description="Transformacao para a camada Silver")
    parser.add_argument(
        "--entidade", default="all", choices=["all", "vendas", "clientes", "categorias"]
    )
    parser.add_argument("--bronze-dir", default="/opt/airflow/data/bronze")
    parser.add_argument("--silver-dir", default="/opt/airflow/data/silver")
    args = parser.parse_args()

    spark = criar_spark()
    spark.sparkContext.setLogLevel("WARN")

    resumo = {}
    try:
        if args.entidade in ("all", "vendas"):
            resumo["vendas"] = processar_vendas(spark, args.bronze_dir, args.silver_dir)
        if args.entidade in ("all", "clientes"):
            resumo["clientes"] = processar_clientes(spark, args.bronze_dir, args.silver_dir)
        if args.entidade in ("all", "categorias"):
            resumo["categorias"] = processar_categorias(spark, args.bronze_dir, args.silver_dir)
    finally:
        spark.stop()

    logger.info("=" * 60)
    logger.info("RESUMO DA SILVER")
    for nome, total in resumo.items():
        logger.info("  %-14s %12s registros", nome, f"{total:,}")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()
