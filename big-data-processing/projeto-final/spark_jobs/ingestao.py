"""
Camada Bronze - ingestao multi-formato.

Le as fontes brutas (Parquet, CSV e JSON) e grava em Parquet sem nenhuma
transformacao de negocio, acrescentando apenas metadados de rastreabilidade.

Regra da Bronze: o dado sai daqui igual ao que entrou. Limpeza e a Silver.

Uso:
    spark-submit ingestao.py --fonte all
    spark-submit ingestao.py --fonte vendas_parceiros
"""

import argparse
import logging
import sys

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-7s | ingestao | %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger("ingestao")


# ---------------------------------------------------------------------------
# Catalogo de fontes
# ---------------------------------------------------------------------------
FONTES = {
    "vendas_master": {
        "formato": "parquet",
        "caminho": "aula_02/vendas_2023_completo.parquet",
        "destino": "vendas_master",
        "descricao": "Base historica de vendas 2023 (1M registros)",
    },
    "vendas_parceiros": {
        "formato": "csv",
        "caminho": "aula_06/dados_sujos/vendas_problemas.csv",
        "destino": "vendas_parceiros",
        "descricao": "Carga incremental de parceiros (CSV, dados nao tratados)",
    },
    "clientes": {
        "formato": "parquet",
        "caminho": "aula_02/clientes.parquet",
        "destino": "clientes",
        "descricao": "Cadastro de clientes",
    },
    "categorias": {
        "formato": "json",
        "caminho": "aula_02/categorias.json",
        "destino": "categorias",
        "descricao": "Arvore de categorias e subcategorias (JSON aninhado)",
    },
}


def criar_spark(app_name: str = "bronze-ingestao") -> SparkSession:
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.session.timeZone", "America/Sao_Paulo")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .getOrCreate()
    )


def ler_fonte(spark: SparkSession, caminho: str, formato: str) -> DataFrame:
    """Le a fonte preservando o dado bruto.

    No CSV nao usamos inferSchema de proposito: tudo entra como string para
    nao perder registros malformados antes da quarentena.
    """
    if formato == "parquet":
        return spark.read.parquet(caminho)

    if formato == "csv":
        return (
            spark.read.option("header", True)
            .option("inferSchema", False)
            .option("encoding", "UTF-8")
            .option("mode", "PERMISSIVE")
            .csv(caminho)
        )

    if formato == "json":
        return spark.read.option("multiLine", True).json(caminho)

    raise ValueError(f"Formato nao suportado: {formato}")


def adicionar_metadados(df: DataFrame, nome_fonte: str, formato: str) -> DataFrame:
    """Colunas de rastreabilidade exigidas na camada Bronze."""
    return (
        df.withColumn("_source", F.lit(nome_fonte))
        .withColumn("_source_format", F.lit(formato))
        .withColumn("_source_file", F.input_file_name())
        .withColumn("_ingestion_ts", F.current_timestamp())
        .withColumn("_ingestion_date", F.current_date())
    )


def ingerir(spark: SparkSession, nome: str, input_dir: str, output_dir: str) -> int:
    cfg = FONTES[nome]
    origem = f"{input_dir.rstrip('/')}/{cfg['caminho']}"
    destino = f"{output_dir.rstrip('/')}/{cfg['destino']}"

    logger.info("Lendo fonte '%s' (%s) em %s", nome, cfg["formato"], origem)
    df = ler_fonte(spark, origem, cfg["formato"])
    df = adicionar_metadados(df, nome, cfg["formato"])

    total = df.count()
    logger.info("Fonte '%s': %s registros, %s colunas", nome, f"{total:,}", len(df.columns))

    # Escrita particionada + partitionOverwriteMode=dynamic: o Spark substitui
    # apenas as particoes presentes nos dados, sem apagar o diretorio inteiro.
    # Alem de ser idempotente, evita a corrida entre a remocao do diretorio e a
    # criacao do _temporary, que causa perda de arquivos em volumes Docker.
    df.write.mode("overwrite").partitionBy("_ingestion_date").parquet(destino)
    logger.info("Bronze gravada em %s", destino)

    return total


def main() -> None:
    parser = argparse.ArgumentParser(description="Ingestao para a camada Bronze")
    parser.add_argument(
        "--fonte",
        default="all",
        choices=["all", *FONTES.keys()],
        help="Fonte a ingerir (default: all)",
    )
    parser.add_argument("--input-dir", default="/opt/airflow/data/raw")
    parser.add_argument("--output-dir", default="/opt/airflow/data/bronze")
    args = parser.parse_args()

    alvos = list(FONTES.keys()) if args.fonte == "all" else [args.fonte]

    spark = criar_spark()
    spark.sparkContext.setLogLevel("WARN")

    resumo = {}
    try:
        for nome in alvos:
            resumo[nome] = ingerir(spark, nome, args.input_dir, args.output_dir)
    finally:
        spark.stop()

    logger.info("=" * 60)
    logger.info("RESUMO DA INGESTAO")
    for nome, total in resumo.items():
        logger.info("  %-18s %12s registros", nome, f"{total:,}")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()
