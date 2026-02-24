import logging
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import col, to_date, trim, upper, regexp_replace, year
import os

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
BRONZE_PATH = os.path.join(BASE_DIR, 'data', 'bronze')
SILVER_PATH = os.path.join(BASE_DIR, 'data', 'silver')


def criar_spark_session():
    return (
        SparkSession.builder
        .appName("GastosPublicos_BronzeToSilver")
        .master("local[*]")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )


def encontrar_arquivo_mais_recente(pasta: str) -> str:
    arquivos = [
        f for f in os.listdir(pasta)
        if f.endswith(".csv") and f.startswith("gastos_")
    ]
    if not arquivos:
        raise FileNotFoundError(f"Nenhum arquivo CSV encontrado em {pasta}")
    arquivos.sort(reverse=True)
    caminho = os.path.join(pasta, arquivos[0])
    logger.info(f"Arquivo selecionado: {caminho}")
    return caminho


def limpar_valor_monetario(df, coluna: str):
    return df.withColumn(
        coluna,
        regexp_replace(col(coluna), r"\.", "").cast("string")
    ).withColumn(
        coluna,
        regexp_replace(col(coluna), ",", ".")
    ).withColumn(
        coluna,
        col(coluna).cast("double")
    )


def normalizar_nome_orgao(df, coluna: str):
    return df.withColumn(
        coluna,
        upper(trim(col(coluna)))
    )


def remover_nulos_criticos(df):
    total_antes = df.count()
    df = df.filter(
        col("orgao").isNotNull() &
        col("pago").isNotNull() &
        col("empenhado").isNotNull() &
        col("liquidado").isNotNull()
    )
    total_depois = df.count()
    removidos = total_antes - total_depois
    if removidos > 0:
        logger.warning(f"{removidos} linhas removidas por nulos críticos.")
    return df


def executar_transformacao():
    logger.info("=== INÍCIO DA TRANSFORMAÇÃO ===")
    spark = criar_spark_session()

    arquivo = encontrar_arquivo_mais_recente(BRONZE_PATH)
    df = spark.read.csv(arquivo, header=True, inferSchema=False, encoding="utf-8")

    logger.info(f"Linhas lidas: {df.count()}")
    logger.info(f"Colunas: {df.columns}")


    df = df.withColumnRenamed("orgaoSuperior", "orgao_superior") \
           .withColumnRenamed("codigoOrgaoSuperior", "codigo_orgao_superior") \
           .withColumnRenamed("codigoOrgao", "codigo_orgao")


    df = limpar_valor_monetario(df, "empenhado")
    df = limpar_valor_monetario(df, "liquidado")
    df = limpar_valor_monetario(df, "pago")

    df = normalizar_nome_orgao(df, "orgao")
    df = normalizar_nome_orgao(df, "orgao_superior")


    df = df.withColumn("ano", col("ano").cast("integer"))

    df = remover_nulos_criticos(df)

    df = df.dropDuplicates()

    logger.info(f"Linhas após limpeza: {df.count()}")

    df.write \
      .mode("overwrite") \
      .parquet(SILVER_PATH)

    logger.info(f"Silver salvo em: {SILVER_PATH}")
    logger.info("=== FIM DA TRANSFORMAÇÃO ===")


if __name__ == "__main__":
    executar_transformacao()