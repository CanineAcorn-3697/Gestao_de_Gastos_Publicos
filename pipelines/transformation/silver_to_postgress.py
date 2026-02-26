import logging
from pyspark.sql import SparkSession
import os

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
SILVER_PATH = os.path.join(BASE_DIR, 'data', 'silver', 'gastos.parquet')

PG_HOST = os.getenv("POSTGRES_HOST", "localhost")
PG_PORT = os.getenv("POSTGRES_PORT", "5432")
PG_DB = os.getenv("POSTGRES_DB", "gastos_publicos")
PG_USER = os.getenv("POSTGRES_USER", "postgres")
PG_PASSWORD = os.getenv("POSTGRES_PASSWORD", "postgress")

def carregar_para_postgres():
    logger.info("=== INÍCIO DA CARGA POSTGRESQL ===")
    spark = (
        SparkSession.builder
        .appName("GastosPublicos_SilverToPostgres")
        .master("local[*]")
        .config("spark.jars.packages", "org.postgresql:postgresql:42.7.3")
        .getOrCreate()
    )

    df = spark.read.parquet(SILVER_PATH)
    logger.info(f"Linhas a Carregar: {df.count()}")

    jdbc_url = f"jdbc:postgresql://{PG_HOST}:{PG_PORT}/{PG_DB}"

    df.write \
        .format("jdbc") \
        .option("url", jdbc_url) \
        .option("dbtable", "silver_gastos") \
        .option("user", PG_USER) \
        .option("password", PG_PASSWORD) \
        .option("driver", "org.postgresql.Driver") \
        .mode("overwrite") \
        .save()

    logger.info("Carga Concluida com Sucesso")
    logger.info("=== FIM DA CARGA POSTGRESQL ===")

if __name__ == "__main__":
    carregar_para_postgres()