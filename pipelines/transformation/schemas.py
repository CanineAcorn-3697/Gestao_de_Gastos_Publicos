from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType, DateType, IntegerType
)

SCHEMA_BRONZE = StructType([
    StructField("orgaoSuperior", StringType(), True),
    StructField("orgao", StringType(), True),
    StructField("valorPago", StringType(), True),
    StructField("dataReferencia", StringType(), True),
])

SCHEMA_SILVER = StructType([
    StructField("orgaoSuperior", StringType(), False),
    StructField("orgao", StringType(), False),
    StructField("valor_pago", DoubleType(), False),
    StructField("data_referencia", DateType(), False),
    StructField("ano_referencia", IntegerType(), False),
])