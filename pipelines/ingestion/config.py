import os
from dotenv import load_dotenv

load_dotenv()

API_URL = "https://api.portaldatransparencia.gov.br/api-de-dados"
API_KEY = os.getenv("TRANSPARENCIA_API_KEY")

RAW_DATA_PATH   = "data/bronze"

ANO_REFERENCIA = 2024
PAGINA_TAMANHO = 500