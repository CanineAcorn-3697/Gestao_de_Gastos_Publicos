import requests
import pandas as pd
import os
import logging
from datetime import datetime
from config import API_URL, API_KEY, RAW_DATA_PATH, ANO_REFERENCIA, PAGINA_TAMANHO

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()
        logging.FileHandler("data/logs/ingestion.log"),
    ]
)
logger = logging.getLogger(__name__)

def buscar_pagina(ano: int, pagina: int) -> list[dict]:
    headers = {
        "chave-api-dados": API_KEY,
        "Accept": "application/json"
    }
    params = {
        "ano": ano,
        "pagina": pagina,
        "tamanhoPagina": PAGINA_TAMANHO
    }
    url = f"{API_URL}/despesas/por_orgao"

    try:
        response = requests.geturl, headers=headers, params=params, timeout=30)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.HTTPError as e:
        logger.error(f"Erro HTTP na página {pagina}: {e}")
        raise
    except requests.exceptions.Timeout:
        logger.error(f"Timeout na página {pagina}")
        raise
    except requests.exceptions.RequestException as e:
        logger.error(f"Erro de conexão: {e}")
        raise

def baixar_todos_os_dados(ano: int) -> pd.DataFrame:
    todas_as_paginas = []
    pagina = 1
    logger.info(f"Iniciando download - Ano: {ano}")
    while True:
        logger.info(f"Baixando página {pagina}...")
        dados = buscar_pagina(ano, pagina)
        if not dados:
            logger.info(f"Paginação completa. Total de páginas: {pagina - 1}")
            break
        todas_as_paginas.extend(dados)
        logger.info(f"Página {pagina} - {len(dados)} registros recebidos")
        pagina += 1
    df = pd.DataFrame(todas_as_paginas)
    logger.info(f"Total de registros baixados: {len(df)}")
    return df

def salvar_bronze(df: pd.DataFrame, ano: int) -> str:
    os.makedirs(RAW_DATA_PATH, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    caminho = f"gastos_{ano}_{timestamp}.csv"

    df.to_csv(caminho, index=False, encoding="utf-8-sig")

    logger.info(f"Arquivo salvo em: {caminho}")
    return caminho

def executar_ingestion():

    logger.info("=== INÍCIO DA INGESTÃO ===")
    df = baixar_todos_os_dados(ANO_REFERENCIA)
    caminho = salvar_bronze(df, ANO_REFERENCIA)
    logger.info("=== FIM DA INGESTÃO ===")
    return caminho

if __name__ == "__main__":
    executar_ingestion()
    