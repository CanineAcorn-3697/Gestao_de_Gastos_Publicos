# app/app.py
import streamlit as st
import pandas as pd
from sqlalchemy import create_engine
import os

st.set_page_config(
    page_title="Gastos Públicos",
    page_icon="💰",
    layout="wide"
)

@st.cache_resource
def get_engine():
    host     = os.getenv("POSTGRES_HOST", "localhost")
    port     = os.getenv("POSTGRES_PORT", "5432")
    db       = os.getenv("POSTGRES_DB", "gastos_publicos")
    user     = os.getenv("POSTGRES_USER", "postgres")
    password = os.getenv("POSTGRES_PASSWORD", "postgress")
    return create_engine(
        f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}"
    )

@st.cache_data(ttl=3600)
def carregar_dados():
    engine = get_engine()
    with engine.connect() as conn:
        return pd.read_sql("SELECT * FROM gold.fato_gastos_por_orgao_ano", conn)

# Página inicial
st.title("💰 Dashboard de Gastos Públicos")
st.markdown("---")

# Métricas gerais no topo
df = carregar_dados()

col1, col2, col3 = st.columns(3)
with col1:
    st.metric(
        label="Total Gasto",
        value=f"R$ {df['liquidado'].sum():,.2f}"
    )
with col2:
    st.metric(
        label="Órgãos",
        value=df['orgao'].nunique()
    )
with col3:
    st.metric(
        label="Anos com Dados",
        value=df['ano'].nunique()
    )

st.markdown("---")
st.markdown("Navegue pelas páginas no menu à esquerda.")