# app/pages/1_gastos_orgao.py
import streamlit as st
import plotly.express as px
import sys
import os

# Importa a função de conexão do app.py principal
sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from app import carregar_dados

st.title("Gastos por Órgão")

df = carregar_dados()

# Filtro de ano na barra lateral
anos = sorted(df['ano'].unique(), reverse=True)
ano_selecionado = st.sidebar.selectbox("Selecione o Ano", anos)

# Filtro de top N
top_n = st.sidebar.slider("Top N órgãos", min_value=5, max_value=30, value=10)

# Filtra e agrega
df_filtrado = (
    df[df['ano'] == ano_selecionado]
    .groupby('orgao')['total_gasto']
    .sum()
    .sort_values(ascending=False)
    .head(top_n)
    .reset_index()
)

# Gráfico
fig = px.bar(
    df_filtrado,
    x='total_gasto',
    y='orgao',
    orientation='h',
    title=f"Top {top_n} órgãos que mais gastaram em {ano_selecionado}",
    labels={'total_gasto': 'Total Gasto (R$)', 'orgao': 'Órgão'},
    color='total_gasto',
    color_continuous_scale='Blues'
)

fig.update_layout(yaxis={'categoryorder': 'total ascending'})
st.plotly_chart(fig, use_container_width=True)

# Tabela abaixo do gráfico
st.dataframe(
    df_filtrado.rename(columns={
        'orgao': 'Órgão',
        'total_gasto': 'Total Gasto (R$)'
    }),
    use_container_width=True
)