# GASTOS_PUBLICOS

> Repositório de um pipeline de dados para análise de gastos públicos. Inclui ingestão, transformação, orquestração com Airflow, armazenamento em PostgreSQL e visualização via Streamlit.

---

## 📌 Autor

- **Pedro Seabra Dornellas**
  - LinkedIn: [pedro-seabra-dornellas](https://www.linkedin.com/in/pedro-seabra-dornellas)
  - GitHub: [CanineAcorn-3697](https://github.com/CanineAcorn-3697)

---

## 🧠 Visão geral do projeto

O repositório está organizado em camadas tradicionais de um data lake (`bronze`, `silver`, `gold`) e possui:

1. **Ingestão**: coleta de CSVs disponíveis em `data/bronze` por meio de `pipelines/ingestion/ingestion.py`.
2. **Transformação**: saneamento e modelagem dos dados via `pipelines/transformation/` e testes unitários.
3. **Orquestração**: DAG do Airflow (`orchestration/dags/gastos_pipeline.py`) que aciona as etapas de ingestão e transformação.
4. **DBT**: projetos dentro de `analytics/dbt` para modelagem SQL e geração de tabelas de dimensões e fatos.
5. **Visualização**: aplicativo Streamlit em `app/` que consome o PostgreSQL de destino e permite exploração interativa.

Contêineres Docker providenciam dois PostgreSQL (um para o Airflow e outro como base de dados do pipeline) e um Airflow completo com scheduler e webserver.

---

## 🛠️ Requisitos

- Linux (testado em Ubuntu 24.04)
- Python 3.12+ (usado no venv)
- [Docker](https://docs.docker.com/get-docker/) e [docker-compose](https://docs.docker.com/compose/install/)
- `virtualenv`/`venv` (embutido no Python)
- (Opcional) `dbt` instalado globalmente para rodar modelos: `pip install dbt-core dbt-postgres`
- `pytest` para executar os testes

---

## 🎯 Primeiros passos

### 1. Clonar o repositório

```bash
git clone https://github.com/CanineAcorn-3697/Gestao_de_Gastos_Publicos.git
cd Gastos_publicos
```

### 2. Criar e ativar os ambientes virtuais

O projeto possui dois ambientes sugeridos: um geral e outro específico para o Streamlit.

#### 2.1. Ambiente principal (`venv`)

```bash
python3 -m venv venv              # cria o virtualenv
source venv/bin/activate          # ativa
pip install --upgrade pip setuptools wheel
# (o arquivo requirements.txt na raiz está vazio; instale dependências conforme necessidade abaixo)
```

> **Dica:** você pode instalar pacotes extras usados nos pipelines, dbt ou Airflow aqui se for executar algo localmente.

#### 2.2. Ambiente do Streamlit (`venv_streamlit`)

```bash
python3 -m venv venv_streamlit
source venv_streamlit/bin/activate
pip install --upgrade pip setuptools wheel
pip install -r app/requirements.txt   # instala bibliotecas da interface
```

> Sempre active o venv correspondente antes de rodar comandos em cada área (pipeline, app, testes etc.).

---

### 3. Subir os contêineres com Docker Compose

```bash
docker compose pull        # baixa as imagens (apenas na primeira vez)
docker compose up -d       # inicia os serviços em background
```

Isso cria os serviços:

- `airflow-postgres` (porta `5433`) – banco do Airflow
- `gastos-postgres` (porta `5432`) – banco `gastos_publicos` usado pelo pipeline
- `airflow-init`, `airflow-webserver`, `airflow-scheduler` – componentes do Airflow

Verifique se estão saudáveis:

```bash
docker compose ps
```

> O script `docker/postgres/init.sql` é executado na primeira inicialização do banco do pipeline para criar tabelas ou usuários.

---

### 4. Acessar a interface do Airflow

Após os contêineres subirem, abra no navegador:

```
http://localhost:8080
```

Usuário padrão: `admin` / `admin` (configurado em `airflow-init`).

O DAG principal chama `executar_ingestao` e `executar_transformacao` e pode ser acionado manualmente ou agendado diariamente.

---

### 5. Rodar a aplicação Streamlit

1. Ative o ambiente do Streamlit conforme a seção 2.2.
2. Execute:

```bash
cd app
streamlit run app.py
```

3. Acesse `http://localhost:8501` para visualizar dashboards.

O app conecta-se automaticamente ao PostgreSQL de pipeline (`postgres://postgres:postgres@localhost:5432/gastos_publicos`).

---

### 6. Executar modelos DBT

Se você deseja executar a modelagem de dados:

```bash
cd analytics/dbt
# instale dbt se ainda não estiver no venv
dbt deps          # baixa dependências de pacotes
dbt seed          # carrega seeds (ex.: de_para_orgaos)
dbt run           # gera os modelos definidos em models/
dbt test          # executa testes definidos em tests/
```

Os artefatos serão salvos em `analytics/dbt/target`.

---

### 7. Testes unitários Python

No ambiente principal (`venv`):

```bash
pytest tests       # executa todos os testes do projeto
```

Atualmente há esqueleto de teste em `tests/`; adicione casos conforme evoluir.

---

## 📁 Organização de pastas

```
├── app/                  # interface Streamlit
├── analytics/dbt/        # projeto dbt para transformação
├── data/                 # raw (bronze), transformado (silver/gold)
├── orchestration/        # definições de DAG do Airflow
├── pipelines/            # código Python de ingestão/transformação
└── tests/                # testes automatizados
```

---

## 🔄 Execução manual das etapas

Caso queira rodar as etapas fora do Airflow:

```bash
# ingestão direta
source venv/bin/activate
python -m pipelines.ingestion.ingestion

# transformação direta
python -m pipelines.transformation.transformation
```

Os módulos expõem as funções `executar_ingestao()` e `executar_transformacao()` usadas no DAG.

---

## 📂 Dados

- `data/bronze` – arquivos CSV originais. Adicione novas coletas aqui e o pipeline tratará de alimentar as demais camadas.
- `data/silver`, `data/gold` – resultados de transformação (gerados pelos scripts ou pelo dbt).

---

## 🔗 Links Úteis

- [Perfil LinkedIn](https://www.linkedin.com/in/pedro-seabra-dornellas)
- [Repositório GitHub](https://github.com/CanineAcorn-3697)

---

## 📝 Observações finais

Este projeto serve de esqueleto para pipelines de dados no estilo ELT/ETL, com foco em transparência dos gastos públicos. Sinta-se à vontade para expandir, testar e adaptar para outros datasets.

Boa análise! 🚀
