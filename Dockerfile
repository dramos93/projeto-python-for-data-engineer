# Use a versão oficial do Airflow como base.
FROM apache/airflow:2.9.2

# Permaneça como 'root' para instalar dependências do sistema.
USER root

# Instala o uv
RUN apt-get update && \
    apt-get install -y curl && \
    curl -LsSf https://astral.sh/uv/install.sh | sh && \
    apt-get purge -y --auto-remove curl && \
    rm -rf /var/lib/apt/lists/*

# Copie os arquivos de dependência e o código-fonte para o diretório home do Airflow.
WORKDIR /opt/airflow
COPY pyproject.toml uv.lock ./
COPY ./src ./src
COPY ./config ./config
COPY .env .env

# Mude a propriedade dos arquivos para o usuário 'airflow' para que ele possa usá-los.
RUN chown -R airflow:root /opt/airflow

# Mude para o usuário 'airflow' ANTES de instalar pacotes Python.
USER airflow

# Instale as dependências. Sem a flag '--system', uv usará o ambiente
# virtual do Airflow (/opt/airflow/.venv), que é o correto.
RUN uv pip install -r pyproject.toml

# Configure o PYTHONPATH.
ENV PYTHONPATH="/opt/airflow/src:${PYTHONPATH}"

# O usuário 'airflow' já está definido para a execução dos contêineres.

EXPOSE 8501
