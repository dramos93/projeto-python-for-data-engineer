# stage 1: imagem oficial do uv (contém o binário /uv)
FROM ghcr.io/astral-sh/uv:latest AS uv-builder

# stage 2: imagem de runtime (Python)
FROM python:3.12-slim

# copia o binário uv do stage anterior
COPY --from=uv-builder /uv /usr/local/bin/uv
RUN chmod +x /usr/local/bin/uv

# Define o diretório de trabalho dentro do contêiner.
WORKDIR /app

# instala dependências do SO necessárias (se precisar)
RUN apt-get update && apt-get install -y curl ca-certificates --no-install-recommends && rm -rf /var/lib/apt/lists/*

# instala uv de forma não-gerenciada em /usr/local/uv
RUN curl -LsSf https://astral.sh/uv/install.sh | env UV_UNMANAGED_INSTALL="/usr/local/uv" sh



# copy lockfiles primeiro para maximizar cache
COPY pyproject.toml uv.lock* /src/ /config/
RUN /usr/local/uv/bin/uv sync --no-editable

COPY . /src

RUN uv pip install -r pyproject.toml

# Isso garante que os diretórios existam para que os volumes montados a partir do host funcionem corretamente.
RUN mkdir -p data/raw data/silver data/gold
RUN mkdir -p logs

# Define o comando padrão para executar a aplicação quando o contêiner for iniciado.
CMD ["python", "src/main.py"]