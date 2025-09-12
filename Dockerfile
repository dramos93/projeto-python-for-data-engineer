# stage 2: imagem de runtime (Python)
FROM python:3.13-slim-bookworm

# copia o binário uv do stage anterior
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/
# RUN chmod +x /usr/local/bin/uv

ADD . /src

# Define o diretório de trabalho dentro do contêiner.
WORKDIR /src

# instala dependências do SO necessárias (se precisar)
RUN apt-get update && apt-get install -y curl ca-certificates --no-install-recommends && rm -rf /var/lib/apt/lists/*

# instala uv de forma não-gerenciada em /usr/local/uv
# RUN curl -LsSf https://astral.sh/uv/install.sh | env UV_UNMANAGED_INSTALL="/usr/local/uv" sh

RUN uv sync --locker

CMD ["sh"]