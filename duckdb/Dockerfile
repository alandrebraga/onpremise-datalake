FROM debian:bullseye

WORKDIR /DW

ARG DUCKDB_VERSION

RUN apt-get update && apt-get install -y \
    wget unzip \
    && rm -rf /var/lib/apt/lists/*

RUN wget https://github.com/duckdb/duckdb/releases/download/v1.0.0/duckdb_cli-linux-amd64.zip \
    && unzip duckdb_cli-linux-amd64.zip -d /usr/local/bin \
    && duckdb -c "INSTALL HTTPFS;" \
    && rm duckdb_cli-linux-amd64.zip

CMD [ "duckdb", "--version"]