FROM flink:2.2.0

USER root

# Instala Python e dependências de compilação
RUN apt-get update -y && \
    apt-get install -y --no-install-recommends \
        python3 \
        python3-pip \
        python3-dev \
        python3-venv \
        build-essential \
        gcc \
        g++ \
        libffi-dev \
        libssl-dev && \
    rm -rf /var/lib/apt/lists/*

# Cria link simbólico para "python"
RUN ln -sf /usr/bin/python3 /usr/bin/python

# Cria ambiente virtual para evitar erro PEP 668 (externally-managed-environment)
RUN python3 -m venv /opt/venv

# Adiciona o venv ao PATH
ENV PATH="/opt/venv/bin:${PATH}"

# Atualiza ferramentas do pip dentro do ambiente virtual
RUN pip install --no-cache-dir --upgrade pip setuptools wheel

# Instala PyFlink
RUN pip install --no-cache-dir apache-flink==2.2.0

RUN wget -O /opt/flink/lib/flink-sql-connector-kafka-4.0.1-2.0.jar \
    https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/4.0.1-2.0/flink-sql-connector-kafka-4.0.1-2.0.jar

USER flink