---
layout: project
title: "Monitoramento de ônibus em tempo real usando apache kafka e apache flink"
date: 2026-07-09
image: /assets/img/posts/monitoramento_sptrans_flink_kafka/thumb.png
description: "Acampanhar o monitoramento das posições dos ônibus em tempo real"
tags: ['python', 'docker', 'grafana', 'apache_kafka','apache flink', 'engenharia de dados', 'processamento de dados em tempo real', 'big data', 'monitoramento']
---

## Introdução 
O objetivo desse projeto é construir e propor uma arquitetura de monitoramento de ônibus em **tempo real**, para visualizar as posições dos ônibus e calculo da **velocidade em tempo** real, usando **apache kafka** e **apache flink**.



##  Tecnologias Utilizadas

- 🐍 **Python** — Processamento de dados, integração e desenvolvimento da aplicação.
- 📨 **Apache Kafka** — Streaming de eventos e mensageria em tempo real.
- 📑 **Schema Registry** — Gerenciamento e versionamento dos esquemas das mensagens (Avro).
- ⚡ **Apache Flink SQL** — Processamento contínuo de streams utilizando SQL.
- 🗄️ **TimescaleDB** — Banco de dados de séries temporais otimizado para armazenar, consultar e analisar dados de telemetria e eventos em tempo real.
- 🐳 **Docker** — Containerização e orquestração do ambiente de desenvolvimento.
- 📊 **Grafana** — Criação de dashboards e visualização de métricas em tempo real.

## Arquitetura da Solução

### Diagrama de classe
A figura abaixo mostra a organização do **diagrama de classe** , toda a estrutura do pipeline foi pensada para ser um **código flexível**, ou seja, permitir trocar parte do código **sem prejudicar o funcionamento do pipeline.**

![Exemplo de imagem](https://github.com/rodrigorocha1234/monitoramento_sptrans_apache_kafka_flink/blob/master/fig/diagrama_classe.png?raw=true)

###  Estrutura do apache kafka

As posições dos ônibus que serão obtidas do serviço da sptrans, serão enviadas ao apache kafka, para um tópico chamado posicoes_sptrans em **intervalo de 15 segundos**, para o cálculo das velocidades e exibições das posições dos ônibus em **tempo real**.

Para o tópico posicoes_sptrans, foram criados **4 partições**, usando o código do prefixo dos ônibus como chave no envio das mensagens para o kafka.

### Estrutura do apache flink

A consulta abaixo mostra a exibição de todas as linhas de ônibus consumindo do tópico, “posicoes_sptrans”, que serão usadas para a exibição das métricas seguintes.

```sql
CREATE TABLE linhas_onibus (
    c STRING,
    cl INT,
    sl INT,
    lt0 STRING,
    lt1 STRING,
    qv INT,
    p INT,
    a BOOLEAN,
    ta BIGINT,
    ta_tempo STRING,
    py DOUBLE,
    px DOUBLE,
    ta_ts AS TO_TIMESTAMP(ta_tempo),
    kafka_time TIMESTAMP(3) METADATA FROM 'timestamp',
    WATERMARK FOR kafka_time AS
        kafka_time - INTERVAL '2' SECOND
)
WITH (
    'connector' = 'kafka',
    'topic' = 'posicoes_sptrans',
    'properties.bootstrap.servers' = 'kafka:29092',
    'properties.group.id' = 'flink-bus-monitor',
    'scan.startup.mode' = 'latest-offset',
    'format' = 'avro-confluent',
    'avro-confluent.url' = 'http://schema-registry:8081'
);
```

### Velocidade

A consulta abaixo, mostra o cálculo da velocidade média, em **intervalo de 3 minutos**, com deslocamento de **1 minuto**.

```sql

CREATE VIEW velocidade_media_3min AS
SELECT
    bus_id,
    linha,
    window_start,
    window_end,
    CAST(
        CASE 
            WHEN SUM(tempo_horas) > 0 THEN ROUND(SUM(distancia_km) / SUM(tempo_horas), 2)
            ELSE 0.0
        END AS DOUBLE
    ) AS velocidade_media
FROM TABLE(
    HOP(
        TABLE velocidades_evento,
        DESCRIPTOR(kafka_time),
        INTERVAL '1' MINUTE,
        INTERVAL '3' MINUTES
    )
)
WHERE velocidade_kmh IS NOT NULL 
  AND velocidade_kmh < 120.0
GROUP BY
    bus_id,
    linha,
    window_start,
    window_end;

CREATE TABLE velocidade_media_sink (

    bus_id BIGINT,
    linha STRING,

    window_start TIMESTAMP(3),
    window_end TIMESTAMP(3),

    velocidade_media DOUBLE,

    PRIMARY KEY (
        bus_id,
        linha,
        window_start
    ) NOT ENFORCED

)
WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:postgresql://timescaledb:5432/sptrans',
    'table-name' = 'velocidade_media_3min',
    'username' = 'admin',
    'password' = 'admin'
);

INSERT INTO velocidade_media_sink
SELECT
    CAST(bus_id AS BIGINT),
    linha,
    window_start,
    window_end,
    velocidade_media
FROM velocidade_media_3min;
```

### Diagrama de arquitetura

![Exemplo de imagem](https://github.com/rodrigorocha1234/monitoramento_sptrans_apache_kafka_flink/blob/master/fig/diagrama_arquitetura_integracao.png?raw=true)

A figura acima, mostra a arquitetura do docker construída:
**Camada 1:**  Aqui, será organizado a comunicação para o dashboard. Irá receber os dados tradados e será feita a exibição no grafana.
**Camada 2:** Aqui, é o processamento do apache flink, para o cálculo da velocidade média e a exibição da posição atual do ônibus.
**Camada 3:** Aqui, é a comunicação do serviço do apache kafka, que será enviado ao apache flink e a definição do schema registry do processamento de dados.

## Demonstração do projeto

Você pode ver o projeto em ação no seguinte link:

# Demonstração do projeto
[![Assistir ao vídeo de demonstração do projeto](https://img.shields.io/badge/🎬%20Assistir%20ao%20vídeo-FF0000?style=for-the-badge&logo=youtube&logoColor=white)](https://youtu.be/pmPsvDJvQTg)






## Repositório do projeto 
[Link do repositório](https://github.com/rodrigorocha1234/monitoramento_sptrans_apache_kafka_flink.git)
