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

CREATE TEMPORARY VIEW velocidades_evento AS
SELECT
    bus_id,
    linha,
    ta_ts,
    kafka_time,

    CASE
        WHEN prev_ta IS NULL THEN NULL
        WHEN TIMESTAMPDIFF(SECOND, prev_ta, ta_ts) <= 0 THEN NULL
        ELSE
            (
                6371.0 * 2 * ASIN(
                    SQRT(
                        POWER(SIN(RADIANS(latitude - prev_latitude) / 2), 2)
                        +
                        COS(RADIANS(prev_latitude))
                        *
                        COS(RADIANS(latitude))
                        *
                        POWER(SIN(RADIANS(longitude - prev_longitude) / 2), 2)
                    )
                )
            )
            /
            (
                TIMESTAMPDIFF(
                    SECOND,
                    prev_ta,
                    ta_ts
                ) / 3600.0
            )
    END AS velocidade_kmh,

    CASE
        WHEN prev_ta IS NULL THEN NULL
        WHEN TIMESTAMPDIFF(SECOND, prev_ta, ta_ts) <= 0 THEN NULL
        ELSE
            (
                6371.0 * 2 * ASIN(
                    SQRT(
                        POWER(SIN(RADIANS(latitude - prev_latitude) / 2), 2)
                        +
                        COS(RADIANS(prev_latitude))
                        *
                        COS(RADIANS(latitude))
                        *
                        POWER(SIN(RADIANS(longitude - prev_longitude) / 2), 2)
                    )
                )
            )
    END AS distancia_km,

    CASE
        WHEN prev_ta IS NULL THEN NULL
        WHEN TIMESTAMPDIFF(SECOND, prev_ta, ta_ts) <= 0 THEN NULL
        ELSE TIMESTAMPDIFF(SECOND, prev_ta, ta_ts) / 3600.0
    END AS tempo_horas

    FROM (
        SELECT
            p AS bus_id,
            c AS linha,
            py AS latitude,
            px AS longitude,
            ta_ts,
            kafka_time,

            LAG(py) OVER (
                PARTITION BY p
                ORDER BY kafka_time
            ) AS prev_latitude,

            LAG(px) OVER (
                PARTITION BY p
                ORDER BY kafka_time
            ) AS prev_longitude,

            LAG(ta_ts) OVER (
                PARTITION BY p
                ORDER BY kafka_time
            ) AS prev_ta

        FROM linhas_onibus
    );

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

---- Relação Linhas
CREATE VIEW linhas_catalogo AS
SELECT DISTINCT
    c,
    lt0,
    lt1
FROM linhas_onibus;


CREATE TABLE linhas_catalogo_sink (

    codigo_linha STRING,
    origem STRING,
    destino STRING,

    PRIMARY KEY (codigo_linha) NOT ENFORCED

)
WITH (

    'connector' = 'jdbc',

    'url' = 'jdbc:postgresql://timescaledb:5432/sptrans',

    'table-name' = 'linhas_catalogo',

    'username' = 'admin',

    'password' = 'admin'

);

INSERT INTO linhas_catalogo_sink
SELECT DISTINCT
    c AS codigo_linha,
    lt0 AS origem,
    lt1 AS destino
FROM linhas_onibus
WHERE c IS NOT NULL;

----- posião
CREATE VIEW ultima_data_por_onibus AS
 SELECT
     p,
     MAX(ta_ts) AS max_ts
 FROM linhas_onibus
 GROUP BY p;

  CREATE VIEW ultima_posicao_onibus AS
 SELECT
     l.p AS bus_id,
     l.c AS linha,
     l.cl AS codigo_linha,
     l.sl AS sentido,
     l.lt0 AS origem,
     l.lt1 AS destino,
     l.py AS latitude,
     l.px AS longitude,
     l.a AS acessivel,
     l.qv AS qtd_veiculos_linha,
     l.ta_ts AS timestamp_evento
 FROM linhas_onibus l
 JOIN ultima_data_por_onibus u
 ON l.p = u.p
 AND l.ta_ts = u.max_ts;

  CREATE TABLE posicao_atual_sink (

    bus_id BIGINT,

    linha STRING,

    codigo_linha INT,

    sentido INT,

    origem STRING,

    destino STRING,

    latitude DOUBLE,

    longitude DOUBLE,

    acessivel BOOLEAN,

    qtd_veiculos_linha INT,

    timestamp_evento TIMESTAMP(3),

    PRIMARY KEY (bus_id) NOT ENFORCED

)
WITH (

    'connector' = 'jdbc',

    'url' = 'jdbc:postgresql://timescaledb:5432/sptrans',

    'table-name' = 'posicao_atual_onibus',

    'username' = 'admin',

    'password' = 'admin'

);

INSERT INTO posicao_atual_sink
SELECT
    bus_id,
    linha,
    codigo_linha,
    sentido,
    origem,
    destino,
    latitude,
    longitude,
    acessivel,
    qtd_veiculos_linha,
    timestamp_evento
FROM ultima_posicao_onibus;
