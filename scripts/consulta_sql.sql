-- Remova a tabela anterior, se existir
DROP TABLE IF EXISTS linhas_onibus;

-- Tabela corrigida incluindo a coluna física da chave Kafka
CREATE TABLE linhas_onibus (
    kafka_key STRING,

    c STRING,
    cl INT,
    sl INT,
    lt0 STRING,
    lt1 STRING,
    qv INT,
    p INT,
    a BOOLEAN,

    ta BIGINT,
    py DOUBLE,
    px DOUBLE,

    ta_ts AS TO_TIMESTAMP_LTZ(ta, 3),

    WATERMARK FOR ta_ts AS ta_ts - INTERVAL '60' SECOND
) WITH (
    'connector' = 'kafka',
    'topic' = 'linhas_onibus',
    'properties.bootstrap.servers' = 'kafka:29092',
    'properties.group.id' = 'flink-sptrans-consumer',
    'scan.startup.mode' = 'earliest-offset',

    'key.format' = 'raw',
    'key.fields' = 'kafka_key',

    'value.format' = 'json',
    'value.fields-include' = 'EXCEPT_KEY',
    'value.json.ignore-parse-errors' = 'true'
);

-- Teste
SELECT *
FROM linhas_onibus
where ;


-- 1) Cria uma visão com a posição anterior de cada ônibus usando LAG()
CREATE TEMPORARY VIEW velocidades_onibus AS
SELECT
    p AS bus_id,
    c AS linha,
    ta_ts,
    py AS latitude,
    px AS longitude,

    -- Posição anterior do mesmo ônibus
    LAG(py) OVER (
        PARTITION BY p
        ORDER BY ta_ts
    ) AS prev_latitude,

    LAG(px) OVER (
        PARTITION BY p
        ORDER BY ta_ts
    ) AS prev_longitude,

    LAG(ta_ts) OVER (
        PARTITION BY p
        ORDER BY ta_ts
    ) AS prev_ta

FROM linhas_onibus;


-- 2) Calcula a velocidade em km/h usando a fórmula de Haversine
SELECT
    bus_id,
    linha,
    ta_ts,
    latitude,
    longitude,

    -- Distância em quilômetros
    6371.0 * 2 * ASIN(
        SQRT(
            POWER(
                SIN(RADIANS(latitude - prev_latitude) / 2),
                2
            )
            +
            COS(RADIANS(prev_latitude))
            *
            COS(RADIANS(latitude))
            *
            POWER(
                SIN(RADIANS(longitude - prev_longitude) / 2),
                2
            )
        )
    ) AS distancia_km,

    -- Tempo decorrido em segundos
    TIMESTAMPDIFF(
        SECOND,
        prev_ta,
        ta_ts
    ) AS delta_segundos,

    -- Velocidade em km/h
    CASE
        WHEN prev_ta IS NULL THEN NULL
        WHEN TIMESTAMPDIFF(SECOND, prev_ta, ta_ts) <= 0 THEN NULL
        ELSE
            (
                6371.0 * 2 * ASIN(
                    SQRT(
                        POWER(
                            SIN(RADIANS(latitude - prev_latitude) / 2),
                            2
                        )
                        +
                        COS(RADIANS(prev_latitude))
                        *
                        COS(RADIANS(latitude))
                        *
                        POWER(
                            SIN(RADIANS(longitude - prev_longitude) / 2),
                            2
                        )
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
    END AS velocidade_kmh

FROM velocidades_onibus
WHERE prev_ta IS NOT NULL;