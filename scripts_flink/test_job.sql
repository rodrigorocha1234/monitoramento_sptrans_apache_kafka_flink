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
    WATERMARK FOR ta_ts AS ta_ts - INTERVAL '10' SECOND
) WITH (
    'connector' = 'kafka',
    'topic' = 'posicoes_sptrans',
    'properties.bootstrap.servers' = 'kafka:29092',
    'properties.group.id' = 'flink-bus-monitor-test',
    'scan.startup.mode' = 'latest-offset',
    'format' = 'avro-confluent',
    'avro-confluent.url' = 'http://schema-registry:8081'
);

CREATE TABLE test_velocidades_sink (
    bus_id BIGINT,
    ta_ts TIMESTAMP(3),
    prev_ta TIMESTAMP(3),
    latitude DOUBLE,
    prev_latitude DOUBLE,
    velocidade_kmh DOUBLE
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:postgresql://timescaledb:5432/sptrans',
    'table-name' = 'test_velocidades',
    'username' = 'admin',
    'password' = 'admin'
);

INSERT INTO test_velocidades_sink
SELECT
    bus_id,
    ta_ts,
    prev_ta,
    latitude,
    prev_latitude,
    CASE
        WHEN prev_ta IS NULL THEN NULL
        WHEN TIMESTAMPDIFF(SECOND, prev_ta, ta_ts) <= 0 THEN NULL
        ELSE 1.0
    END AS velocidade_kmh
FROM (
    SELECT
        p AS bus_id,
        ta_ts,
        LAG(ta_ts) OVER (PARTITION BY p ORDER BY ta_ts) AS prev_ta,
        py AS latitude,
        LAG(py) OVER (PARTITION BY p ORDER BY ta_ts) AS prev_latitude
    FROM linhas_onibus
);
