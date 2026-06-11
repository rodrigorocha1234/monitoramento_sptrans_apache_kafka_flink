---Velocidade média de cada ônibus

truncate table velocidade_media_3min;

CREATE TABLE velocidade_media_3min (
    bus_id BIGINT,
    linha VARCHAR(50),

    window_start TIMESTAMP,
    window_end TIMESTAMP,

    velocidade_media NUMERIC(10,2),

    PRIMARY KEY (
        bus_id,
        linha,
        window_start
    )
);

SELECT add_retention_policy(
    'velocidade_media_3min',
    INTERVAL '180 days'
);

ALTER TABLE velocidade_media_3min
SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'linha,bus_id'
);

SELECT add_compression_policy(
    'velocidade_media_3min',
    INTERVAL '7 days'
);




SELECT create_hypertable(
    'velocidade_media_3min',
    'window_start',
    if_not_exists => TRUE
);

select * 
from velocidade_media_3min vmm
where vmm.velocidade_media  is not null
order by vmm.window_end desc;

---- Lista linhas

DROP TABLE IF EXISTS linhas_catalogo;

CREATE TABLE linhas_catalogo (
    codigo_linha VARCHAR(50) PRIMARY KEY,
    origem VARCHAR(255),
    destino VARCHAR(255),
    ultima_atualizacao TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);


select *
from linhas_catalogo;

--- última posição

DROP TABLE IF EXISTS posicao_atual_onibus;

CREATE TABLE posicao_atual_onibus (

    bus_id BIGINT PRIMARY KEY,

    linha VARCHAR(50),

    codigo_linha INTEGER,

    sentido INTEGER,

    origem TEXT,

    destino TEXT,

    latitude DOUBLE PRECISION,

    longitude DOUBLE PRECISION,

    acessivel BOOLEAN,

    qtd_veiculos_linha INTEGER,

    timestamp_evento TIMESTAMP

);

select count(*)
from posicao_atual_onibus;



