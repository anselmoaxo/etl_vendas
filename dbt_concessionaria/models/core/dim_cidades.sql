{{ config(materialized='table') }}

    SELECT
        id_cidades as cidades_id
        ,nome_cidade
        ,id_estados as estados_id
        ,data_inclusao
        ,data_atualizacao 
    FROM {{ ref('stg_cidades') }}
