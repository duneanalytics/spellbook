{{ config(
        alias = 'signatures',
        schema = 'abi',
        partition_by = ['created_at_month'],
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['created_at', 'unique_signature_id'],
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.created_at')],
        post_hook='{{ expose_spells(\'["ethereum","bnb","avalanche_c","optimism","arbitrum","gnosis","polygon","fantom","celo","base"]\',
                        "sector",
                        "abi",
                        \'["ilemi","tomfutago"]\') }}'
        )
}}

{% set chains = [
    source('ethereum', 'signatures')
    ,source('optimism', 'signatures')
    ,source('arbitrum', 'signatures')
    ,source('avalanche_c', 'signatures')
    ,source('polygon', 'signatures')
    ,source('bnb', 'signatures')
    ,source('gnosis', 'signatures')
    ,source('fantom', 'signatures')
    ,source('celo', 'signatures')
    ,source('base', 'signatures')
    ,source('zksync', 'signatures')
    ,source('scroll', 'signatures')
] %}

WITH
    signatures as (
        {% for chain_source in chains %}

            SELECT
                abi,
                created_at,
                coalesce(try(from_hex(id)), cast(id as varbinary)) as id,
                signature,
                type,
                concat(cast(id as varchar), signature, type) as unique_signature_id
            FROM {{ chain_source }}

            {% if is_incremental() %}
            WHERE {{incremental_predicate('created_at')}}
            {% endif %}

            {% if not loop.last %}
            union all
            {% endif %}

        {% endfor %}
    )

    SELECT
    *
    FROM (
        SELECT
            id
            , signature
            , abi
            , type
            , created_at
            , date(date_trunc('month', created_at)) as created_at_month
            , unique_signature_id
            , row_number() over (partition by unique_signature_id order by created_at desc) recency
        FROM signatures
    ) a
    WHERE recency = 1
    {% if is_incremental() %}
    AND NOT EXISTS (SELECT 1 FROM {{ this }} WHERE unique_signature_id = a.unique_signature_id)
    {% endif %}
