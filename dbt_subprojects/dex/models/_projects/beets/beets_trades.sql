{{ config(
        schema = 'beets',
        alias = 'trades',
        materialized = 'view',
        post_hook='{{ expose_spells(blockchains = \'["sonic"]\',
                                      spell_type = "project", 
                                      spell_name = "beets", 
                                      contributors = \'["franz"]\') }}'
        )
}}

{% set beets_models = [
    ref('beets_v2_sonic_trades'),
    ref('beets_v3_sonic_trades'),
] %}


SELECT *
FROM (
    {% for model in beets_models %}
        SELECT 
                blockchain,
                project,
                version,
                block_date,
                block_number,
                block_month,
                block_time,
                token_bought_symbol,
                token_sold_symbol,
                token_pair,
                token_bought_amount,
                token_sold_amount,
                token_bought_amount_raw,
                token_sold_amount_raw,
                amount_usd,
                token_bought_address,
                token_sold_address,
                taker,
                maker,
                project_contract_address,
                pool_id,
                swap_fee,
                pool_symbol,
                pool_type,
                tx_hash,
                tx_from,
                tx_to,
                evt_index
        FROM {{ model }}
        {% if not loop.last %}
        UNION ALL
        {% endif %}
        {% endfor %}
        )