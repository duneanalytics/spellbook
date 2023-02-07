{{ config(
        alias ='trades',
        post_hook='{{ expose_spells(\'["ethereum","arbitrum", "optimism", "polygon"]\',
                                "project",
                                "uniswap",
                                \'["jeff-dude","mtitus6", "Henrystats]\') }}'
        )
}}

-- defining the different uniswap models to be used in the union
-- each model is defined in the respective chain's folder and contains all versions of the dex
{% set uniswap_models = [
'uniswap_ethereum_trades'
,'uniswap_optimism_trades' 
,'uniswap_arbitrum_trades'
,'uniswap_polygon_trades'
] %}

/*
    This query is a union of all the uniswap models defined in the uniswap_models list
    The union is used to combine all the different versions of the dex into one table

    The query is structured as follows:
    1. Loop through each dex model in the uniswap_models list
    2. For each dex model, select all the columns from the model
    3. If it is not the last iteration of the loop, add a UNION ALL to the query
    4. If it is the last iteration of the loop, do not add a UNION ALL to the query
    5. End the loop

    The result is a single table with all the uniswap trades from all the different versions of the dex across all chains.

    The query is structured this way to allow for easy addition of new dex versions and chains.
*/

SELECT *
FROM (
    {% for dex_model in uniswap_models %} -- loop through each dex model
    SELECT
        blockchain,
        project,
        version,
        block_date,
        block_time,
        token_bought_symbol,
        token_sold_symbol,
        token_pair,
        token_bought_amount,
        token_sold_amount,
        CAST(token_bought_amount_raw AS DECIMAL(38,0)) AS token_bought_amount_raw, -- current DuneSQL workaround
        CAST(token_sold_amount_raw AS DECIMAL(38,0)) AS token_sold_amount_raw, -- current DuneSQL workaround
        token_bought_address,
        token_sold_address,
        taker,
        maker,
        project_contract_address,
        tx_hash,
        tx_from,
        tx_to,
        trace_address,
        evt_index
    FROM {{ ref(dex_model) }}
    {% if not loop.last %} --if not loop last, union. loop last is a jinja2 variable that is true if it is the last iteration of the loop
    UNION ALL
    {% endif %} -- end if
    {% endfor %} -- end loop
)
;