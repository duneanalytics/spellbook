{{
    config(
        schema = 'tokens'
        ,alias = 'erc20'
        ,tags = ['static']
        ,materialized = 'table'
        ,post_hook='{{ expose_spells(\'["arbitrum","avalanche_c","base","bnb","celo","ethereum","fantom","gnosis","goerli","optimism","polygon","scroll","zksync","zora"]\',
                        "sector",
                        "tokens",
                        \'["0xManny","hildobby","soispoke","dot2dotseurat","mtitus6","wuligy","lgingerich","0xRob","jeff-dude"]\') }}'
    )
}}

{% set models = {
    'tokens_arbitrum': {'blockchain': 'arbitrum', 'model': ref('tokens_arbitrum_erc20')},
    'tokens_avalanche_c': {'blockchain': 'avalanche_c', 'model': ref('tokens_avalanche_c_erc20')},
    'tokens_base': {'blockchain': 'base', 'model': ref('tokens_base_erc20')},
    'tokens_bnb': {'blockchain': 'bnb', 'model': ref('tokens_bnb_bep20')},
    'tokens_celo': {'blockchain': 'celo', 'model': ref('tokens_celo_erc20')},
    'tokens_ethereum': {'blockchain': 'ethereum', 'model': ref('tokens_ethereum_erc20')},
    'tokens_fantom': {'blockchain': 'fantom', 'model': ref('tokens_fantom_erc20')},
    'tokens_gnosis': {'blockchain': 'gnosis', 'model': ref('tokens_gnosis_erc20')},
    'tokens_goerli': {'blockchain': 'goerli', 'model': ref('tokens_goerli_erc20')},
    'tokens_optimism': {'blockchain': 'optimism', 'model': ref('tokens_optimism_erc20')},
    'tokens_polygon': {'blockchain': 'polygon', 'model': ref('tokens_polygon_erc20')},
    'tokens_scroll': {'blockchain': 'scroll', 'model': ref('tokens_scroll_erc20')},
    'tokens_zksync': {'blockchain': 'zksync', 'model': ref('tokens_zksync_erc20')},
    'tokens_zora': {'blockchain': 'zora', 'model': ref('tokens_zora_erc20')}
} %}

with manual_tokens as (
    SELECT *
    FROM
    (
        {% for key, value in models.items() %}
        SELECT
            '{{ value.blockchain }}' as blockchain
            , contract_address
            , symbol
            , decimals
        FROM
            {{ value.model }}
        {% if value.blockchain == 'optimism' %}
        WHERE
            symbol IS NOT NULL --This can be removed if/when all other chains show all ERC20 tokens, rather than only mapped ones
        {% endif %}
        {% if not loop.last %}
        UNION ALL
        {% endif %}
        {% endfor %}
    )
)
select
    *
from
    manual_tokens