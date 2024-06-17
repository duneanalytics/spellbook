{{
    config(
        schema = 'tokens'
        , alias = 'erc20_stablecoins'
        , materialized = 'table'
        , post_hook='{{ expose_spells(blockchains = \'["arbitrum","avalanche_c","base","blast","bnb","celo","ethereum","fantom","gnosis","optimism","polygon","scroll","zkevm","zksync","linea", "tron"]\',
                        spell_type = "sector",
                        spell_name = "tokens",
                        contributors = \'["thetroyharris", "gentrexha", "dot2dotseurat", "msilb7", "lgingerich", "Henrystats"]\') }}'
    )
}}

{% set static_models = {
    'tokens_arbitrum': {'blockchain': 'arbitrum', 'model': ref('tokens_arbitrum_erc20_stablecoins')}
    ,'tokens_avalanche_c': {'blockchain': 'avalanche_c', 'model': ref('tokens_avalanche_c_erc20_stablecoins')}
    ,'tokens_base': {'blockchain': 'base', 'model': ref('tokens_base_erc20_stablecoins')}
    ,'tokens_blast': {'blockchain': 'blast', 'model': ref('tokens_blast_erc20_stablecoins')}
    ,'tokens_bnb': {'blockchain': 'bnb', 'model': ref('tokens_bnb_bep20_stablecoins')}
    ,'tokens_celo': {'blockchain': 'celo', 'model': ref('tokens_celo_erc20_stablecoins')}
    ,'tokens_ethereum': {'blockchain': 'ethereum', 'model': ref('tokens_ethereum_erc20_stablecoins')}
    ,'tokens_fantom': {'blockchain': 'fantom', 'model': ref('tokens_fantom_erc20_stablecoins')}
    ,'tokens_gnosis': {'blockchain': 'gnosis', 'model': ref('tokens_gnosis_erc20_stablecoins')}
    ,'tokens_optimism': {'blockchain': 'optimism', 'model': ref('tokens_optimism_erc20_stablecoins')}
    ,'tokens_polygon': {'blockchain': 'polygon', 'model': ref('tokens_polygon_erc20_stablecoins')}
    ,'tokens_scroll': {'blockchain': 'scroll', 'model': ref('tokens_scroll_erc20_stablecoins')}
    ,'tokens_zkevm': {'blockchain': 'zkevm', 'model': ref('tokens_zkevm_erc20_stablecoins')}
    ,'tokens_zksync': {'blockchain': 'zksync', 'model': ref('tokens_zksync_erc20_stablecoins')}
    ,'tokens_linea': {'blockchain': 'linea', 'model': ref('tokens_linea_erc20_stablecoins')}
    ,'tokens_tron': {'blockchain': 'tron', 'model': ref('tokens_tron_trc20_stablecoins')}
} %}

SELECT *
FROM (
   {% for key, value in static_models.items() %}
    SELECT
        '{{ value.blockchain }}' AS blockchain
        , contract_address
        , symbol
        , decimals
        , name
    FROM
        {{ value.model }}
    {% if not loop.last %}
    union all
    {% endif %}
    {% endfor %}
)
