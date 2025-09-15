{{ config(
    schema = 'gas',
    alias = 'fees',
    post_hook='{{ expose_spells(\'[
                                    "abstract"
                                    , "apechain"
                                    , "arbitrum"
                                    , "avalanche_c"
                                    , "b3"
                                    , "base"
                                    , "berachain"
                                    , "blast"
                                    , "bnb"
                                    , "bob"
                                    , "boba"
                                    , "celo"
                                    , "corn"
                                    , "degen"
                                    , "ethereum"
                                    , "fantom"
                                    , "flare"
                                    , "gnosis"
                                    , "hemi"
                                    , "ink"
                                    , "kaia"
                                    , "katana"
                                    , "lens"
                                    , "linea"
                                    , "mantle"
                                    , "nova"
                                    , "opbnb"
                                    , "optimism"
                                    , "plume"
                                    , "polygon"
                                    , "ronin"
                                    , "scroll"
                                    , "sei"
                                    , "shape"
                                    , "sonic"
                                    , "sophon"
                                    , "superseed"
                                    , "tac"
                                    , "taiko"
                                    , "tron"
                                    , "unichain"
                                    , "worldchain"
                                    , "zkevm"
                                    , "zksync"
                                    , "zora"
                                ]\',
                                "sector",
                                "gas",
                                \'["soispoke", "ilemi", "0xRob", "jeff-dude", "krishhh"]\'
                            )
                        }}'
        )
}}

{% set chains = [
    "abstract"
    , "apechain"
    , "arbitrum"
    , "avalanche_c"
    , "b3"
    , "base"
    , "berachain"
    , "blast"
    , "bnb"
    , "bob"
    , "boba"
    , "celo"
    , "corn"
    , "degen"
    , "ethereum"
    , "fantom"
    , "flare"
    , "gnosis"
    , "hemi"
    , "ink"
    , "kaia"
    , "katana"
    , "linea"
    , "lens"
    , "mantle"
    , "nova"
    , "opbnb"
    , "optimism"
    , "plume"
    , "polygon"
    , "ronin"
    , "scroll"
    , "sei"
    , "shape"
    , "sonic"
    , "sophon"
    , "superseed"
    , "tac"
    , "taiko"
    , "tron"
    , "unichain"
    , "worldchain"
    , "zkevm"
    , "zksync"
    , "zora"
] %}


SELECT
    *
FROM
(
    {% for blockchain in chains %}
    SELECT
        blockchain
        ,block_month
        ,block_date
        ,block_time
        ,block_number
        ,tx_hash
        ,tx_from
        ,tx_to
        ,gas_price
        ,gas_used
        ,currency_symbol
        ,tx_fee
        ,tx_fee_usd
        ,tx_fee_raw
        ,tx_fee_breakdown
        ,tx_fee_breakdown_usd
        ,tx_fee_breakdown_raw
        ,tx_fee_currency
        ,block_proposer
        ,gas_limit
        ,gas_limit_usage
    FROM
        {{ ref('gas_' ~ blockchain ~ '_fees') }}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
)