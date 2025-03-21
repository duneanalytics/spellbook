{{ config(
    schema = 'gas',
    alias = 'fees',
    post_hook='{{ expose_spells(\'[
                                    "abstract"
                                    , "apechain"
                                    , "arbitrum"
                                    , "avalanche_c"
                                    , "base"
                                    , "blast"
                                    , "bnb"
                                    , "celo"
                                    , "ethereum"
                                    , "fantom"
                                    , "gnosis"
                                    , "linea"
                                    , "mantle"
                                    , "optimism"
                                    , "polygon"
                                    , "ronin"
                                    , "scroll"
                                    , "sei"
                                    , "solana"
                                    , "tron"
                                    , "zkevm"
                                    , "zksync"
                                    , "zora"
                                ]\',
                                "sector",
                                "gas",
                                \'["soispoke", "ilemi", "0xRob", "jeff-dude"]\'
                            )
                        }}'
        )
}}

{% set chains = [
    "abstract"
    , "apechain"
    , "arbitrum"
    , "avalanche_c"
    , "base"
    , "blast"
    , "bnb"
    , "celo"
    , "ethereum"
    , "fantom"
    , "gnosis"
    , "linea"
    , "mantle"
    , "optimism"
    , "polygon"
    , "ronin"
    , "scroll"
    , "sei"
    , "tron"
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

    UNION ALL

    SELECT
        blockchain
        , block_month
        , block_date
        , block_time
        , block_slot AS block_number
        , cast(from_base58(tx_hash) as varbinary) as tx_hash
        , cast(from_base58(signer) as varbinary) as tx_from
        , cast (NULL AS varbinary) tx_to -- this concept doesn't exist in solana
        , cast (NULL AS double) AS gas_price -- this concept doesn't exist in solana
        , cast (NULL AS double) AS gas_used -- this concept doesn't exist in solana
        , currency_symbol
        , tx_fee
        , tx_fee_usd
        , tx_fee_raw
        , tx_fee_breakdown
        , tx_fee_breakdown_usd
        , tx_fee_breakdown_raw
        , cast(from_base58(tx_fee_currency) as varbinary) as tx_fee_currency
        , cast(from_base58(leader) as varbinary) AS block_proposer
        , cast (NULL AS double) AS gas_limit -- this concept doesn't exist in solana
        , cast (NULL AS double) AS gas_limit_usage -- this concept doesn't exist in solana
    FROM {{ source('gas_solana', 'fees') }}
)