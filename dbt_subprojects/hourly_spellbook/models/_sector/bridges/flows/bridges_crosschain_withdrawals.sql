{{ config(
    schema = 'bridges_crosschain'
    , alias = 'withdrawals'
    , materialized = 'view'
    , post_hook='{{ expose_spells(\'["arbitrum", "avalanche_c", "base", "blast", "bnb", "ethereum", "hyperevm", "ink", "lens", "linea", "optimism", "plasma", "polygon", "scroll", "unichain", "worldchain", "zksync", "zora", "fantom", "gnosis", "nova", "opbnb", "berachain", "corn", "flare", "sei", "mantle"]\',
                                "sector",
                                "bridges",
                                \'["hildobby"]\') }}'
)
}}
{% set vms = [
    'evms'
] %}

SELECT *
    FROM (
        {% for vm in vms %}
        SELECT deposit_chain
        , withdrawal_chain
        , bridge_name
        , bridge_version
        , block_date
        , block_time
        , block_number
        , withdrawal_amount
        , withdrawal_amount_raw
        , CAST(sender AS VARCHAR) AS sender
        , CAST(recipient AS VARCHAR) AS recipient
        , withdrawal_token_standard
        , CAST(withdrawal_token_address AS VARCHAR) AS withdrawal_token_address
        , CAST(tx_from AS VARCHAR) AS tx_from
        , CAST(tx_hash AS VARCHAR) AS tx_hash
        , evt_index
        , CAST(contract_address AS VARCHAR) AS contract_address
        , bridge_transfer_id
        , deposit_chain_id
        FROM {{ ref('bridges_'~vm~'_withdrawals') }}
        {% if not loop.last %}
        UNION ALL
        {% endif %}
        {% endfor %}
        )