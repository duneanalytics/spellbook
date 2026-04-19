{% set chains = [
    'abstract'
    , 'apechain'
    , 'arbitrum'
    , 'avalanche_c'
    , 'base'
    , 'berachain'
    , 'blast'
    , 'bnb'
    , 'boba'
    , 'celo'
    , 'corn'
    , 'ethereum'
    , 'fantom'
    , 'flare'
    , 'flow'
    , 'gnosis'
    , 'hemi'
    , 'hyperevm'
    , 'ink'
    , 'kaia'
    , 'katana'
    , 'linea'
    , 'mantle'
    , 'megaeth'
    , 'mezo'
    , 'monad'
    , 'nova'
    , 'opbnb'
    , 'optimism'
    , 'peaq'
    , 'plasma'
    , 'plume'
    , 'polygon'
    , 'ronin'
    , 'scroll'
    , 'sei'
    , 'shape'
    , 'somnia'
    , 'sonic'
    , 'sophon'
    , 'story'
    , 'superseed'
    , 'tac'
    , 'taiko'
    , 'tempo'
    , 'unichain'
    , 'worldchain'
    , 'xlayer'
    , 'zkevm'
    , 'zksync'
    , 'zora'
] %}

{{ config(
    schema = 'dex'
    , alias = 'trades'
    , partition_by = ['block_month', 'blockchain', 'project']
    , materialized = 'incremental'
    , file_format = 'delta'
    , incremental_strategy = 'merge'
    , unique_key = ['blockchain', 'project', 'version', 'tx_hash', 'evt_index', 'block_month']
    , incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    , merge_skip_unchanged = true
    , post_hook='{{ expose_spells(blockchains = \'["' + chains | join('","') + '"]\',
                                    spell_type = "sector",
                                    spell_name = "dex",
                                    contributors = \'["hosuke", "0xrob", "jeff-dude", "tomfutago", "viniabussafi", "krishhh", "kryptaki"]\') }}'
    )
}}

{% for chain in chains %}
SELECT
     blockchain
    , project
    , version
    , block_month
    , CAST(block_date AS date) AS block_date
    , block_time
    , block_number
    , token_bought_symbol
    , token_sold_symbol
    , token_pair
    , token_bought_amount
    , token_sold_amount
    , token_bought_amount_raw
    , token_sold_amount_raw
    , amount_usd
    , token_bought_address
    , token_sold_address
    , taker
    , maker
    , project_contract_address
    , tx_hash
    , tx_from
    , tx_to
    , CAST(evt_index AS bigint) AS evt_index
    , _updated_at
FROM
    {{ ref('dex_'~chain~'_trades') }}
{% if var('dev_dates', false) -%}
WHERE block_date > current_date - interval '3' day
{%- else -%}
{% if is_incremental() %}
WHERE {{ incremental_predicate('block_time') }}
{% endif %}
{%- endif %}
{% if not loop.last %}
UNION ALL
{% endif %}
{% endfor %}
