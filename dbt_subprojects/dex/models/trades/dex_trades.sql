{% set chains = dex_evm_chains() %}

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
{% if target.name == 'ci' -%}
-- dex_trades only ever unions in already-built chain models, so a full-history CI build adds no
-- coverage over 7 days; use target.name instead of dev_dates so this also bounds CI, not just local dev.
WHERE block_date > current_date - interval '7' day
{%- else -%}
{% if is_incremental() %}
WHERE {{ incremental_predicate('block_time') }}
{% endif %}
{%- endif %}
{% if not loop.last %}
UNION ALL
{% endif %}
{% endfor %}
