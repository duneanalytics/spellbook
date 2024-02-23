{{
  config(
    schema = 'lending',
    alias = 'flashloans',
    partition_by = ['blockchain', 'project', 'block_month'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['blockchain', 'project', 'version', 'tx_hash', 'evt_index'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
    post_hook = '{{ expose_spells(\'["arbitrum", "avalanche_c", "base", "bnb", "celo", "ethereum", "fantom", "gnosis", "optimism", "polygon", "scroll", "zksync"]\',
                                "sector",
                                "lending",
                                \'["tomfutago"]\') }}'
  )
}}

{{
  lending_enrich_flashloans(
    model = ref('lending_base_flashloans')
  )
}}
