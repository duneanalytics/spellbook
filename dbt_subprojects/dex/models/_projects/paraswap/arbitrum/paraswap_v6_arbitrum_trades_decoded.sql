{{ config(
    schema = 'paraswap_v6_arbitrum',
    alias = 'trades_decoded',
    partition_by = ['block_month'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.blocktime')],
    unique_key = ['call_tx_hash', 'method', 'call_trace_address'],    
    post_hook='{{ expose_spells(blockchains = \'["arbitrum"]\',
	                                spell_type = "project",
	                                spell_name = "paraswap_v6",
	                                contributors = \'["eptighte", "mwamedacen"]\') }}'
    )

}}

{{ paraswap_v6_trades_master('arbitrum', 'paraswap') }}