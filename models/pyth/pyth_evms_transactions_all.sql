{{ config(
        schema = 'pyth',
        alias = 'transactions_all',
        post_hook='{{ expose_spells(\'["arbitrum","avalanche_c","base","bnb","celo","ethereum","gnosis","goerli","optimism","polygon","zkevm","zksync"]\',
                                "project",
                                "pyth",
                                \'["synthquest"]\') }}'
        )
}}

{% set pyth_transaction_models = [
 ref('pyth_arbitrum_transactions'),
 ref('pyth_avalanche_c_transactions'),
 ref('pyth_base_transactions'),
 ref('pyth_blast_transactions'),
 ref('pyth_bnb_transactions'),
 ref('pyth_celo_transactions'),
 ref('pyth_ethereum_transactions'),
 ref('pyth_fantom_transactions'),
 ref('pyth_gnosis_transactions'),
 ref('pyth_linea_transactions'),
 ref('pyth_mantle_transactions'),
 ref('pyth_optimism_transactions'),
 ref('pyth_polygon_transactions'),
 ref('pyth_scroll_transactions'),
 ref('pyth_zkevm_transactions'),
 ref('pyth_zksync_transactions')
] %}


SELECT *
FROM (
    {% for transfer_model in pyth_transaction_models %}
    SELECT
       chain,
       pyth_contract,
       chain_type,
       block_time,
       block_date,
       block_number,
       trace_value,
       txn_value,
       trace_gas_used,
       txn_gas_used,
       tx_hash,
       trace_from,
       trace_to,
       txn_from,
       txn_to,
       call_type,
       trace_address,
       sub_traces,
       trace_gas_paid_in_eth,
       txn_gas_paid_in_eth,
       function_signature,
       namespace,
       name
    FROM {{ transfer_model }}
    {% if is_incremental() %}
    WHERE {{ incremental_predicate('block_time') }}
    {% endif %}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
)
