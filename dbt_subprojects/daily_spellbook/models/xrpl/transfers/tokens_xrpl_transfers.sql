{{
    config(
        schema = 'tokens_xrpl',
        alias = 'transfers',
        materialized = 'view',
        post_hook='{{ expose_spells(\'["xrpl"]\',
                                    "sector",
                                    "tokens",
                                    \'["krishhh"]\') }}'
    )
}}

{% set transfer_models = [
    ref('tokens_xrpl_payments_transfers'),
    ref('tokens_xrpl_payment_channel_claims_transfers'),
    ref('tokens_xrpl_check_cash_transfers'),
    ref('tokens_xrpl_escrow_finish_transfers'),
    ref('tokens_xrpl_amm_deposits_transfers'),
    ref('tokens_xrpl_amm_withdraws_transfers')
] %}

SELECT
    *
FROM (
    {% for transfer_model in transfer_models %}
    SELECT
        blockchain
        ,block_month
        ,block_date
        ,block_time
        ,ledger_index
        ,tx_hash
        ,tx_index
        ,evt_index
        ,transfer_type
        ,token_standard
        ,transaction_type
        ,transaction_result
        ,sequence
        ,fee
        ,tx_from
        ,tx_to
        ,from_address
        ,to_address
        ,issuer
        ,currency
        ,currency_hex
        ,symbol
        ,amount_requested_raw
        ,amount_delivered_raw
        ,amount_requested
        ,amount_delivered
        ,amount
        ,partial_payment_flag
        ,price_usd
        ,amount_usd
    FROM {{ transfer_model }}
    {% if is_incremental() %}
    WHERE {{ incremental_predicate('block_date') }}
    {% endif %}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
)
WHERE amount > 0