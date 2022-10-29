{{ config(
    schema = 'sushiswap_avalanche_c'
    ,alias = 'trades'
    ,partition_by = ['block_date']
    ,materialized = 'incremental'
    ,file_format = 'delta'
    ,incremental_strategy = 'merge'
    ,unique_key = ['block_date', 'blockchain', 'project', 'version', 'tx_hash', 'evt_index', 'trace_address']
    ,post_hook='{{ expose_spells(\'["avalanche_c"]\',
                                      "project",
                                      "sushiswap",
                                    \'["hosuke", "zhongyiio"]\') }}'
    )
}}

WITH decodes_with_output_amounts AS (

    SELECT call_block_number,
           call_block_time,
           call_success,
           call_trace_address,
           call_tx_hash,
           contract_address,
           deadline,
           output_amounts,
           path,
           `to`
    FROM {{ source('sushiswap_v2_avalanche_c', 'SushiSwapRouter_call_swapETHForExactTokens') }}
    {% if is_incremental() %}
    WHERE call_block_time >= date_trunc("day", now() - interval '1 week')
    {% endif %}

    UNION ALL

    SELECT call_block_number,
           call_block_time,
           call_success,
           call_trace_address,
           call_tx_hash,
           contract_address,
           deadline,
           output_amounts,
           path,
           `to`
    FROM {{ source('sushiswap_v2_avalanche_c', 'SushiSwapRouter_call_swapExactETHForTokens') }}
    {% if is_incremental() %}
    WHERE call_block_time >= date_trunc("day", now() - interval '1 week')
    {% endif %}

    UNION ALL

    SELECT call_block_number,
           call_block_time,
           call_success,
           call_trace_address,
           call_tx_hash,
           contract_address,
           deadline,
           output_amounts,
           path,
           `to`
    FROM {{ source('sushiswap_v2_avalanche_c', 'SushiSwapRouter_call_swapExactTokensForETH') }}
    {% if is_incremental() %}
    WHERE call_block_time >= date_trunc("day", now() - interval '1 week')
    {% endif %}

    UNION ALL

    SELECT call_block_number,
           call_block_time,
           call_success,
           call_trace_address,
           call_tx_hash,
           contract_address,
           deadline,
           output_amounts,
           path,
           `to`
    FROM {{ source('sushiswap_v2_avalanche_c', 'SushiSwapRouter_call_swapExactTokensForTokens') }}
    {% if is_incremental() %}
    WHERE call_block_time >= date_trunc("day", now() - interval '1 week')
    {% endif %}

    UNION ALL

    SELECT call_block_number,
           call_block_time,
           call_success,
           call_trace_address,
           call_tx_hash,
           contract_address,
           deadline,
           output_amounts,
           path,
           `to`
    FROM {{ source('sushiswap_v2_avalanche_c', 'SushiSwapRouter_call_swapTokensForExactETH') }}
    {% if is_incremental() %}
    WHERE call_block_time >= date_trunc("day", now() - interval '1 week')
    {% endif %}

    UNION ALL

    SELECT call_block_number,
           call_block_time,
           call_success,
           call_trace_address,
           call_tx_hash,
           contract_address,
           deadline,
           output_amounts,
           path,
           `to`
    FROM {{ source('sushiswap_v2_avalanche_c', 'SushiSwapRouter_call_swapTokensForExactTokens') }}
    {% if is_incremental() %}
    WHERE call_block_time >= date_trunc("day", now() - interval '1 week')
    {% endif %}


),

decodes_without_amounts AS (
    SELECT call_block_number,
           call_block_time,
           call_success,
           call_trace_address,
           call_tx_hash,
           contract_address,
           deadline,
           path,
           `to`
    FROM {{ source('sushiswap_v2_avalanche_c', 'SushiSwapRouter_call_swapExactETHForTokensSupportingFeeOnTransferTokens') }}
    {% if is_incremental() %}
    WHERE call_block_time >= date_trunc("day", now() - interval '1 week')
    {% endif %}

    UNION ALL

    SELECT call_block_number,
           call_block_time,
           call_success,
           call_trace_address,
           call_tx_hash,
           contract_address,
           deadline,
           path,
           `to`
    FROM {{ source('sushiswap_v2_avalanche_c', 'SushiSwapRouter_call_swapExactTokensForETHSupportingFeeOnTransferTokens') }}
    {% if is_incremental() %}
    WHERE call_block_time >= date_trunc("day", now() - interval '1 week')
    {% endif %}

    UNION ALL

    SELECT call_block_number,
           call_block_time,
           call_success,
           call_trace_address,
           call_tx_hash,
           contract_address,
           deadline,
           path,
           `to`
    FROM {{ source('sushiswap_v2_avalanche_c', 'SushiSwapRouter_call_swapExactTokensForTokensSupportingFeeOnTransferTokens') }}
    {% if is_incremental() %}
    WHERE call_block_time >= date_trunc("day", now() - interval '1 week')
    {% endif %}

)

SELECT 0;