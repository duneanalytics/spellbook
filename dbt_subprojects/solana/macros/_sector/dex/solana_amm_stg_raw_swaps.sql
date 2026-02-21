{% macro solana_amm_stg_raw_swaps(
    program_id,
    discriminator_filter,
    project_start_date,
    pool_id_expression = "CAST(NULL AS VARCHAR)"
) %}

WITH swaps AS (
    SELECT
          block_slot
        , CAST(date_trunc('month', block_date) AS DATE) AS block_month
        , block_date
        , block_time
        , COALESCE(inner_instruction_index, 0) AS inner_instruction_index
        , outer_instruction_index
        , outer_executing_account
        , is_inner
        , tx_id
        , tx_signer
        , tx_index
        , {{ pool_id_expression }} AS pool_id
        , {{ solana_instruction_key(
              'block_slot'
            , 'tx_index'
            , 'outer_instruction_index'
            , 'inner_instruction_index'
          ) }} AS surrogate_key
    FROM {{ source('solana', 'instruction_calls') }}
    WHERE 1=1
        AND executing_account = '{{ program_id }}'
        AND tx_success = true
        AND {{ discriminator_filter }}
        {% if is_incremental() or true -%}
        AND {{ incremental_predicate('block_date') }}
        {% else -%}
        AND block_date >= DATE '{{ project_start_date }}'
        {% endif -%}
)

SELECT * FROM swaps

{% endmacro %}
