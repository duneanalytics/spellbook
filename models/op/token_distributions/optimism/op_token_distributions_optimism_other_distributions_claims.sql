{{ config(
    alias = 'other_distributions_claims',
    
    partition_by = ['block_date'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_date', 'evt_block_time', 'evt_block_number', 'evt_tx_hash', 'evt_index'],
    post_hook='{{ expose_spells(\'["optimism"]\',
                                "project",
                                "op_token_distributions",
                                \'["msilb7", "chuxin"]\') }}'
    )
}}

{% set op_token_address = '0x4200000000000000000000000000000000000042' %}
{% set op_token_launch_date = '2022-05-31'  %}
{% set foundation_label = 'OP Foundation'  %}
{% set grants_descriptor = 'OP Foundation Grants'  %}


--protocols that never claimed and transferred from the fnd wallet
WITH all_labels AS (
    SELECT address, label, proposal_name, address_descriptor, project_name FROM {{ ref('op_token_distributions_optimism_all_distributions_labels') }}
)

, aave_lm_claims AS ( 
SELECT
    CAST(DATE_TRUNC('day',evt_block_time) as date) AS block_date,
    evt_block_time, evt_block_number, evt_index, 
    tx_to_address, tx_from_address,
    evt_tx_hash, from_label, from_type, from_name, 
    to_type, to_label, to_name, op_amount_decimal, tx_method,
    MIN(evt_tfer_index) AS min_evt_tfer_index, MAX(evt_tfer_index) AS max_evt_tfer_index,
    
    (array_agg(
        CASE WHEN claim_rank_asc = 1 THEN from_address ELSE NULL END
        ) filter (where 
        CASE WHEN claim_rank_asc = 1 THEN from_address ELSE NULL END
        is not NULL))[1] as from_address,
    
    (array_agg(
        CASE WHEN claim_rank_asc = 1 THEN to_address ELSE NULL END
        ) filter (where 
        CASE WHEN claim_rank_asc = 1 THEN to_address ELSE NULL END
        is not NULL))[1] as to_address
FROM (
    SELECT 
    DATE_TRUNC('day',r.evt_block_time) AS block_date, 
    r.evt_block_time, r.evt_block_number, r.evt_index,
        tf."from" AS from_address, tf.to AS to_address, tx.to AS tx_to_address, tx."from" AS tx_from_address, r.evt_tx_hash,
        'Project' as from_label, 'Partner Fund' AS from_type, 'Aave' AS from_name, 
        tf.to as user_address
            ,'Aave - Liquidity Mining' AS to_type
            ,COALESCE(
                lbl_to.label
                , 'Other'
                ) AS to_label,
            'Other' AS to_name, cast(amount as double) / cast(1e18 as double) AS op_amount_decimal
            --get last
            , tf.evt_index AS evt_tfer_index
            , bytearray_substring(tx.data, 1, 4) AS tx_method
            
            ,ROW_NUMBER() OVER (PARTITION BY r.evt_tx_hash, r.evt_index ORDER BY tf.evt_index DESC) AS claim_rank_desc
            ,ROW_NUMBER() OVER (PARTITION BY r.evt_tx_hash, r.evt_index ORDER BY tf.evt_index ASC) AS claim_rank_asc
        
        FROM {{ source('aave_v3_optimism','RewardsController_evt_RewardsClaimed') }} r
            inner JOIN {{ source('erc20_optimism', 'evt_transfer') }} tf
                ON tf.evt_tx_hash = r.evt_tx_hash
                AND tf.evt_block_number = r.evt_block_number
                AND tf.contract_address = r.reward
                AND value = amount
                {% if is_incremental() %} 
                and tf.evt_block_time >= date_trunc('day', now() - interval '7' day)
                {% else %}
                and tf.evt_block_time >= cast('{{op_token_launch_date}}' as date)
                {% endif %}
            left JOIN all_labels lbl_from
                ON lbl_from.address = tf."from"
            -- if the recipient is in this list to, then we track it
            LEFT JOIN all_labels lbl_to
                ON lbl_to.address = tf.to
            LEFT JOIN {{ source('optimism', 'transactions') }} tx
                ON tx.hash = tf.evt_tx_hash
                AND tx.block_number = tf.evt_block_number
                AND lbl_to.label IS NULL -- don't try if we have a label on the to transfer
                {% if is_incremental() %} 
                AND tx.block_time >= date_trunc('day', now() - interval '7' day)
                {% else %}
                AND tx.block_time >= cast('{{op_token_launch_date}}' as date)
                {% endif %}

            
        WHERE reward = {{op_token_address}} --OP Token
        and cast(amount as double)/cast(1e18 as double) > 0
        AND lbl_from.label = '{{foundation_label}}'
        {% if is_incremental() %} 
        and r.evt_block_time >= date_trunc('day', now() - interval '7' day)
        {% endif %}

    ) a
    GROUP BY
    evt_block_time, evt_block_number, evt_index, 
    tx_to_address, tx_from_address,
    evt_tx_hash, from_label, from_type, from_name, 
    to_type, to_label, to_name, op_amount_decimal, tx_method

)


SELECT * FROM aave_lm_claims
-- UNION ALL
-- Add additional edge case claims here