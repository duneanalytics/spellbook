{{ config(
    alias = 'transfer_mapping',
    
    partition_by = ['block_date'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_date','evt_block_time', 'evt_block_number', 'evt_tx_hash', 'evt_index'],
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

-- should rebuild on each update to upstream tables

WITH all_labels AS (
    SELECT address, label, proposal_name, address_descriptor, project_name FROM {{ ref('op_token_distributions_optimism_all_distributions_labels') }}
)


, disperse_contracts AS (
    SELECT * FROM all_labels WHERE label = 'Utility'
    )

, outgoing_distributions AS (
    WITH tfers AS (
        -- transfers out
            SELECT
                evt_block_time, 
                evt_block_number, 
                evt_index,
                tf."from" AS from_address, 
                tf.to AS to_address, 
                tx.to AS tx_to_address, 
                tx."from" AS tx_from_address,  
                evt_tx_hash,
            
            COALESCE(
                    lbl_from_util_tx.address_descriptor
                    ,lbl_from.address_descriptor
                    ) 
                    AS from_type, --override if to an incentive tx address
            COALESCE(
                    lbl_to.address_descriptor
                    ,'Other'
                    )
                    AS to_type,
                    
            COALESCE(
                     lbl_from_util_tx.label 
                    ,lbl_from.label
                    ) 
                    AS from_label, --override if to an incentive tx address
            COALESCE(
                    lbl_to.label
                    , 'Other') 
                    AS to_label,
                    
            COALESCE(
                    lbl_from_util_tx.project_name
                    ,lbl_from.project_name
                    ) 
                    AS from_name, --override if to an incentive tx address
            COALESCE(
                    lbl_to.project_name
                    ,'Other'
                    ) AS to_name,
                    
                cast(tf.value as double)/cast( 1e18 as double) AS op_amount_decimal,
                evt_index AS evt_tfer_index,
                
                bytearray_substring(tx.data, 1, 4) AS tx_method
                
            FROM {{source('erc20_optimism','evt_transfer') }} tf
            -- We want either the send or receiver to be the foundation or a project (also includes utility transfers)
            INNER JOIN all_labels lbl_from
                ON lbl_from.address = tf."from"
            -- if the recipient is in this list to, then we track it
            LEFT JOIN all_labels lbl_to
                ON lbl_to.address = tf.to
    
                
            LEFT JOIN {{ source('optimism','transactions') }} tx
                ON tx.hash = tf.evt_tx_hash
                AND tx.block_number = tf.evt_block_number
                {% if is_incremental() %} 
                and tx.block_time >= date_trunc('day', now() - interval '7' day)
                {% else %}
                AND tx.block_time >= cast('{{op_token_launch_date}}' as date)
                {% endif %}
            
            LEFT JOIN disperse_contracts dc
                ON tx.to = dc.address
            
            LEFT JOIN all_labels lbl_from_util_tx
                ON lbl_from_util_tx.address = tx."from" --label of the transaction sender
                AND dc.address IS NOT NULL --we have a disperse
                
            -- LEFT JOIN tx_labels txl
            --     ON txl.tx_hash = tf.evt_tx_hash
                
            WHERE tf.contract_address = {{op_token_address}}
            --exclude Wintermute funding tfers
            AND NOT (tf."from" = 0x2501c477d0a35545a387aa4a3eee4292a9a8b3f0
                    and tf.to IN (0x4f3a120e72c76c22ae802d129f599bfdbc31cb81
                            ,0x51d3a2f94e60cbecdce05ab41b61d7ce5240b8ff)
                    )
            {% if is_incremental() %} 
            and tf.evt_block_time >= date_trunc('day', now() - interval '7' day)
            {% else %}
            AND tf.evt_block_time >= cast('{{op_token_launch_date}}' as date)
            {% endif %}
            
            -- For CEXs, exclude CEX to CEX or CEX withdrawals
            AND 1= ( 
                CASE
                    WHEN lbl_from.label != 'CEX' THEN 1 --when not a CEX, keep it
                    WHEN lbl_from.label = 'CEX' AND lbl_to.label = 'CEX' THEN 0
                    WHEN lbl_from.label = 'CEX' AND lbl_to.label IS NULL THEN 0
                    ELSE 1
                END
                )
            -- for utility contracts, ensure that the tx_from is a project wallet
            AND 1 = (
                CASE
                WHEN dc.address IS NULL THEN 1 -- when not a utility transfer, keep it
                WHEN dc.address IS NOT NULL
                    AND (
                        tx."from" IN (SELECT address FROM all_labels WHERE label != 'Utility')
                        OR
                        tf."from" IN (SELECT address FROM all_labels WHERE label != 'Utility')
                     ) THEN 1 --when utility, make sure the transaction or transfer is from a project wallet
                ELSE 0
                END

            )

            GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17 --get uniques b/c of duplicated receiver addresses
            
            )

        SELECT
            evt_block_time, 
            evt_block_number, 
            evt_index,
            from_address, 
            to_address, 
            tx_to_address, 
            tx_from_address, 
            evt_tx_hash,
            from_type, 
            to_type, 
            from_label, 
            from_name, 
            to_label, 
            o.to_name, 
            op_amount_decimal, 
            tx_method
        FROM {{ ref('op_token_distributions_optimism_other_distributions_claims') }} o
        {% if is_incremental() %} 
            where o.evt_block_time >= date_trunc('day', now() - interval '7' day)
        {% endif %}

        UNION ALL
        
        SELECT
            t.evt_block_time, 
            t.evt_block_number, 
            t.evt_index,
            t.from_address, 
            t.to_address, 
            t.tx_to_address, 
            t.tx_from_address, 
            t.evt_tx_hash,
            t.from_type, 
            t.to_type, 
            t.from_label, 
            t.from_name, 
            t.to_label, 
            t.to_name, 
            t.op_amount_decimal, 
            t.tx_method
        
        FROM tfers t
        LEFT JOIN {{ ref('op_token_distributions_optimism_other_distributions_claims') }} o --don't double count - at the amount level b/c there could be multiple claims in one tx
            ON t.evt_block_number = o.evt_block_number
            AND t.evt_block_time = o.evt_block_time
            AND t.evt_tx_hash = o.evt_tx_hash
            AND (
                t.evt_tfer_index = o.min_evt_tfer_index
                OR
                t.evt_tfer_index = o.max_evt_tfer_index
                )
            {% if is_incremental() %} 
            and o.evt_block_time >= date_trunc('day', now() - interval '7' day)
            {% endif %}
        WHERE o.evt_block_number IS NULL
)


, distributions AS (

SELECT 
     evt_block_time, evt_block_number, evt_index, evt_tx_hash
    --
    , from_address, to_address
    , tx_to_address, tx_from_address
    --
    , from_type, to_type
    , from_label, to_label
    , from_name, to_name
    , op_amount_decimal, tx_method,

    -- Assume 'Other' addresses (i.e. an unknown address) are end users.
    CASE WHEN to_label LIKE '%Other%' THEN op_amount_decimal ELSE 0 END AS op_claimed,
    
    -- When tokens go to a 'Deployed' address, we assume deployed. Or to an end user from an address we don't already know to be deployed.
    CASE WHEN  ( to_label LIKE '%Other%' OR to_label LIKE '%Deployed%') AND from_label NOT LIKE '%Deployed%' THEN op_amount_decimal
         WHEN (from_name != to_name) AND from_label LIKE '%Project%' AND to_label LIKE '%Project%' THEN op_amount_decimal --handle for distirbutions to other projects (i.e. Uniswap to Gamma)
        ELSE 0 END
    AS op_deployed,
    
    -- When from the foundation grants wallets to a project, we mark as "op to project"
    CASE WHEN from_label = '{{foundation_label}}' AND from_type = '{{grants_descriptor}}' AND to_label LIKE '%Project%' THEN op_amount_decimal
        ELSE 0 END
    AS op_to_project,
    
    -- Tokens being transferred between projects (i.e. Uniswap to Gamma)
    CASE WHEN from_label LIKE '%Project%' AND to_label LIKE '%Project%'
            AND from_name != to_name THEN op_amount_decimal
        ELSE 0 END
    AS op_between_projects,
    
    -- Tokens going from being deployed back to the project
    CASE
        WHEN from_label NOT LIKE '%Project%' and to_label LIKE '%Project%' AND from_name = to_name THEN op_amount_decimal --Projects Clawback
        WHEN from_type = 'OP Foundation Airdrops' AND to_label = '{{foundation_label}}' THEN op_amount_decimal --Airdrop Clawback
        ELSE 0 END
    AS op_incoming_clawback, --Project's deployer back to the OG project wallet

    -- Tokens going to an intermediate utility contract to be deployed
    CASE WHEN from_label LIKE '%Project%' and to_label = 'Utility' THEN op_amount_decimal
        ELSE 0 END
    AS op_to_utility_contract,

    -- Tokens coming from unkown wallets back to the project
    CASE WHEN from_label LIKE '%Other%' and (to_label LIKE '%Project%' OR to_label = 'OP Foundation') THEN op_amount_decimal
        ELSE 0 END
    AS op_incoming_other,

    -- Tag Retropgf distributions - if OP
    CASE WHEN from_label = '{{foundation_label}}' AND from_type = '{{grants_descriptor}}' AND to_label = 'RetroPGF' THEN op_amount_decimal
        ELSE 0 END
    AS op_for_retropgf
            
    FROM outgoing_distributions od

)



SELECT 
    cast(DATE_TRUNC('day',evt_block_time) AS date) AS block_date
    , evt_block_time, evt_block_number, evt_index, evt_tx_hash
    --
    , from_address, to_address
    , tx_to_address, tx_from_address
    --
    , from_type, to_type
    , d.from_label, d.to_label
    , COALESCE(dfrom.address_name, d.from_name) AS from_name
    , COALESCE(dto.address_name, dtxto.address_name, d.to_name) AS to_name
    --
    , op_amount_decimal, tx_method
    --
    , cast(op_claimed as double) AS op_claimed
    , cast(op_deployed as double) as op_deployed
    , cast(op_to_project as double) as op_to_project
    , cast(op_between_projects as double) as op_between_projects
    , cast(op_incoming_clawback as double) as op_incoming_clawback
    , cast(op_to_utility_contract as double) AS op_to_utility_contract
    , cast(op_incoming_other as double) AS op_incoming_other
    , cast(op_for_retropgf as double) AS op_for_retropgf
    --
    , d.to_name AS og_to_name --keep original name in case we want it
    , d.from_name AS og_from_name --keep original name in case we want it
    
FROM distributions d
-- read in other tags
LEFT JOIN {{ ref('op_token_distributions_optimism_other_tags') }} dto
    ON dto.address = d.to_address
    AND d.to_name = 'Other' -- don't overwrite existing mapping
LEFT JOIN {{ ref('op_token_distributions_optimism_other_tags') }} dtxto
    ON dtxto.address = d.tx_to_address
    AND d.to_name = 'Other' -- don't overwrite existing mapping
LEFT JOIN {{ ref('op_token_distributions_optimism_other_tags') }} dfrom
    ON dfrom.address = d.from_address
    AND d.from_name = 'Other' -- don't overwrite existing mapping
