{{ config(
    schema = 'op_token_optimism',
    alias = 'transfer_mapping',
    partition_by = ['block_date'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_date', 'evt_block_time', 'evt_block_number', 'evt_tx_hash', 'evt_index'],
    post_hook='{{ expose_spells(\'["optimism"]\',
                                "project",
                                "op_token",
                                \'["msilb7"]\') }}'
    )
}}

{% set op_token_address = '0x4200000000000000000000000000000000000042' %}
{% set op_token_launch_date = '2022-05-31'  %}


WITH all_labels AS (
    SELECT address, label, proposal_name, address_descriptor, project_name FROM {{ ref('op_token_distributions_optimism_project_wallets') }}
    UNION ALL
    SELECT address, label, NULL AS proposal_name, address_descriptor, address_descriptor AS project_name FROM {{ ref('op_token_distributions_optimism_disperse_contracts') }}
    UNION ALL
    SELECT address, 'CEX' as label, distinct_name AS proposal_name, cex_name AS proposal_name, cex_name AS project_name FROM {{ ref('addresses_optimism_cex') }}
        WHERE address NOT IN (SELECT address FROM {{ ref('op_token_distributions_optimism_project_wallets') }})
)

, disperse_contracts AS (
    SELECT * FROM all_labels WHERE label = 'Utility'
    )

, other_tags AS (
        SELECT * FROM {{ ref('op_token_distributions_optimism_other_tags') }}
)

, other_claims AS ( --protocols that never claimed and transferred from the fnd wallet
SELECT 
    evt_block_time, evt_block_number, evt_index, 
    -- from_address, to_address, 
    tx_to_address, tx_from_address,
    evt_tx_hash, from_label, from_type, from_name, 
    --user_address,
    to_type, to_label, to_name, amount_dec, method, to_contract,
    MIN(evt_tfer_index) AS min_evt_tfer_index, MAX(evt_tfer_index) AS max_evt_tfer_index,
    -- FIRST_VALUE(CASE WHEN claim_rank_asc = 1 THEN from_address ELSE NULL END) AS from_address_map,
    -- LAST_VALUE(CASE WHEN claim_rank_desc = 1 THEN to_address ELSE NULL END) AS to_address_map,
    
    (array_agg(
        CASE WHEN claim_rank_asc = 1 THEN from_address ELSE NULL END
        ) filter (where 
        CASE WHEN claim_rank_asc = 1 THEN from_address ELSE NULL END
        is not NULL))[1] as from_address_map,
    
    (array_agg(
        CASE WHEN claim_rank_asc = 1 THEN to_address ELSE NULL END
        ) filter (where 
        CASE WHEN claim_rank_asc = 1 THEN to_address ELSE NULL END
        is not NULL))[1] as to_address_map
FROM (
    SELECT r.evt_block_time, r.evt_block_number, r.evt_index,
        tf.`from` AS from_address, tf.to AS to_address, tx.to AS tx_to_address, tx.`from` AS tx_from_address, r.evt_tx_hash,
        'Project' as from_label, 'Parter Fund' AS from_type, 'Aave' AS from_name, 
        tf.to as user_address, lbl_to.address_descriptor AS to_type
            ,COALESCE(
                lbl_to.label
                , 'Other'
                ) AS to_label,
            NULL AS to_name, cast(amount as double) / cast(1e18 as double) AS amount_dec
            --get last
            , tf.evt_index AS evt_tfer_index
            , substring(tx.data,1,10) AS method --bytearray_substring(tx.data, 1, 4) AS method
            , contract_project AS to_contract
            
            ,ROW_NUMBER() OVER (PARTITION BY r.evt_tx_hash, r.evt_index ORDER BY tf.evt_index DESC) AS claim_rank_desc
            ,ROW_NUMBER() OVER (PARTITION BY r.evt_tx_hash, r.evt_index ORDER BY tf.evt_index ASC) AS claim_rank_asc
        
        FROM {{ source('aave_v3_optimism','RewardsController_evt_RewardsClaimed') }} r
            inner JOIN {{ source('erc20_optimism', 'evt_transfer') }} tf
                ON tf.evt_tx_hash = r.evt_tx_hash
                AND tf.evt_block_number = r.evt_block_number
                AND tf.contract_address = r.reward
                AND value = amount
                {% if is_incremental() %} 
                and tf.evt_block_time >= date_trunc("day", now() - interval '1 week')
                {% endif %}
            left JOIN all_labels lbl_from
                ON lbl_from.address = tf.`from`
            -- if the recipient is in this list to, then we track it
            LEFT JOIN all_labels lbl_to
                ON lbl_to.address = tf.to
            LEFT JOIN {{ source('optimism', 'transactions') }} tx
                ON tx.hash = tf.evt_tx_hash
                AND tx.block_number = tf.evt_block_number
                AND lbl_to.label IS NULL -- don't try if we have a label on the to transfer
                AND tx.block_time > cast('{{op_token_launch_date}}' as date)
                {% if is_incremental() %} 
                and tx.block_time >= date_trunc("day", now() - interval '1 week')
                {% endif %}
            LEFT JOIN {{ ref('contracts_optimism_contract_mapping') }} cm
                ON cm.contract_address = tx.to
            
        WHERE reward = '{{op_token_address}}' --OP Token
        and cast(amount as double)/cast(1e18 as double) > 0
        AND tf.evt_block_time > cast('{{op_token_launch_date}}' as date) --OP token launch date
        AND lbl_from.label = 'Foundation'
        {% if is_incremental() %} 
        and r.evt_block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}

    ) a
    GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15-- Get Uniques

)


-- get token dump transactions
, get_dex_trades AS (
SELECT block_time, taker AS project_wallet, tx_hash, 'DEX Sale' AS label, 'DEX Sale - ' || array_join( array_agg(distinct project), ',') as project, SUM(token_sold_amount) AS amount_sold
FROM {{ ref('dex_trades') }} t
    INNER JOIN all_labels al
        ON al.address = t.taker
        AND al.label = 'Project'
WHERE blockchain = 'optimism'
AND token_sold_address = '{{op_token_address}}'
AND block_date > cast('2022-05-30' as date)
{% if is_incremental() %} 
and t.block_time >= date_trunc("day", now() - interval '1 week')
{% endif %}
GROUP BY 1,2,3,4

)


  , outgoing_distributions AS 
            (
                WITH tfers AS (
                -- transfers out
                    SELECT
                        evt_block_time, evt_block_number, evt_index,
                        tf.`from` AS from_address, tf.to AS to_address, tx.to AS tx_to_address, tx.`from` AS tx_from_address,  evt_tx_hash,
                        COALESCE(lbl_from_util_tx.address_descriptor
                            -- lbl_to_tx.address_descriptor,
                            ,lbl_from.address_descriptor
                            ) 
                            AS from_type, --override if to an incentive tx address
                    COALESCE(
                        CASE WHEN tf.to = dc.address THEN lbl_from_util_tx.address_descriptor ELSE NULL END --if utility, mark as internal
                        ,lbl_to.address_descriptor
                        )
                        AS to_type,
                    COALESCE(lbl_from_util_tx.label
                            -- lbl_to_tx.label,
                            ,lbl_from.label,
                            'Other') 
                            AS from_label, --override if to an incentive tx address
                    COALESCE(
                            dc.project_name,--if we have a name override, like airdrop 2
                            lbl_from_util_tx.project_name
                            -- lbl_to_tx.project,
                            ,lbl_from.project_name) 
                            AS from_name, --override if to an incentive tx address
                    COALESCE(
                        /*txl.tx_type
                        ,*/CASE WHEN tf.to = dc.address THEN lbl_from_util_tx.label ELSE NULL END --if utility, mark as internal
                        ,lbl_to.label
                        , dext.label
                            , 'Other') 
                            AS to_label,
                    COALESCE(
                        /*txl.tx_name
                        ,*/CASE WHEN tf.to = dc.address THEN lbl_from_util_tx.project_name ELSE NULL END --if utility, mark as internal
                        ,lbl_to.project_name
                        ,dext.project
                        ) AS to_name,
                        
                        contract_project AS to_contract,
                        
                        cast(tf.value as double)/POWER(10,18) AS amount_dec,
                        cast(amount_sold as double)/POWER(10,18) AS amount_sold_dec,
                        evt_index AS evt_tfer_index,
                        
                        substring(tx.data,1,10) AS method --bytearray_substring(tx.data, 1, 4) AS method
                        
                        FROM {{source('erc20_optimism','evt_transfer') }} tf
                        -- OLD: we want the sender to always be either the foundation or a project or a CEX
                        -- NEW: We want either the send or receiver to be the foundation or a project
                        INNER JOIN all_labels lbl_from
                            ON lbl_from.address = tf.`from`
                        -- if the recipient is in this list to, then we track it
                        LEFT JOIN all_labels lbl_to
                            ON lbl_to.address = tf.to
            
                        
                        LEFT JOIN {{ source('optimism','transactions') }} tx
                            ON tx.hash = tf.evt_tx_hash
                            AND tx.block_number = tf.evt_block_number
                            AND tx.block_time > cast('{{op_token_launch_date}}' as date)
                            {% if is_incremental() %} 
                            and tx.block_time >= date_trunc("day", now() - interval '1 week')
                            {% endif %}
                            AND lbl_to.label IS NULL -- don't try if we have a label on the to transfer
                
                        -- LEFT JOIN other_claims lbl_to_tx
                        -- ON lbl_to_tx.address = tx.to
                        -- AND lbl_to.label IS NULL -- don't try if we have a label on the to transfer
                    
                    LEFT JOIN disperse_contracts dc
                        ON tx.to = dc.address
                        -- AND tf.`from` = tx.to
                        -- AND tf.`from` IN (SELECT address FROM all_labels)
                    
                    LEFT JOIN all_labels lbl_from_util_tx
                        ON lbl_from_util_tx.address = tx.`from` --label of the transaction sender
                        AND dc.address IS NOT NULL --we have a disperse
                    
                    LEFT JOIN get_dex_trades dext
                        ON dext.block_time = tf.evt_block_time
                        AND dext.tx_hash = tf.evt_tx_hash
                        AND dext.project_wallet = tf.`from`
                    
                    LEFT JOIN {{ ref('contracts_optimism_contract_mapping') }} cm
                        ON cm.contract_address = tx.to
                        AND is_self_destruct = false
                        AND tx.to IS NOT NULL
                        
                    -- LEFT JOIN tx_labels txl
                    --     ON txl.tx_hash = tf.evt_tx_hash
                        
                        WHERE tf.contract_address = '{{op_token_address}}'
                        --exclude Wintermute funding tfers
                        AND NOT (tf.`from` = '0x2501c477d0a35545a387aa4a3eee4292a9a8b3f0'
                                and tf.to IN ('0x4f3a120e72c76c22ae802d129f599bfdbc31cb81'
                                        ,'0x51d3a2f94e60cbecdce05ab41b61d7ce5240b8ff')
                                )
                        AND tf.evt_block_time > cast('{{op_token_launch_date}}' as date)
                        {% if is_incremental() %} 
                        and tf.evt_block_time >= date_trunc("day", now() - interval '1 week')
                        {% endif %}
                        
                        AND 1= ( --exclude CEX to CEX or CEX withdrawals
                            CASE
                                WHEN lbl_from.label != 'CEX' THEN 1
                                WHEN lbl_from.label = 'CEX' AND lbl_to.label = 'CEX' THEN 0
                                WHEN lbl_from.label = 'CEX' AND lbl_to.label IS NULL THEN 0
                                ELSE 1
                            END
                            )
                    
                    )
                    
                    , others AS (
                        SELECT 
                        evt_block_time, evt_block_number, evt_index,
                        from_address_map AS from_address, to_address_map AS to_address, tx_to_address, tx_from_address, evt_tx_hash,
                        from_type, to_type, from_label, from_name, to_label, to_name, amount_dec, 0 AS amount_sold_dec,
                        min_evt_tfer_index, max_evt_tfer_index, method, to_contract
                        FROM other_claims
                    )
                    
                    SELECT 
                        evt_block_time, evt_block_number, evt_index,
                        from_address, to_address, tx_to_address, tx_from_address, evt_tx_hash,
                        from_type, to_type, from_label, from_name, to_label, to_name, amount_dec, amount_sold_dec, method, to_contract
                    FROM others o
                    
                    UNION ALL
                    
                    SELECT 
                        t.evt_block_time, t.evt_block_number, t.evt_index,
                        t.from_address, t.to_address, t.tx_to_address, t.tx_from_address, t.evt_tx_hash,
                        t.from_type, t.to_type, t.from_label, t.from_name, t.to_label, t.to_name, t.amount_dec, t.amount_sold_dec, t.method, t.to_contract
                    
                    FROM tfers t
                    LEFT JOIN others o --don't double count - at the amount level b/c there could be multiple claims in one tx
                        ON t.evt_block_number = o.evt_block_number
                        AND t.evt_block_time = o.evt_block_time
                        AND t.evt_tx_hash = o.evt_tx_hash
                        AND (
                            t.evt_tfer_index = o.min_evt_tfer_index
                            OR
                            t.evt_tfer_index = o.max_evt_tfer_index
                            )
                    WHERE o.evt_block_number IS NULL
                    -- WHERE t.evt_block_number IS NULL
            )

, distributions AS (

SELECT *,
    CASE WHEN to_label = 'Other' THEN amount_dec ELSE 0 END AS op_claimed,
    
    CASE WHEN  to_label IN ('Other','Deployed') AND from_label != 'Deployed' THEN amount_dec
         WHEN (from_name != to_name) AND from_label = 'Project' AND to_label = 'Project' THEN amount_dec --handle for distirbutions to other projects (i.e. Uniswap to Gamma)
    ELSE 0 END AS op_deployed,
    
    CASE WHEN from_label = 'Foundation' AND from_type = 'Grants' AND to_label = 'Project' THEN amount_dec ELSE 0 END AS op_to_project,
    
    CASE WHEN from_label = 'Project' AND to_label = 'Project'
            AND from_name != to_name THEN amount_dec ELSE 0 END AS op_between_projects,
            
    CASE WHEN from_label='Deployed' and to_label='Project' AND from_name = to_name THEN amount_dec ELSE 0 END AS op_incoming_clawback, --Project's deployer back to the OG project wallet
    -- tokens sent to users
    SUM(CASE WHEN to_label = 'Other' THEN amount_dec ELSE 0 END)
            OVER (PARTITION BY from_address, from_label, from_name, to_address ORDER BY evt_block_time ASC) AS running_op_claims,
    SUM(CASE WHEN to_label = 'Other' THEN amount_dec ELSE 0 END)
            OVER (PARTITION BY from_address, from_label, from_name ORDER BY evt_block_time ASC) AS total_running_op_claims
            
    FROM outgoing_distributions

)

SELECT distinct
DATE_TRUNC('day',evt_block_time) AS block_date,
    evt_block_time, evt_block_number, evt_index,
    from_address, to_address,
    tx_to_address, tx_from_address, evt_tx_hash,
    from_type, to_type, from_label
    , COALESCE(dfrom.name,from_name) AS from_name, to_label, COALESCE(dto.name,dtxto.name,to_name) AS to_name
    , op_amount_decimal, method as tx_method
    --
    ,cast(op_claimed as decimal) AS op_claimed
    ,cast(op_deployed as decimal) as op_deployed
    ,cast(op_to_project as decimal) as op_to_project
    ,cast(op_between_projects as decimal) as op_between_projects
    ,cast(op_incoming_clawback as decimal) as op_incoming_clawback
    ,cast(running_op_claims as decimal) as running_op_claims
    ,cast(total_running_op_claims as decimal) as total_running_op_claims
    ,cast(amount_sold_dec as decimal) as op_sold
    
    , to_name AS og_to_name
    , from_name AS og_from_name
    , to_contract
    
FROM distributions d
-- read in other tags
LEFT JOIN other_tags dto
    ON dto.address = d.to_address
    AND d.to_name IS NULL -- don't overwrite existing
LEFT JOIN other_tags dtxto
    ON dtxto.address = d.tx_to_address
    AND d.to_name IS NULL -- don't overwrite existing
LEFT JOIN other_tags dfrom
    ON dfrom.address = d.from_address
    AND d.from_name IS NULL -- don't overwrite existing