{{ config(
        alias = 'batches',
        
        post_hook='{{ expose_spells(\'["ethereum", "gnosis"]\',
                                    "project",
                                    "cow_protocol",
                                    \'["bh2smith", "gentrexha", "olgafetisova"]\') }}'
        )
}}
SELECT *
FROM
(
        SELECT
            'ethereum' AS blockchain,
            'cow_protocol' AS project,
            '1' AS version,
            block_date,
            block_time,
            num_trades,
            dex_swaps,
            batch_value,
            solver_address,
            tx_hash,
            gas_price,
            gas_used,
            tx_cost_usd,
            fee_value,
            call_data_size,
            unwraps,
            token_approvals
        FROM {{ ref('cow_protocol_ethereum_batches') }}

        UNION ALL

        SELECT
            'gnosis' AS blockchain,
            'cow_protocol' AS project,
            '1' AS version,
            block_date,
            block_time,
            num_trades,
            dex_swaps,
            batch_value,
            solver_address,
            tx_hash,
            gas_price,
            gas_used,
            tx_cost_usd,
            fee_value,
            call_data_size,
            unwraps,
            token_approvals
        FROM {{ ref('cow_protocol_gnosis_batches') }}

         UNION ALL

        SELECT
            'arbitrum' AS blockchain,
            'cow_protocol' AS project,
            '1' AS version,
            block_date,
            block_time,
            num_trades,
            dex_swaps,
            batch_value,
            solver_address,
            tx_hash,
            gas_price,
            gas_used,
            tx_cost_usd,
            fee_value,
            call_data_size,
            unwraps,
            token_approvals
        FROM {{ ref('cow_protocol_arbitrum_batches') }}

         UNION ALL

        SELECT
            'base' AS blockchain,
            'cow_protocol' AS project,
            '1' AS version,
            block_date,
            block_time,
            num_trades,
            dex_swaps,
            batch_value,
            solver_address,
            tx_hash,
            gas_price,
            gas_used,
            tx_cost_usd,
            fee_value,
            call_data_size,
            unwraps,
            token_approvals
        FROM {{ ref('cow_protocol_base_batches') }}

         UNION ALL

        SELECT
            'avalanche_c' AS blockchain,
            'cow_protocol' AS project,
            '1' AS version,
            block_date,
            block_time,
            num_trades,
            dex_swaps,
            batch_value,
            solver_address,
            tx_hash,
            gas_price,
            gas_used,
            tx_cost_usd,
            fee_value,
            call_data_size,
            unwraps,
            token_approvals
        FROM {{ ref('cow_protocol_avalanche_c_batches') }}
    
         UNION ALL

        SELECT
            'polygon' AS blockchain,
            'cow_protocol' AS project,
            '1' AS version,
            block_date,
            block_time,
            num_trades,
            dex_swaps,
            batch_value,
            solver_address,
            tx_hash,
            gas_price,
            gas_used,
            tx_cost_usd,
            fee_value,
            call_data_size,
            unwraps,
            token_approvals
        FROM {{ ref('cow_protocol_polygon_batches') }}
)
