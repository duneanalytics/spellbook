{% set blockchain = 'plasma' %}

{{
    config(
        schema = 'balancer_v3_plasma',
        alias = 'protocol_fee', 
        materialized = 'table',
        file_format = 'delta'
    )
}}

WITH base AS (
    {{
        balancer_v3_compatible_protocol_fee_macro(
            blockchain = blockchain,
            version = '3',        
            project_decoded_as = 'balancer_v3',
            base_spells_namespace = 'balancer',
            pool_labels_model = 'balancer_v3_pools_plasma'
        )
    }}
),
deduplicated AS (
    SELECT 
        *,
        ROW_NUMBER() OVER (
            PARTITION BY 
                day,
                pool_id,
                token_address,
                fee_type
            ORDER BY protocol_fee_collected_usd DESC
        ) AS rn
    FROM base
)
SELECT *
FROM deduplicated
WHERE rn = 1
