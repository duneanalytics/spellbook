{{ config(
    schema = 'balancer_hyperevm',
    alias = 'pools_tokens_weights'
    )
}}

SELECT * FROM {{ ref('balancer_v3_hyperevm_pools_tokens_weights') }}
