{{
        config
        (
                tags=['legacy'],
                materialized = 'view',
                alias = alias('erc20_noncompliant', legacy_model=True)
        ) 
}}

select distinct token_address
from {{ ref('transfers_ethereum_erc20_rolling_day_legacy') }}
where round(amount/power(10, 18), 6) < -0.001