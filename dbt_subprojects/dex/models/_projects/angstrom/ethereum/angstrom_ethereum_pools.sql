{{ config(
        schema = 'angstrom_ethereum',
        alias = 'pools'
        )
}}

with 

all_pools as (

{{
    angstrom_pool_info(
          blockchain = 'ethereum'
        , angstrom_contract_addr = '0x0000000aa232009084Bd71A5797d089AA4Edfad4'
        , controller_v1_contract_addr = '0x1746484EA5e11C75e009252c102C8C33e0315fD4'
        , earliest_block = '22971781'
        , controller_pool_configured_log_topic0 = '0xf325a037d71efc98bc41dc5257edefd43a1d1162e206373e53af271a7a3224e9'
    )
}}

) 

select 
    'ethereum' as blockchain
    , * 
from 
all_pools 