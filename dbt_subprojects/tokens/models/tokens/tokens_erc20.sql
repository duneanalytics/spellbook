{{
    config(
        schema = 'tokens'
        ,alias = 'erc20'
        ,materialized = 'incremental'
        ,incremental_strategy = 'append'
        ,unique_key = ['blockchain', 'contract_address']
        ,file_format = 'delta'
        ,partition_by = ['blockchain']
        ,post_hook='{{ expose_spells(\'[
                                        "arbitrum"
                                        ,"avalanche_c"
                                        ,"base"
                                        ,"blast"
                                        ,"bnb"
                                        ,"boba"
                                        ,"celo"
                                        ,"corn"
                                        ,"ethereum"
                                        ,"fantom"
                                        ,"fuse"
                                        ,"gnosis"
                                        ,"goerli"
                                        ,"ink"
                                        ,"abstract"
                                        ,"kaia"
                                        ,"lens"
                                        ,"linea"
                                        ,"mantle"
                                        ,"nova"
                                        ,"optimism"
                                        ,"ronin"
                                        ,"plume"
                                        ,"polygon"
                                        ,"scroll"
                                        ,"sei"
                                        ,"sepolia"
                                        ,"shape"
                                        ,"worldchain"
                                        ,"zkevm"
                                        ,"zksync"
                                        ,"zora"
                                        ,"bob"
                                        ,"sonic"
                                        ,"sophon"
                                        ,"berachain"
                                        ,"apechain"
                                        ,"opbnb"
                                        ,"unichain"
                                    ]\',
                                    "sector",
                                    "tokens",
                                    \'["bh2smith","0xManny","hildobby","soispoke","dot2dotseurat","mtitus6","wuligy","lgingerich","0xRob","jeff-dude","viniabussafi","IrishLatte19","angus_1","Henrystats","rantum", "IrishLatte19", "captncrunch"]\') }}'
    )
}}

with t as (
    select 
        blockchain
        ,contract_address
        ,symbol
        ,name
        ,decimals   
    from {{source('tokens_v2', 'erc20')}}
    union all
    select 
        blockchain
        ,contract_address
        ,symbol
        ,symbol as name
        ,decimals
    from (
        select * from {{ref('tokens_v1_erc20')}}
        where blockchain not in (select distinct blockchain from {{source('tokens_v2', 'erc20')}})
    )
)
select
    *
from t
{% if is_incremental() -%}
left join {{this}} as target
    on t.blockchain = target.blockchain
    and t.contract_address = target.contract_address
where target.blockchain is null
{% endif -%}