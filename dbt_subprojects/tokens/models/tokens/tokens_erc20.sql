{{
    config(
        schema = 'tokens'
        ,alias = 'erc20'
        ,materialized = 'table'
        ,post_hook='{{ expose_spells(\'["arbitrum","avalanche_c","base","bnb","celo","ethereum","fantom","fuse","gnosis","goerli","mantle","optimism","polygon","scroll","zkevm","zksync","zora","blast","sepolia","sei","nova","linea"]\',
                        "sector",
                        "tokens",
                        \'["bh2smith","0xManny","hildobby","soispoke","dot2dotseurat","mtitus6","wuligy","lgingerich","0xRob","jeff-dude","viniabussafi","IrishLatte19","angus_1","Henrystats","rantum"]\') }}'
    )
}}

/*
    the main source for erc20 tokens is dune.definedfi.dataset_tokens -- an automated source pulled into Dune
    in order to maintain the same amount of coverage of tokens as before, and provide future addition framework, each chain still contains a static file to add for any missing in the automated source
*/

{% set static_models = {
    'tokens_arbitrum': {'blockchain': 'arbitrum', 'model': ref('tokens_arbitrum_erc20')}
    ,'tokens_avalanche_c': {'blockchain': 'avalanche_c', 'model': ref('tokens_avalanche_c_erc20')}
    ,'tokens_base': {'blockchain': 'base', 'model': ref('tokens_base_erc20')}
    ,'tokens_bnb': {'blockchain': 'bnb', 'model': ref('tokens_bnb_bep20')}
    ,'tokens_celo': {'blockchain': 'celo', 'model': ref('tokens_celo_erc20')}
    ,'tokens_ethereum': {'blockchain': 'ethereum', 'model': ref('tokens_ethereum_erc20')}
    ,'tokens_fantom': {'blockchain': 'fantom', 'model': ref('tokens_fantom_erc20')}
    ,'tokens_fuse': {'blockchain': 'fuse', 'model': ref('tokens_fuse_erc20')}
    ,'tokens_gnosis': {'blockchain': 'gnosis', 'model': ref('tokens_gnosis_erc20')}
    ,'tokens_goerli': {'blockchain': 'goerli', 'model': ref('tokens_goerli_erc20')}
    ,'tokens_mantle': {'blockchain': 'mantle', 'model': ref('tokens_mantle_erc20')}
    ,'tokens_optimism': {'blockchain': 'optimism', 'model': ref('tokens_optimism_erc20')}
    ,'tokens_polygon': {'blockchain': 'polygon', 'model': ref('tokens_polygon_erc20')}
    ,'tokens_scroll': {'blockchain': 'scroll', 'model': ref('tokens_scroll_erc20')}
    ,'tokens_zkevm': {'blockchain': 'zkevm', 'model': ref('tokens_zkevm_erc20')}
    ,'tokens_zksync': {'blockchain': 'zksync', 'model': ref('tokens_zksync_erc20')}
    ,'tokens_zora': {'blockchain': 'zora', 'model': ref('tokens_zora_erc20')}
    ,'tokens_blast': {'blockchain': 'blast', 'model': ref('tokens_blast_erc20')}
    ,'tokens_sepolia': {'blockchain': 'sepolia', 'model': ref('tokens_sepolia_erc20')}
    ,'tokens_sei': {'blockchain': 'sei', 'model': ref('tokens_sei_erc20')}
    ,'tokens_nova': {'blockchain': 'nova', 'model': ref('tokens_nova_erc20')}
    ,'tokens_linea': {'blockchain': 'linea', 'model': ref('tokens_linea_erc20')}
} %}

with
  automated_source as (
    with raw_source as (
        select
            i.blockchain
            , t.address as contract_address
            , t.symbol
            , t.decimals
            , row_number() over (
                partition by
                    i.blockchain,
                    t.address
                order by
                    t.createdat desc
            ) as rn
        from
            {{ source("definedfi", "dataset_tokens", database="dune") }} as t
            join {{ source('evms', 'info') }} as i on t.networkid = i.chain_id
    )
    select
        blockchain
        , contract_address
        , symbol
        , decimals
    from
        raw_source
    where
        rn = 1
), clean_automated_source as (
    select
        *
    from
        automated_source
    where
        contract_address not in (
            --incorrect decimal assignment in raw source
            0xeb9951021698b42e4399f9cbb6267aa35f82d59d
            , 0x0ba45a8b5d5575935b8158a88c631e9f9c95a2e5
        )
), static_source as (
    {% for key, value in static_models.items() %}
    select
        '{{ value.blockchain }}' as blockchain
        , contract_address
        , symbol
        , decimals
    from
        {{ value.model }}
    {% if value.blockchain == 'optimism' %}
    where
        symbol is not null --This can be removed if/when all other chains show all ERC20 tokens, rather than only mapped ones
    {% endif %}
    {% if not loop.last %}
    union all
    {% endif %}
    {% endfor %}
), clean_static_source as (
    select
        s.blockchain,
        s.contract_address,
        s.symbol,
        s.decimals
    from
        static_source as s
        left join automated_source as a on s.blockchain = a.blockchain
        and s.contract_address = a.contract_address
    where
        a.contract_address is null
)
select
    *
from
    clean_automated_source
union all
select
    *
from
    clean_static_source