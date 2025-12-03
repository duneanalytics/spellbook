{{
    config(
        schema = 'tokens_v1'
        ,alias = 'erc20'
        ,materialized = 'table'
        ,partition_by = ['blockchain']
    )
}}

/*
    the main source for v1 erc20 tokens is dune.definedfi.dataset_tokens -- an automated source pulled into Dune
    in order to maintain the same amount of coverage of tokens as before, and provide future addition framework, each chain still contains a static file to add for any missing in the automated source
*/

{% set static_models = {
    'tokens_abstract': {'blockchain': 'abstract', 'model': ref('tokens_abstract_v1_erc20')}
    ,'tokens_apechain': {'blockchain': 'apechain', 'model': ref('tokens_apechain_v1_erc20')}
    ,'tokens_arbitrum': {'blockchain': 'arbitrum', 'model': ref('tokens_arbitrum_v1_erc20')}
    ,'tokens_avalanche_c': {'blockchain': 'avalanche_c', 'model': ref('tokens_avalanche_c_v1_erc20')}
    ,'tokens_base': {'blockchain': 'base', 'model': ref('tokens_base_v1_erc20')}
    ,'tokens_berachain': {'blockchain': 'berachain', 'model': ref('tokens_berachain_v1_erc20')}
    ,'tokens_blast': {'blockchain': 'blast', 'model': ref('tokens_blast_v1_erc20')}
    ,'tokens_bnb': {'blockchain': 'bnb', 'model': ref('tokens_bnb_v1_erc20')}
    ,'tokens_bob': {'blockchain': 'bob', 'model': ref('tokens_bob_v1_erc20')}
    ,'tokens_boba': {'blockchain': 'boba', 'model': ref('tokens_boba_v1_erc20')}
    ,'tokens_celo': {'blockchain': 'celo', 'model': ref('tokens_celo_v1_erc20')}
    ,'tokens_corn': {'blockchain': 'corn', 'model': ref('tokens_corn_v1_erc20')}
    ,'tokens_ethereum': {'blockchain': 'ethereum', 'model': ref('tokens_ethereum_v1_erc20')}
    ,'tokens_fantom': {'blockchain': 'fantom', 'model': ref('tokens_fantom_v1_erc20')}
    ,'tokens_flare': {'blockchain': 'flare', 'model': ref('tokens_flare_v1_erc20')}
    ,'tokens_fuse': {'blockchain': 'fuse', 'model': ref('tokens_fuse_v1_erc20')}
    ,'tokens_gnosis': {'blockchain': 'gnosis', 'model': ref('tokens_gnosis_v1_erc20')}
    ,'tokens_goerli': {'blockchain': 'goerli', 'model': ref('tokens_goerli_v1_erc20')}
    ,'tokens_ink': {'blockchain': 'ink', 'model': ref('tokens_ink_v1_erc20')}
    ,'tokens_kaia': {'blockchain': 'kaia', 'model': ref('tokens_kaia_v1_erc20')}
    ,'tokens_lens': {'blockchain': 'lens', 'model': ref('tokens_lens_v1_erc20')}
    ,'tokens_linea': {'blockchain': 'linea', 'model': ref('tokens_linea_v1_erc20')}
    ,'tokens_mantle': {'blockchain': 'mantle', 'model': ref('tokens_mantle_v1_erc20')}
    ,'tokens_nova': {'blockchain': 'nova', 'model': ref('tokens_nova_v1_erc20')}
    ,'tokens_opbnb': {'blockchain': 'opbnb', 'model': ref('tokens_opbnb_v1_erc20')}
    ,'tokens_optimism': {'blockchain': 'optimism', 'model': ref('tokens_optimism_v1_erc20')}
    ,'tokens_plume': {'blockchain': 'plume', 'model': ref('tokens_plume_v1_erc20')}
    ,'tokens_polygon': {'blockchain': 'polygon', 'model': ref('tokens_polygon_v1_erc20')}
    ,'tokens_ronin': {'blockchain': 'ronin', 'model': ref('tokens_ronin_v1_erc20')}
    ,'tokens_scroll': {'blockchain': 'scroll', 'model': ref('tokens_scroll_v1_erc20')}
    ,'tokens_sei': {'blockchain': 'sei', 'model': ref('tokens_sei_v1_erc20')}
    ,'tokens_sepolia': {'blockchain': 'sepolia', 'model': ref('tokens_sepolia_v1_erc20')}
    ,'tokens_shape': {'blockchain': 'shape', 'model': ref('tokens_shape_v1_erc20')}
    ,'tokens_sonic': {'blockchain': 'sonic', 'model': ref('tokens_sonic_v1_erc20')}
    ,'tokens_sophon': {'blockchain': 'sophon', 'model': ref('tokens_sophon_v1_erc20')}
    ,'tokens_tron': {'blockchain': 'tron', 'model': ref('tokens_tron_v1_erc20')}
    ,'tokens_unichain': {'blockchain': 'unichain', 'model': ref('tokens_unichain_v1_erc20')}
    ,'tokens_viction': {'blockchain': 'viction', 'model': ref('tokens_viction_v1_erc20')}
    ,'tokens_worldchain': {'blockchain': 'worldchain', 'model': ref('tokens_worldchain_v1_erc20')}
    ,'tokens_xlayer': {'blockchain': 'xlayer', 'model': ref('tokens_xlayer_v1_erc20')}
    ,'tokens_zkevm': {'blockchain': 'zkevm', 'model': ref('tokens_zkevm_v1_erc20')}
    ,'tokens_zksync': {'blockchain': 'zksync', 'model': ref('tokens_zksync_v1_erc20')}
    ,'tokens_zora': {'blockchain': 'zora', 'model': ref('tokens_zora_v1_erc20')}
} %}

with automated_source as (
    with blockchain_info as (
        select
            name as blockchain
            , chain_id
        from
            {{ source('dune', 'blockchains') }}
    ), raw_source as (
        select
            i.blockchain
            , t.address as contract_address
            , case
                when (
                    t.address = 0x0000000000000000000000000000000000001010
                    and i.blockchain = 'polygon'
                ) then 'POL' -- source has incorrect symbol for POL on polygon post-migration
                else t.symbol
            end as symbol
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
            join blockchain_info as i on t.networkid = i.chain_id
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
            -- incorrect decimal assignment in raw source
            0xeb9951021698b42e4399f9cbb6267aa35f82d59d
            , 0x0ba45a8b5d5575935b8158a88c631e9f9c95a2e5
            -- incorrect naming of raw source
            , 0x136471a34f6ef19fe571effc1ca711fdb8e49f2b -- USYC
        )
), static_source as (
    {% for key, value in static_models.items() %}
    select
        '{{ value.blockchain }}' as blockchain
        , contract_address
        , symbol
        , cast(decimals as integer) as decimals
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
        cast(s.decimals as integer) as decimals
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
