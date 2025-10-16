{{-
    config(
        schema = 'oneinch',
        alias = 'intent_resolvers',
        materialized = 'table',
        unique_key = ['address', 'name']
    )
-}}



with

registrations as (
    select
        addr as address
        , max_by(status, evt_block_time) as status
        , max(evt_block_time) as status_at
    from (
        select addr, evt_block_time, 'Registered' as status
        from {{ source('oneinch_ethereum', 'FusionWhitelistRegistryV1_evt_Registered') }}
        union all
        select addr, evt_block_time, 'Unregistered' as status
        from {{ source('oneinch_ethereum', 'FusionWhitelistRegistryV1_evt_Unregistered') }}
        union all
        select addr, evt_block_time, 'Registered' as status
        from {{ source('oneinch_ethereum', 'FusionWhitelistRegistryV2_evt_Registered') }}
        union all
        select addr, evt_block_time, 'Unregistered' as status
        from {{ source('oneinch_ethereum', 'FusionWhitelistRegistryV2_evt_Unregistered') }}
    )
    group by 1
)

, names(
    address
    , name
    , kyc
    , access_token_ids
) as (values
      (0xf63392356a985ead50b767a3e97a253ff870e91a, '1inch Labs'     , true  , array[0,15,17,66,67,79,81,100])
    , (0xa260f8b7c8f37c2f1bc11b04c19902829de6ac8a, 'Arctic Bastion' , true  , array[27,28,29,30,31,32,33,34,35,102])
    , (0xcfa62f77920d6383be12c91c71bd403599e1116f, 'The Open DAO'   , true  , array[])
    , (0xe023f53f735c196e4a028233c2ee425957812a41, 'Seawise'        , true  , array[])
    , (0x754bcbaf851f94ca0065d0d06d53b168daab17b8, 'Alpha'          , true  , array[])
    , (0xb2153caa185484fd377f488d89143a7fd76695ce, 'Laertes'        , true  , array[])
    , (0xc975671642534f407ebdcaef2428d355ede16a2c, 'GlueX'          , true  , array[]) -- OTEX
    , (0xd7f6f541d4210550ca56f7b4c4a549efd4cafb49, 'The T'          , true  , array[3,5,7,8,9,10,11,12,13,14,54])
    , (0x21b7db78e76dcd100f717206ee655daab2de118c, 'Spider Labs'    , true  , array[])
    , (0x12e5ceb5c14f3a1a9971da154f6530c1cf7274ac, 'Rosato LLC'     , true  , array[])
    , (0xee230dd7519bc5d0c9899e8704ffdc80560e8509, 'Kinetex Labs'   , true  , array[])
    , (0xaf3803348f4f1f527a8b6f611c30c8702bacd2af, 'Wintermute'     , true  , array[])
    , (0xdcdf16a03360d4971ca4c1fd9967a47125f3c995, 'Rizzolver'      , true  , array[])
    , (0x05d18b713dab812c34edb48c76cd9c836d56752b, 'Propeller Swap' , true  , array[])
    , (0x1113db6080ea2b9f92b2e9937ea712b3d730b3f1, 'Clipper'        , true  , array[])
    , (0xa8be6b2afe6e060985675675615c2108a66135c8, 'Alipo'          , true  , array[])
    , (0xd3eeebc3f13532fa4bccab0275da36a413ad101b, 'TrustedVolumes' , true  , array[])
    , (0x5721898d36b15e19441ee2a403098ca187c92bb6, 'Flowmatic'      , true  , array[])
    , (0xf9c4af1c4d97260c6f0cc370046ecc2f87c81cca, 'WOWMAX'         , true  , array[])
    , (0xd6ff6abb93ef058a474769f0d05c7fef440920f8, 'Swaap Labs'     , true  , array[])
    , (0x857851ee6e398651cb7c72462cc7ce2a94d8f1c6, 'Lumia'          , true  , array[])
    , (0x98e4428508511774d0164c826003eacd8cdc9b77, 'Lumia'          , true  , array[68])
    , (0x7777777777b90a790898805f90069ec55fe93a73, 'NuConstruct'    , true  , array[])
    , (0xdeb65e00cc96af530e2ae2407eaa73f6a345687c, 'GSR'            , true  , array[])
    , (0x9cb8d9bae84830b7f5f11ee5048c04a80b8514ba, 'SwapNet'        , true  , array[])
    , (0xc6093fd9cc143f9f058938868b2df2daf9a91d28, 'Keystone'       , true  , array[21])
    , (0xe76706d6cd96cba98c783dfbd0f6de4c0ec32278, 'JPEG Trading'   , true  , array[52,67])
    , (0x451244d9e1b08e7c2340351d980aaf775d8484d8, 'AlgoLabs'       , true  , array[25])
    , (0xbc33a1f908612640f2849b56b67a4de4d179c151, 'Apollo Labs'    , true  , array[])
    , (0xbeeb41b6df0fa04b91a817108c42d7978ba67fff, 'Kipseli Capital', true  , array[39,57,72,84])
    , (0xbe90a38de6d3fd55b32f4b8ca28bd015c6b6d24c, 'Quadratic Lab'  , true  , array[])
    , (0xa6219c7d74edeb12d74a3c664f7aaeb7d01ab902, '[name]'         , false , array[])
    , (0x74c629c4096e234029c78c7760dc0aadb717adb0, '[name]'         , false , array[])
    , (0x7c7047337995c338c1682f12bc38d4e4108309bb, '[name]'         , false , array[])
    , (0x685018ea5c682c5e6d9e4116193f02018f306255, '[name]'         , false , array[])
    , (0x995f47734ec1b19baad65944504d71362a502daa, '[name]'         , false , array[51])
    , (0x0000000000004f00f577a9bec8d29b13c99bb726, '[name]'         , false , array[65])
)

, access_tokens as (
    select
        cast(null as varbinary) as address
        , '[access token]' as name
        , cast(null as boolean) as kyc
        , array_except(minted, defined) as access_token_ids
    from (
        select array_agg(distinct tokenId) as minted
        from ({% for contract, contract_data in oneinch_meta_cfg_macro()['contracts'].items() if contract_data['type'] == 'AccessToken' %}
            -- {{ contract }} --
            {%- for blockchain in contract_data['blockchains'] %}
                select tokenId from {{ source('oneinch_' + blockchain, contract + '_evt_transfer') }}
                {% if not loop.last %}union all{% endif -%}
            {%- endfor %}
            {% if not loop.last %}union all{% endif %}
        {% endfor %})
    ), (select array_agg(distinct id) as defined from names, unnest(access_token_ids) as ids(id))
)

-- output --

select
    coalesce(address, 0x) as address
    , coalesce(name, concat_ws('…', cast(substr(address, 1, 2) as varchar), substr(cast(address as varchar), 39))) as name
    , status
    , status_at
    , kyc
    , array_sort(access_token_ids) as access_token_ids
from registrations
full join (
    select * from names
    union all
    select * from access_tokens
) using(address)
order by 3 nulls last, 2, 1