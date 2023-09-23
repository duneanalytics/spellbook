{{
    config(
        schema = 'oneinch',
        alias = alias('fusion_resolvers'),
        materialized = 'table',
        file_format = 'delta',
        unique_key = ['address', 'last_changed_at'],
        tags = ['dunesql']
    )
}}



{% set project_start_date = "timestamp '2022-12-25'" %} 



with
        
names(
    address
    , name
    , kyc
) as (
    values
          (0xf63392356a985ead50b767a3e97a253ff870e91a, '1inch Labs'     , true  )
        , (0xa260f8b7c8f37c2f1bc11b04c19902829de6ac8a, 'Arctic Bastion' , true  )
        , (0xcfa62f77920d6383be12c91c71bd403599e1116f, 'The Open DAO'   , true  )
        , (0xe023f53f735c196e4a028233c2ee425957812a41, 'Seawise'        , true  )
        , (0x754bcbaf851f94ca0065d0d06d53b168daab17b8, 'Alpha'          , true  )
        , (0xb2153caa185484fd377f488d89143a7fd76695ce, 'Laertes'        , true  )
        , (0xc975671642534f407ebdcaef2428d355ede16a2c, 'OTEX'           , true  )
        , (0xd7f6f541d4210550ca56f7b4c4a549efd4cafb49, 'The T'          , true  )
        , (0x21b7db78e76dcd100f717206ee655daab2de118c, 'Spider Labs'    , true  )
        , (0x12e5ceb5c14f3a1a9971da154f6530c1cf7274ac, 'Rosato LLC'     , true  )
        , (0xee230dd7519bc5d0c9899e8704ffdc80560e8509, 'Kinetex Labs'   , true  )
        , (0xaf3803348f4f1f527a8b6f611c30c8702bacd2af, 'Wintermute'     , true  )
        , (0xdcdf16a03360d4971ca4c1fd9967a47125f3c995, 'Rizzolver'      , true  ) -- Wintermute
        , (0x05d18b713dab812c34edb48c76cd9c836d56752b, 'Propeller Swap' , true  )
        , (0x1113db6080ea2b9f92b2e9937ea712b3d730b3f1, 'Clipper'        , true  )
        , (0xa6219c7d74edeb12d74a3c664f7aaeb7d01ab902, ''               , false )
        , (0x74c629c4096e234029c78c7760dc0aadb717adb0, ''               , false )
        , (0x7c7047337995c338c1682f12bc38d4e4108309bb, ''               , false )
        , (0x685018ea5c682c5e6d9e4116193f02018f306255, ''               , false )
)

, registrations as (
    select
        substr(data, 13, 20) as address
        , max_by(
            case topic0
                when 0x2d3734a8e47ac8316e500ac231c90a6e1848ca2285f40d07eaa52005e4b3a0e9 then 'Registered'
                when 0x75cd6de711483e11488a1cd9b66172abccb9e5c19572f92015a7880f0c8c0edc then 'Unregistered'
            end
            , block_time
        ) as status
        , max(block_time) as last_changed_at
    from {{ source('ethereum', 'logs') }}
    where
        contract_address in (0xcb8308fcb7bc2f84ed1bea2c016991d34de5cc77, 0xF55684BC536487394B423e70567413faB8e45E26) -- WhitelistRegistry
        and topic0 in (0x2d3734a8e47ac8316e500ac231c90a6e1848ca2285f40d07eaa52005e4b3a0e9, 0x75cd6de711483e11488a1cd9b66172abccb9e5c19572f92015a7880f0c8c0edc)
        and block_time >= {{ project_start_date }}
    group by 1
)


select
      address
    , if(name = '', '0x' || lower(to_hex(substr(address, 1, 2))) || '..' || lower(to_hex(substr(address, 19))), coalesce(name, 'UNSPECIFIED')) as name
    , status
    , last_changed_at
    , kyc
from registrations
left join names using(address)
order by 3, 2, 1