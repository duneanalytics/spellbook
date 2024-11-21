 {{
  config(
        schema = 'solana_utils',
        alias = 'sns_domains',
        materialized='table',
        
        post_hook='{{ expose_spells(\'["solana"]\',
                                    "sector",
                                    "solana_utils",
                                    \'["ilemi"]\') }}')
}}

with 
    names as (
        SELECT 
            trim(from_utf8(bytearray_substring(data
                , 6 --start from discrim (1 byte) + length (4 bytes) => 6th byte start
                ,bytearray_to_bigint(bytearray_reverse(bytearray_substring(data,2,4))) --strings are vec<u32> so we get the first 4 bytes for length then convert binary string to utf8
                ))) || '.sol' as string_utf8
            , tx_id
            , block_time
            , tx_signer as owner
            , case when bytearray_substring(data,1,1) IN (0x0d, 0x00, 0x11) then account_arguments[3]
                when bytearray_substring(data,1,1) = 0x01 then account_arguments[5]
                when bytearray_substring(data,1,1) = 0x09 then account_arguments[4]
                end as domain_account
                  --0x00 https://solscan.io/tx/4hFMPcncHeTa4SUCqAr8NWbE7NaAGvz7F8LNVbuieJuHCVvewkuZpFkPZxXezfsBKWiKHfa1rJoviTrSrDiUYNG6
                  --0x01 https://solscan.io/tx/4776cgKwMaHuF1gZjPd5UXqTu1ChjZWh9gVDh3A96PmGzfW49RFvDo2ToBHHEqUmmxx6jgXD3UxeM3T7RxkHUJuC
                  --0x09 https://solscan.io/tx/3KYM9GFVoBiS2da1rk55GLiXgqEcriFHsCtJY7JtQZKuwCqYrYVBXU4Fh7EUq35EfzmqhnR4GLHH4ABmduWUHMyv
                  --0x0d https://solscan.io/tx/28Wr9Tg83FwWTJNXMLT561ZDd7sqo29Z9dCeWJhvuWeNFtkAQvnF7ajGyL7haAicEmKozTaWGPiyorWNNpAhupc6
                  --0x11 https://solscan.io/tx/325dGEvxPLdMJgQH62ea89EJcaRSEuJtrd761Xhxadj1ZtirPhJmLvgx2ENaSodU74tXwzQk8ShvFsZQ1qw3T53E
        FROM {{ source('solana','instruction_calls') }}
        WHERE executing_account = 'jCebN34bUfdeUYJT13J1yG16XWQpt5PDx6Mse9GUqhR'
        and bytearray_substring(data,1,1) IN (0x01, 0x09, 0x0d, 0x00, 0x11) --different register instructions
    )
    
    , transfers as (
        --namesLPneVptA9Z5rqUDD9tMTWEJwofgaYwp8cawRkX executing_account to transfer
        SELECT
            toBase58(bytearray_substring(data,2,32)) as owner
            , account_arguments[1] as domain_account
            , block_time
            , row_number() OVER (partition by account_arguments[1] order by block_time desc) as latest
            , tx_id
        FROM {{ source('solana','instruction_calls') }}
        WHERE executing_account = 'namesLPneVptA9Z5rqUDD9tMTWEJwofgaYwp8cawRkX'
        and bytearray_substring(data,1,1) = 0x02
    )
    
    , latest_holders as (
        SELECT 
            n.domain_account
            , n.string_utf8
            , COALESCE(t.owner, n.owner) as owner
            , n.block_time as registered_at
            , t.block_time as transferred_at
        FROM names n
        LEFT JOIN transfers t ON t.domain_account = n.domain_account AND t.latest = 1
    )
    
    , favorites as (
        --example transaction: https://solscan.io/tx/24UqDsCzqATuA4LMf83b1MC77VfQvJqfeFVZF653ezsBCmYwNifU9SMMRD4u4CCzGJf4fAfbyrsG7cFjCzFVFypJ
        SELECT 
            distinct
            f.owner
            , f.domain_account as fav_domain_account
            , n.domain_account
            , n.string_utf8 as string_utf8
        FROM (
        SELECT
            account_arguments[3] as owner
            , account_arguments[1] as domain_account
            , account_arguments
            , block_time
            , tx_id
            , row_number() OVER (partition by account_arguments[3] order by block_time desc) as latest_by_favorite
            , row_number() OVER (partition by account_arguments[1] order by block_time desc) as latest_by_domain
        FROM {{ source('solana','instruction_calls') }}
        WHERE executing_account = '85iDfUvr3HJyLM2zcq5BXSiDvUWfw6cSE1FfNBo8Ap29'
        and bytearray_substring(data,1,1) = 0x06
        ) f
        LEFT JOIN names n ON n.domain_account = f.domain_account
        WHERE latest_by_favorite = 1
        ANd latest_by_domain = 1 
    )
    
SELECT 
COALESCE(h.owner, f.owner) as owner 
, f.string_utf8 as favorite_domain
, array_agg(distinct h.string_utf8) as domains_owned
FROM latest_holders h
FULL OUTER JOIN favorites f ON f.owner = h.owner
group by 1,2