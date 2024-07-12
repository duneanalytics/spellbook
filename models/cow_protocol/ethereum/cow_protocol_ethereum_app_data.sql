{{ config(alias='app_data',
        
        post_hook='{{ expose_spells(\'["ethereum"]\',
                                    "project",
                                    "cow_protocol",
                                    \'["olgafetisova"]\') }}'
)}}

-- Find the PoC Query here: https://dune.com/queries/3913360
with
 results as (
    select
        contract_app_data as app_hash,
        CAST(json_extract(encode, '$.appCode')  AS VARCHAR) as app_code,
        CAST(json_extract(encode, '$.environment')  AS VARCHAR) as environment,
        CAST(json_extract(encode, '$.metadata.orderClass.orderClass')  AS VARCHAR) as  order_class,
        LOWER(cast(COALESCE(json_extract(encode, '$.metadata.referrer.address'),json_extract(encode, '$.metadata.referrer.referrer')) as varchar)) AS referrer,
        CAST(json_extract(encode, '$.metadata.partnerFee.bps')  AS double) as partner_bps,
        CAST(json_extract(encode, '$.metadata.partnerFee.recipient')  AS VARCHAR) as partner_recipient,
        CAST(json_extract(encode, '$.metadata.quote.slippageBips')  AS DOUBLE) as slippage_bips,
        json_extract(encode, '$.metadata.utm') as utm,
        CAST(json_extract(encode, '$.metadata.utm.utmSource')  AS VARCHAR) as utm_source,
        CAST(json_extract(encode, '$.metadata.utm.utmMedium')  AS VARCHAR) as utm_medium,
        CAST(json_extract(encode, '$.metadata.utm.utmContent')  AS VARCHAR) as utm_content,
        CAST(json_extract(encode, '$.metadata.utm.utmCampaign')  AS VARCHAR) as utm_campaign,
        CAST(json_extract(encode, '$.metadata.utm.utmTerm')  AS VARCHAR) as utm_term,
        CAST(json_extract(encode, '$.metadata.utm.utmCampaign')  AS VARCHAR) as utm_campaign,
        CAST(json_extract(encode, '$.metadata.widget.appCode')  AS VARCHAR) as widget_app_code,
        CAST(json_extract(encode, '$.metadata.widget.environment')  AS VARCHAR) as widget_environment

    from {{ source('dune_upload', 'dataset_app_data_mainnet') }}
    where contract_app_data!=0x0a7bcea9eec07f1634f0adfcc43c00cb391f16aef78f6422eac1203fb997a12a
)
select * from results
