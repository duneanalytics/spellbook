{{ config(alias='app_data',
        
        post_hook='{{ expose_spells(\'["gnosis"]\',
                                    "project",
                                    "cow_protocol",
                                    \'["olgafetisova"]\') }}'
)}}

-- Find the PoC Query here: https://dune.com/queries/3909628
with
 results as (
    select
        contract_app_data as app_hash,
        CAST(json_extract(full_app_data, '$.appCode')  AS VARCHAR) as app_code,
        CAST(json_extract(full_app_data, '$.environment')  AS VARCHAR) as environment,
        CAST(json_extract(full_app_data, '$.metadata.orderClass.orderClass')  AS VARCHAR) as  order_class,
        LOWER(cast(COALESCE(json_extract(full_app_data, '$.metadata.referrer.address'),json_extract(full_app_data, '$.metadata.referrer.referrer')) as varchar)) AS referrer,
        CAST(json_extract(full_app_data, '$.metadata.partnerFee.bps')  AS double) as partner_bps,
        CAST(json_extract(full_app_data, '$.metadata.partnerFee.recipient')  AS VARCHAR) as partner_recipient,
        CAST(json_extract(full_app_data, '$.metadata.quote.slippageBips')  AS DOUBLE) as slippage_bips,
        json_extract(full_app_data, '$.metadata.utm') as utm,
        CAST(json_extract(full_app_data, '$.metadata.utm.utmSource')  AS VARCHAR) as utm_source,
        CAST(json_extract(full_app_data, '$.metadata.utm.utmMedium')  AS VARCHAR) as utm_medium,
        CAST(json_extract(full_app_data, '$.metadata.utm.utmContent')  AS VARCHAR) as utm_content,
        CAST(json_extract(full_app_data, '$.metadata.utm.utmCampaign')  AS VARCHAR) as utm_campaign,
        CAST(json_extract(full_app_data, '$.metadata.utm.utmTerm')  AS VARCHAR) as utm_term,
        CAST(json_extract(full_app_data, '$.metadata.utm.utmCampaign')  AS VARCHAR) as utm_campaign,
        CAST(json_extract(full_app_data, '$.metadata.widget.appCode')  AS VARCHAR) as widget_app_code,
        CAST(json_extract(full_app_data, '$.metadata.widget.environment')  AS VARCHAR) as widget_environment

    from {{ source('cowprotocol', 'dataset_app_data_gnosis') }}
)
select * from results