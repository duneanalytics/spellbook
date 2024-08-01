{{ config(alias='app_data',
        
        post_hook='{{ expose_spells(\'["ethereum"]\',
                                    "project",
                                    "cow_protocol",
                                    \'["bh2smith"]\') }}'
)}}

-- Find the PoC Query here: https://dune.com/queries/1751965
with
  partially_unpacked_app_content as (
    select
        distinct from_hex(app_hash) as app_hash,
        content.appCode as app_code,
        content.environment,
        content.metadata.orderClass.orderClass as order_class,
        content.metadata.partnerFee as partner_fee,
        content.metadata.quote,
        content.metadata.referrer,
        content.metadata.utm,
        content.metadata.widget
    from {{ source('cowswap', 'raw_app_data') }}
  ),
  unpacked_referrer_app_data as (
    select
        app_hash,
        app_code,
        environment,
        order_class,
        partner_fee,
        quote,
        -- different app data versions put referrer in two possible places.
        from_hex(coalesce(referrer.address, referrer.referrer)) as referrer,
        utm,
        widget
    from partially_unpacked_app_content
  ),
  results as (
    select
        app_hash,
        app_code,
        environment,
        order_class,
        referrer,
        partner_fee.bps as partner_fee_bps,
        partner_fee.recipient as partner_recipient,
        cast(quote.slippageBips as integer) as slippage_bips,
        utm,
        utm.utmSource as utm_source,
        utm.utmMedium as utm_medium,
        utm.utmContent as utm_content,
        utm.utmCampaign as utm_campaign,
        utm.utmTerm as utm_term,
        widget.appCode as widget_app_code,
        widget.environment as widget_environment
    from unpacked_referrer_app_data
)
select * from results
