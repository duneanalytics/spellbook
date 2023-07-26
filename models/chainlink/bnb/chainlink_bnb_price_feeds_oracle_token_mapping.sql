{{
  config(
    tags=['dunesql'],
    alias=alias('price_feeds_oracle_token_mapping'),
    post_hook='{{ expose_spells(\'["bnb"]\',
                                "project",
                                "chainlink",
                                \'["msilb7","0xroll","linkpool_ryan"]\') }}'
  )
}}

SELECT
  'bnb' as blockchain,
   feed_name,
   proxy_address,
   underlying_token_address,
   CAST(extra_decimals AS BIGINT) as extra_decimals
FROM (values
  ('EUR / USD',0x0bf79f617988c472dca68ff41efe1338955b9a80,0x23b8683ff98f9e4781552dfe6f12aa32814924e8,0),
  ('GBP / USD',0x8faf16f710003e538189334541f5d4a391da46a0,0x048e9b1ddf9ebbb224812372280e94ccac443f9e,0),
  ('CHF / USD',0x964261740356cb4aad0c3d2003ce808a4176a46d,0x7c869b5a294b1314e985283d01c702b62224a05f,0),
  ('ZAR / USD',0xde1952a1bf53f8e558cc761ad2564884e55b2c6f,0x6b8b9ae0627a7622c593a1696859ca753c61a670,0),
  ('BRL / USD',0x5cb1cb3ea5fb46de1ce1d0f3badb3212e8d8ef48,0x316622977073bbc3df32e7d2a9b3c77596a0a603,0)
) a (feed_name, proxy_address, underlying_token_address, extra_decimals)