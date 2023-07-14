{{
  config(
  	tags=['legacy'],
    alias=alias('price_feeds_oracle_addresses', legacy_model=True),
    post_hook='{{ expose_spells(\'["bnb"]\',
                                "project",
                                "chainlink",
                                \'["msilb7","0xroll","linkpool_ryan"]\') }}'
  )
}}

SELECT "bnb"                    AS blockchain,
       feed_name,
       CAST(decimals AS BIGINT)    AS decimals,
       LOWER(proxy_address)         AS proxy_address,
       LOWER(aggregator_address)    AS aggregator_address
FROM (values
        ("EUR / USD",8,"0x0bf79f617988c472dca68ff41efe1338955b9a80","0xd2528b74ca91bb07b9bd9685ce533367c6fa657c"),
        ("GBP / USD",8,"0x8faf16f710003e538189334541f5d4a391da46a0","0xe0a34b8fc5e80c877fd568bd22b49e1bca977b6f"),
        ("CHF / USD",8,"0x964261740356cb4aad0c3d2003ce808a4176a46d","0x6c9c9757f0478bc38bf73abda27ac42864de0645"),
        ("ZAR / USD",8,"0xde1952a1bf53f8e558cc761ad2564884e55b2c6f","0x86eb1bb8c66f365ea3df12a565a46cea204f6283"),
        ("BRL / USD",8,"0x5cb1cb3ea5fb46de1ce1d0f3badb3212e8d8ef48","0x2f92dc0711ada3dc255e2197e7c15c8adc6b6537")
) a (feed_name, decimals, proxy_address, aggregator_address)