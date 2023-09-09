{{
  config(
	  tags=['legacy', 'remove'],
    alias=alias('price_feeds_oracle_addresses', legacy_model=True),
    post_hook='{{ expose_spells(\'["polygon"]\',
                                "project",
                                "chainlink",
                                \'["msilb7","0xroll","linkpool_ryan"]\') }}'
  )
}}

SELECT "polygon"                    AS blockchain,
       feed_name,
       CAST(decimals AS BIGINT)    AS decimals,
       LOWER(proxy_address)         AS proxy_address,
       LOWER(aggregator_address)    AS aggregator_address
FROM (values
        ("EUR / USD",8,"0x73366fe0aa0ded304479862808e02506fe556a98","0x310990e8091b5cf083fa55f500f140cfbb959016"),
        ("GBP / USD",8,"0x099a2540848573e94fb1ca0fa420b00acbbc845a","0x3f7f90e0f782e325401f6323ba93e717f519f382"),
        ("CHF / USD",8,"0xc76f762cedf0f78a439727861628e0fdfe1e70c2","0x8123beacb5bca3afa0c9ff71b28549d58cec8176"),
        ("SGD / USD",8,"0x8ce3cac0e6635ce04783709ca3cc4f5fc5304299","0x45ede0ea5cbbe380c663c7c3015cc7c986669fec"),
        ("PHP / USD",8,"0x218231089bebb2a31970c3b77e96ecfb3ba006d1","0x8a2355ec4678186164dc17dfc2c5d0d083d7fd66"),
        ("CAD / USD",8,"0xaca44abb8b04d07d883202f99fa5e3c53ed57fb5","0x88b79bfce730bbb74f23ab8940b37b86859caa2e"),
        ("JPY / USD",8,"0xd647a6fc9bc6402301583c91decc5989d8bc382d","0xeaf35f06410014234bee87980a902c21f78cb426"),
        ("COP / USD",8,"0xde6302dfa0ac45b2b1b1a23304469da630b2f59b","0xe74eb858c9dc7d013ce1392468ea1161e8c75fbd"),
        ("SEK / USD",8,"0xbd92b4919ae82be8473859295def0e778a626302","0x542d2af7f89a61205f3da2d3d13e29b56bde7b46"),
        ("AUD / USD",8,"0x062df9c4efd2030e243ffcc398b652e8b8f95c6f","0x0a9823c5cd84099fde8566a1adf0f2bb41cc6e7d"),
        ("CNY / USD",8,"0x04bb437aa63e098236fa47365f0268547f6eab32","0xf07eac7a48eb772613479d6a8fc42675f1befb47"),
        ("KRW / USD",8,"0x24b820870f726da9b0d83b0b28a93885061dbf50","0xfd54f97a6c408561b5df798c04ae08b27ca0d7f7"),
        ("MXN / USD",8,"0x171b16562ea3476f5c61d1b8dad031dba0768545","0x2e2ed40fc4f1774def278830f8fe3b6e77956ec8"),
        ("NGN / USD",8,"0x0df812c4d675d155815b1216ce1da9e68f1b7050","0x0000000000000000000000000000000000000000"),
        ("NZD / USD",8,"0xa302a0b8a499fd0f00449df0a490dede21105955","0xe63032a70f6eb617970829fbfa365d7c44bdbbbf"),
        ("PLN / USD",8,"0xb34bce11040702f71c11529d00179b2959bce6c0","0x08f8d217e6f07ae423a2ad2ffb226ffcb577708d"),
        ("XAU / USD",8,"0x0C466540B2ee1a31b441671eac0ca886e051E410","0x704179beb09282eaef98ca8aaa443c1e273ebbc2"),
        ("BRL / USD",8,"0xB90DA3ff54C3ED09115abf6FbA0Ff4645586af2c","0x6dbd1be1a83005d26b582d61937b406300b05a8f")
) a (feed_name, decimals, proxy_address, aggregator_address)