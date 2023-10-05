{{
  config(
  	tags=['legacy', 'remove'],
    alias=alias('price_feeds_oracle_token_mapping', legacy_model=True),
    post_hook='{{ expose_spells(\'["polygon"]\',
                                "project",
                                "chainlink",
                                \'["msilb7","0xroll","linkpool_ryan"]\') }}'
  )
}}

SELECT "polygon"                             AS blockchain,
       feed_name,
       LOWER(proxy_address)                  AS proxy_address,
       LOWER(underlying_token_address)       AS underlying_token_address,
       CAST( extra_decimals AS BIGINT)      AS extra_decimals
FROM (values
        ("EUR / USD","0x73366fe0aa0ded304479862808e02506fe556a98","0x4e3decbb3645551b8a19f0ea1678079fcb33fb4c",0),
        ("GBP / USD","0x099a2540848573e94fb1ca0fa420b00acbbc845a","0x767058f11800fba6a682e73a6e79ec5eb74fac8c",0),
        ("CHF / USD","0xc76f762cedf0f78a439727861628e0fdfe1e70c2","0xbd1463f02f61676d53fd183c2b19282bff93d099",0),
        ("SGD / USD","0x8ce3cac0e6635ce04783709ca3cc4f5fc5304299","0xa926db7a4cc0cb1736d5ac60495ca8eb7214b503",0),
        ("PHP / USD","0x218231089bebb2a31970c3b77e96ecfb3ba006d1","0x486880fb16408b47f928f472f57bec55ac6089d1",0),
        ("CAD / USD","0xaca44abb8b04d07d883202f99fa5e3c53ed57fb5","0x8ca194a3b22077359b5732de53373d4afc11dee3",0),
        ("JPY / USD","0xd647a6fc9bc6402301583c91decc5989d8bc382d","0x8343091f2499fd4b6174a46d067a920a3b851ff9",0),
        ("COP / USD","0xde6302dfa0ac45b2b1b1a23304469da630b2f59b","0xe6d222caaed5f5dd73a9713ac91c95782e80acbf",0),
        ("SEK / USD","0xbd92b4919ae82be8473859295def0e778a626302","0x197e5d6ccff265ac3e303a34db360ee1429f5d1a",0),
        ("AUD / USD","0x062df9c4efd2030e243ffcc398b652e8b8f95c6f","0xcb7f1ef7246d1497b985f7fc45a1a31f04346133",0),
        ("CNY / USD","0x04bb437aa63e098236fa47365f0268547f6eab32","0x84526c812d8f6c4fd6c1a5b68713aff50733e772",0),
        ("KRW / USD","0x24b820870f726da9b0d83b0b28a93885061dbf50","0xa22f6bc96f13bcc84df36109c973d3c0505a067e",0),
        ("MXN / USD","0x171b16562ea3476f5c61d1b8dad031dba0768545","0xbd1fe73e1f12bd2bc237de9b626f056f21f86427",0),
        ("NGN / USD","0x0df812c4d675d155815b1216ce1da9e68f1b7050","0x182c76e977161f703bb8f111047df6c43cfacc56",0),
        ("NZD / USD","0xa302a0b8a499fd0f00449df0a490dede21105955","0x6b526daf03b4c47af2bcc5860b12151823ff70e0",0),
        ("PLN / USD","0xb34bce11040702f71c11529d00179b2959bce6c0","0x08e6d1f0c4877ef2993ad733fc6f1d022d0e9dbf",0),
        ("XAU / USD","0x0C466540B2ee1a31b441671eac0ca886e051E410","0x192ef3fff1708456d3a1f21354fa8d6bfd86b45c",0),
        ("BRL / USD","0xB90DA3ff54C3ED09115abf6FbA0Ff4645586af2c","0xf2f77fe7b8e66571e0fca7104c4d670bf1c8d722",0)
) a (feed_name, proxy_address, underlying_token_address, extra_decimals)