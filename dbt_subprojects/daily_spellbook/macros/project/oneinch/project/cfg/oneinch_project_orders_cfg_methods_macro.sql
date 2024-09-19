{% macro oneinch_project_orders_cfg_methods_macro() %}


-- designing methods

{% set methods = [] %}

-- ZeroEx --

{% set _beginning = "4 + bytearray_to_bigint(substr(input, 4 + 24 + 1, 8))" %}
{% set _maker_data = "bytearray_to_bigint(substr(input, " ~ _beginning ~ " + 32*10 + 24 + 1, 8))" %}
{% set _taker_data = "bytearray_to_bigint(substr(input, " ~ _beginning ~ " + 32*11 + 24 + 1, 8))" %}
{% set methods = methods + [{
    "project":          "ZeroEx",
    "selector":         "0xb4be83d5",
    "name":             "fillOrder",
    "event":            "0x0bcc4c97732e47d9946f229edb95f5b6323f601300e4690de719993f3c371129",
    "maker":            "substr(input   , 4 + 32*3 + 12 + 1         , 20)",
    "maker_asset":      "substr(input   , " ~ _beginning ~ " + " ~ _maker_data ~ " + 32*1 + 4 + 12 + 1, 20)",
    "taker_asset":      "substr(input   , " ~ _beginning ~ " + " ~ _taker_data ~ " + 32*1 + 4 + 12 + 1, 20)",
    "maker_max_amount": "substr(input   , 4 + 32*7 + 1              , 32)",
    "taker_max_amount": "substr(input   , 4 + 32*1 + 1              , 32)",
    "making_amount":    "substr(output  , 32*0 + 1                  , 32)",
    "taking_amount":    "substr(output  , 32*1 + 1                  , 32)",
}] %}
{% set methods = methods + [{
    "project":          "ZeroEx",
    "selector":         "0x9b44d556",
    "name":             "fillOrder",
    "event":            "0x6869791f0a34781b29882982cc39e882768cf2c96995c2a110c577c53bc932d5",
    "maker":            "substr(input   , 4 + 32*3 + 12 + 1         , 20)",
    "maker_asset":      "substr(input   , " ~ _beginning ~ " + " ~ _maker_data ~ " + 32*1 + 4 + 12 + 1, 20)",
    "taker_asset":      "substr(input   , " ~ _beginning ~ " + " ~ _taker_data ~ " + 32*1 + 4 + 12 + 1, 20)",
    "maker_max_amount": "substr(input   , 4 + 32*7 + 1              , 32)",
    "taker_max_amount": "substr(input   , 4 + 32*8 + 1              , 32)",
    "making_amount":    "substr(output  , 32*0 + 1                  , 32)",
    "taking_amount":    "substr(output  , 32*1 + 1                  , 32)",
}] %}
{% set methods = methods + [{
    "project":          "ZeroEx",
    "selector":         "0x64a3bc15",
    "name":             "fillOrKillOrder",
    "event":            "0x0bcc4c97732e47d9946f229edb95f5b6323f601300e4690de719993f3c371129",
    "maker":            "substr(input   , 4 + 32*3 + 12 + 1         , 20)",
    "maker_asset":      "substr(input   , " ~ _beginning ~ " + " ~ _maker_data ~ " + 32*1 + 4 + 12 + 1, 20)",
    "taker_asset":      "substr(input   , " ~ _beginning ~ " + " ~ _taker_data ~ " + 32*1 + 4 + 12 + 1, 20)",
    "maker_max_amount": "substr(input   , 4 + 32*7 + 1              , 32)",
    "taker_max_amount": "substr(input   , 4 + 32*1 + 1              , 32)",
    "making_amount":    "substr(output  , 32*0 + 1                  , 32)",
    "taking_amount":    "substr(output  , 32*1 + 1                  , 32)",
}] %}
{% set methods = methods + [{
    "project":          "ZeroEx",
    "selector":         "0x3e228bae",
    "name":             "fillOrderNoThrow",
    "event":            "0x0bcc4c97732e47d9946f229edb95f5b6323f601300e4690de719993f3c371129",
    "maker":            "substr(input   , 4 + 32*3 + 12 + 1         , 20)",
    "maker_asset":      "substr(input   , " ~ _beginning ~ " + " ~ _maker_data ~ " + 32*1 + 4 + 12 + 1, 20)",
    "taker_asset":      "substr(input   , " ~ _beginning ~ " + " ~ _taker_data ~ " + 32*1 + 4 + 12 + 1, 20)",
    "maker_max_amount": "substr(input   , 4 + 32*7 + 1              , 32)",
    "taker_max_amount": "substr(input   , 4 + 32*1 + 1              , 32)",
    "making_amount":    "substr(output  , 32*0 + 1                  , 32)",
    "taking_amount":    "substr(output  , 32*1 + 1                  , 32)",
}] %}
{% set methods = methods + [{
    "project":          "ZeroEx",
    "selector":         "0xf6274f66",
    "name":             "fillLimitOrder",
    "event":            "0xab614d2b738543c0ea21f56347cf696a3a0c42a7cbec3212a5ca22a4dcff2124",
    "maker":            "substr(input   , 4 + 32*5 + 12 + 1         , 20)",
    "maker_asset":      "substr(input   , 4 + 32*0 + 12 + 1         , 20)",
    "taker_asset":      "substr(input   , 4 + 32*1 + 12 + 1         , 20)",
    "maker_max_amount": "substr(input   , 4 + 32*2 + 1              , 32)",
    "taker_max_amount": "substr(input   , 4 + 32*3 + 1              , 32)",
    "making_amount":    "substr(output  , 32*1 + 1                  , 32)",
    "taking_amount":    "substr(output  , 32*0 + 1                  , 32)",
}] %}
{% set methods = methods + [{
    "project":          "ZeroEx",
    "selector":         "0x414e4ccf",
    "name":             "_fillLimitOrder",
    "event":            "0xab614d2b738543c0ea21f56347cf696a3a0c42a7cbec3212a5ca22a4dcff2124",
    "maker":            "substr(input   , 4 + 32*5 + 12 + 1         , 20)",
    "maker_asset":      "substr(input   , 4 + 32*0 + 12 + 1         , 20)",
    "taker_asset":      "substr(input   , 4 + 32*1 + 12 + 1         , 20)",
    "maker_max_amount": "substr(input   , 4 + 32*2 + 1              , 32)",
    "taker_max_amount": "substr(input   , 4 + 32*3 + 1              , 32)",
    "making_amount":    "substr(output  , 32*1 + 1                  , 32)",
    "taking_amount":    "substr(output  , 32*0 + 1                  , 32)",
}] %}
{% set methods = methods + [{
    "project":          "ZeroEx",
    "selector":         "0x9240529c",
    "name":             "fillOrKillLimitOrder",
    "event":            "0xab614d2b738543c0ea21f56347cf696a3a0c42a7cbec3212a5ca22a4dcff2124",
    "maker":            "substr(input   , 4 + 32*5 + 12 + 1         , 20)",
    "maker_asset":      "substr(input   , 4 + 32*0 + 12 + 1         , 20)",
    "taker_asset":      "substr(input   , 4 + 32*1 + 12 + 1         , 20)",
    "maker_max_amount": "substr(input   , 4 + 32*2 + 1              , 32)",
    "taker_max_amount": "substr(input   , 4 + 32*3 + 1              , 32)",
    "making_amount":    "substr(output  , 32*0 + 1                  , 32)",
}] %}
{% set methods = methods + [{
    "project":          "ZeroEx",
    "selector":         "0xdac748d4",
    "name":             "fillOtcOrder",
    "event":            "0xac75f773e3a92f1a02b12134d65e1f47f8a14eabe4eaf1e24624918e6a8b269f",
    "maker":            "substr(input   , 4 + 32*4 + 12 + 1         , 20)",
    "maker_asset":      "substr(input   , 4 + 32*0 + 12 + 1         , 20)",
    "taker_asset":      "substr(input   , 4 + 32*1 + 12 + 1         , 20)",
    "maker_max_amount": "substr(input   , 4 + 32*2 + 1              , 32)",
    "taker_max_amount": "substr(input   , 4 + 32*3 + 1              , 32)",
    "making_amount":    "substr(output  , 32*1 + 1                  , 32)",
    "taking_amount":    "substr(output  , 32*0 + 1                  , 32)",
}] %}
{% set methods = methods + [{
    "project":          "ZeroEx",
    "selector":         "0xe4ba8439",
    "name":             "_fillOtcOrder",
    "event":            "0xac75f773e3a92f1a02b12134d65e1f47f8a14eabe4eaf1e24624918e6a8b269f",
    "maker":            "substr(input   , 4 + 32*4 + 12 + 1         , 20)",
    "maker_asset":      "substr(input   , 4 + 32*0 + 12 + 1         , 20)",
    "taker_asset":      "substr(input   , 4 + 32*1 + 12 + 1         , 20)",
    "maker_max_amount": "substr(input   , 4 + 32*2 + 1              , 32)",
    "taker_max_amount": "substr(input   , 4 + 32*3 + 1              , 32)",
    "making_amount":    "substr(output  , 32*1 + 1                  , 32)",
    "taking_amount":    "substr(output  , 32*0 + 1                  , 32)",
}] %}
{% set methods = methods + [{
    "project":          "ZeroEx",
    "selector":         "0xa578efaf",
    "name":             "fillOtcOrderForEth",
    "event":            "0xac75f773e3a92f1a02b12134d65e1f47f8a14eabe4eaf1e24624918e6a8b269f",
    "maker":            "substr(input   , 4 + 32*4 + 12 + 1         , 20)",
    "maker_asset":      "substr(input   , 4 + 32*0 + 12 + 1         , 20)",
    "taker_asset":      "substr(input   , 4 + 32*1 + 12 + 1         , 20)",
    "maker_max_amount": "substr(input   , 4 + 32*2 + 1              , 32)",
    "taker_max_amount": "substr(input   , 4 + 32*3 + 1              , 32)",
    "making_amount":    "substr(output  , 32*1 + 1                  , 32)",
    "taking_amount":    "substr(output  , 32*0 + 1                  , 32)",
}] %}
{% set methods = methods + [{
    "project":          "ZeroEx",
    "selector":         "0x706394d5",
    "name":             "fillOtcOrderWithEth",
    "event":            "0xac75f773e3a92f1a02b12134d65e1f47f8a14eabe4eaf1e24624918e6a8b269f",
    "maker":            "substr(input   , 4 + 32*4 + 12 + 1         , 20)",
    "maker_asset":      "substr(input   , 4 + 32*0 + 12 + 1         , 20)",
    "taker_asset":      "substr(input   , 4 + 32*1 + 12 + 1         , 20)",
    "maker_max_amount": "substr(input   , 4 + 32*2 + 1              , 32)",
    "taker_max_amount": "substr(input   , 4 + 32*3 + 1              , 32)",
    "making_amount":    "substr(output  , 32*1 + 1                  , 32)",
    "taking_amount":    "substr(output  , 32*0 + 1                  , 32)",
}] %}
{% set methods = methods + [{
    "project":          "ZeroEx",
    "selector":         "0x724d3953",
    "name":             "fillTakerSignedOtcOrderForEth",
    "event":            "0xac75f773e3a92f1a02b12134d65e1f47f8a14eabe4eaf1e24624918e6a8b269f",
    "maker":            "substr(input   , 4 + 32*4 + 12 + 1         , 20)",
    "maker_asset":      "substr(input   , 4 + 32*0 + 12 + 1         , 20)",
    "taker_asset":      "substr(input   , 4 + 32*1 + 12 + 1         , 20)",
    "maker_max_amount": "substr(input   , 4 + 32*2 + 1              , 32)",
    "taker_max_amount": "substr(input   , 4 + 32*3 + 1              , 32)",
}] %}
{% set methods = methods + [{
    "project":          "ZeroEx",
    "selector":         "0x4f948110",
    "name":             "fillTakerSignedOtcOrder",
    "event":            "0xac75f773e3a92f1a02b12134d65e1f47f8a14eabe4eaf1e24624918e6a8b269f",
    "maker":            "substr(input   , 4 + 32*4 + 12 + 1         , 20)",
    "maker_asset":      "substr(input   , 4 + 32*0 + 12 + 1         , 20)",
    "taker_asset":      "substr(input   , 4 + 32*1 + 12 + 1         , 20)",
    "maker_max_amount": "substr(input   , 4 + 32*2 + 1              , 32)",
    "taker_max_amount": "substr(input   , 4 + 32*3 + 1              , 32)",
}] %}
{% set methods = methods + [{
    "project":          "ZeroEx",
    "selector":         "0xaa77476c",
    "name":             "fillRfqOrder",
    "event":            "0x829fa99d94dc4636925b38632e625736a614c154d55006b7ab6bea979c210c32",
    "maker":            "substr(input   , 4 + 32*4 + 12 + 1         , 20)",
    "maker_asset":      "substr(input   , 4 + 32*0 + 12 + 1         , 20)",
    "taker_asset":      "substr(input   , 4 + 32*1 + 12 + 1         , 20)",
    "maker_max_amount": "substr(input   , 4 + 32*2 + 1              , 32)",
    "taker_max_amount": "substr(input   , 4 + 32*3 + 1              , 32)",
    "making_amount":    "substr(output  , 32*1 + 1                  , 32)",
    "taking_amount":    "substr(output  , 32*0 + 1                  , 32)",
}] %}
{% set methods = methods + [{
    "project":          "ZeroEx",
    "selector":         "0xaa6b21cd",
    "name":             "_fillRfqOrder",
    "event":            "0x829fa99d94dc4636925b38632e625736a614c154d55006b7ab6bea979c210c32",
    "maker":            "substr(input   , 4 + 32*4 + 12 + 1         , 20)",
    "maker_asset":      "substr(input   , 4 + 32*0 + 12 + 1         , 20)",
    "taker_asset":      "substr(input   , 4 + 32*1 + 12 + 1         , 20)",
    "maker_max_amount": "substr(input   , 4 + 32*2 + 1              , 32)",
    "taker_max_amount": "substr(input   , 4 + 32*3 + 1              , 32)",
    "making_amount":    "substr(output  , 32*1 + 1                  , 32)",
    "taking_amount":    "substr(output  , 32*0 + 1                  , 32)",
}] %}
{% set methods = methods + [{
    "project":          "ZeroEx",
    "selector":         "0xa656186b",
    "name":             "_fillRfqOrder",
    "event":            "0x829fa99d94dc4636925b38632e625736a614c154d55006b7ab6bea979c210c32",
    "maker":            "substr(input   , 4 + 32*4 + 12 + 1         , 20)",
    "maker_asset":      "substr(input   , 4 + 32*0 + 12 + 1         , 20)",
    "taker_asset":      "substr(input   , 4 + 32*1 + 12 + 1         , 20)",
    "maker_max_amount": "substr(input   , 4 + 32*2 + 1              , 32)",
    "taker_max_amount": "substr(input   , 4 + 32*3 + 1              , 32)",
    "making_amount":    "substr(output  , 32*1 + 1                  , 32)",
    "taking_amount":    "substr(output  , 32*0 + 1                  , 32)",
}] %}
{% set methods = methods + [{
    "project":          "ZeroEx",
    "selector":         "0x438cdfc5",
    "name":             "fillOrKillRfqOrder",
    "event":            "0x829fa99d94dc4636925b38632e625736a614c154d55006b7ab6bea979c210c32",
    "maker":            "substr(input   , 4 + 32*4 + 12 + 1         , 20)",
    "maker_asset":      "substr(input   , 4 + 32*0 + 12 + 1         , 20)",
    "taker_asset":      "substr(input   , 4 + 32*1 + 12 + 1         , 20)",
    "maker_max_amount": "substr(input   , 4 + 32*2 + 1              , 32)",
    "taker_max_amount": "substr(input   , 4 + 32*3 + 1              , 32)",
    "making_amount":    "substr(output  , 32*0 + 1                  , 32)",
}] %}

-- Hashflow --

{% set methods = methods + [{
    "project":          "Hashflow",
    "selector":         "0x1e9a2e92",
    "name":             "tradeSingleHop",
    "event":            "0xb709ddcc6550418e9b89df1f4938071eeaa3f6376309904c77e15d46b16066f5",
    "maker":            "substr(input   , 4 + 32*2 + 12 + 1         , 20)",
    "maker_external":   "substr(input   , 4 + 32*3 + 12 + 1         , 20)",
    "maker_asset":      "substr(input   , 4 + 32*7 + 12 + 1         , 20)",
    "taker_asset":      "substr(input   , 4 + 32*6 + 12 + 1         , 20)",
    "maker_max_amount": "substr(input   , 4 + 32*10 + 1             , 32)",
    "taker_max_amount": "substr(input   , 4 + 32*9 + 1              , 32)",
}] %}
{% set methods = methods + [{
    "project":          "Hashflow",
    "selector":         "0xf0210929",
    "name":             "tradeSingleHop",
    "event":            "0x8cf3dec1929508e5677d7db003124e74802bfba7250a572205a9986d86ca9f1e",
    "maker":            "substr(input   , 4 + 32*1 + 12 + 1         , 20)",
    "maker_external":   "substr(input   , 4 + 32*2 + 12 + 1         , 20)",
    "maker_asset":      "substr(input   , 4 + 32*6 + 12 + 1         , 20)",
    "taker_asset":      "substr(input   , 4 + 32*5 + 12 + 1         , 20)",
    "maker_max_amount": "substr(input   , 4 + 32*9 + 1              , 32)",
    "taker_max_amount": "substr(input   , 4 + 32*8 + 1              , 32)",
}] %}
{% set methods = methods + [{
    "project":          "Hashflow",
    "selector":         "0xc52ac720",
    "name":             "tradeRFQT",
    "event":            "0x34f57786fb01682fb4eec88d340387ef01a168fe345ea5b76f709d4e560c10eb",
    "maker":            "substr(input   , 4 + 32*1 + 12 + 1         , 20)",
    "maker_external":   "substr(input   , 4 + 32*2 + 12 + 1         , 20)",
    "maker_asset":      "substr(input   , 4 + 32*6 + 12 + 1         , 20)",
    "taker_asset":      "substr(input   , 4 + 32*5 + 12 + 1         , 20)",
    "maker_max_amount": "substr(input   , 4 + 32*9 + 1              , 32)",
    "taker_max_amount": "substr(input   , 4 + 32*8 + 1              , 32)",
    "order_hash":       "substr(input   , 4 + 32*12 + 1             , 32)",
}] %}

-- Native --

{% set methods = methods + [{
    "project":          "Native",
    "selector":         "0xe525b10b",
    "name":             "tradeRFQT",
    "event":            "0x32f38ef2842789f9cd8fd5ae2497e7acfd3ca27d341fa0878305c3072b63a06d",
    "maker":            "substr(input   , 4 + 32*2 + 12 + 1         , 20)",
    "taker":            "substr(input   , 4 + 32*3 + 12 + 1         , 20)",
    "maker_asset":      "substr(input   , 4 + 32*5 + 12 + 1         , 20)",
    "taker_asset":      "substr(input   , 4 + 32*4 + 12 + 1         , 20)",
    "maker_max_amount": "substr(input   , 4 + 32*8 + 1              , 32)",
    "taker_max_amount": "substr(input   , 4 + 32*6 + 1              , 32)",
    "order_hash":       "substr(input   , 4 + 32*11 + 1             , 16)",
}] %}
{% set _beginning = "4 + 32*1 + bytearray_to_bigint(substr(input, 4 + 32*1 + 24 + 1, 8))" %}
{% set methods = methods + [{
    "project":          "Native",
    "selector":         "0xc7cd9748",
    "name":             "exactInputSingle",
    "event":            "0x0c3ca67555399daacbfbeef89219bf4eca6380fdc58f2ed80cdc0841616c5818",
    "taker":            "substr(input   , 4 + 32*2 + 12 + 1         , 20)",
    "maker_asset":      "substr(input   , " ~ _beginning ~ " + 32*2 + 20*3 + 1, 20)",
    "taker_asset":      "substr(input   , " ~ _beginning ~ " + 32*2 + 20*4 + 1, 20)",
    "maker_min_amount": "substr(input   , 4 + 32*4 + 1              , 32)",
    "taker_max_amount": "substr(input   , 4 + 32*3 + 1              , 32)",
    "making_amount":    "substr(output  , 32*0 + 1                  , 32)",
    "order_hash":       "substr(input   , " ~ _beginning ~ " + 32*8 + 24 + 1, 16)",
}] %}
{% set methods = methods + [{
    "project":          "Native",
    "selector":         "0x2794949c",
    "name":             "exactInputSingle",
    "event":            "null",
    "taker":            "substr(input   , 4 + 32*2 + 12 + 1         , 20)",
    "maker_asset":      "substr(input   , " ~ _beginning ~ " + 32*2 + 20*3 + 1, 20)",
    "taker_asset":      "substr(input   , " ~ _beginning ~ " + 32*2 + 20*4 + 1, 20)",
    "taker_max_amount": "substr(input   , 4 + 32*3 + 1              , 32)",
    "making_amount":    "substr(output  , 32*0 + 1                  , 32)",
    "taking_amount":    "substr(input   , " ~ _beginning ~ " + 32*2 + 20*5 + 32*1 + 1, 32)",
}] %}
{% set _order_length = "bytearray_to_bigint(substr(input, " ~ _beginning ~ " + 24 + 1, 8))" %}
{% set methods = methods + [{
    "project":          "Native",
    "selector":         "0x5dc7b981",
    "name":             "exactInput",
    "event":            "null",
    "maker":            "substr(input, " ~ _beginning ~ " + 32*2 + 1, 20)",
    "taker":            "substr(input   , 4 + 32*2 + 12 + 1         , 20)",
    "maker_asset":      "substr(input, " ~ _beginning ~ " + 32*1 + " ~ _order_length ~ " / 32 * 32 - 32*6 - 18 + 1, 20)",
    "taker_asset":      "substr(input, " ~ _beginning ~ " + 32*2 + 20*4 + 1, 20)",
    "making_amount":    "substr(output  , 32*0 + 1                  , 32)",
    "taking_amount":    "substr(input, " ~ _beginning ~ " + 32*2 + 20*5 + 32*1 + 1, 32)",
}] %}
{% set methods = methods + [{
    "project":          "Native",
    "selector":         "0x68ab0bdb",
    "name":             "exactInput",
    "event":            "null",
    "maker":            "substr(input, " ~ _beginning ~ " + 32*2 + 1, 20)",
    "taker":            "substr(input   , 4 + 32*2 + 12 + 1         , 20)",
    "maker_asset":      "substr(input, " ~ _beginning ~ " + 32*14 + 5 + 1, 20)",
    "taker_asset":      "substr(input, " ~ _beginning ~ " + 32*2 + 20*4 + 1, 20)",
    "making_amount":    "substr(output  , 32*0 + 1                  , 32)",
    "taking_amount":    "substr(input, " ~ _beginning ~ " + 32*2 + 20*5 + 32*1 + 1, 32)",
}] %}

-- Clipper --

{% set methods = methods + [{
    "project":          "Clipper",
    "selector":         "0x2b651a6c",
    "name":             "swap",
    "event":            "0x4be05c8d54f5e056ab2cfa033e9f582057001268c3e28561bb999d35d2c8f2c8",
    "taker":            "substr(input   , 4 + 32*5 + 12 + 1         , 20)",
    "maker_asset":      "substr(input   , 4 + 32*1 + 12 + 1         , 20)",
    "taker_asset":      "substr(input   , 4 + 32*0 + 12 + 1         , 20)",
    "maker_max_amount": "substr(input   , 4 + 32*3 + 1              , 32)",
    "taker_max_amount": "substr(input   , 4 + 32*2 + 1              , 32)",
}] %}
{% set methods = methods + [{
    "project":          "Clipper",
    "selector":         "0x4cb6864c",
    "name":             "sellTokenForEth",
    "event":            "0x4be05c8d54f5e056ab2cfa033e9f582057001268c3e28561bb999d35d2c8f2c8",
    "taker":            "substr(input   , 4 + 32*4 + 12 + 1         , 20)",
    "taker_asset":      "substr(input   , 4 + 32*0 + 12 + 1         , 20)",
    "maker_max_amount": "substr(input   , 4 + 32*2 + 1              , 32)",
    "taker_max_amount": "substr(input   , 4 + 32*1 + 1              , 32)",
}] %}
{% set methods = methods + [{
    "project":          "Clipper",
    "selector":         "0x27a9b424",
    "name":             "sellEthForToken",
    "event":            "0x4be05c8d54f5e056ab2cfa033e9f582057001268c3e28561bb999d35d2c8f2c8",
    "taker":            "substr(input   , 4 + 32*4 + 12 + 1         , 20)",
    "maker_asset":      "substr(input   , 4 + 32*0 + 12 + 1         , 20)",
    "taker_max_amount": "substr(input   , 4 + 32*2 + 1              , 32)",
    "maker_max_amount": "substr(input   , 4 + 32*1 + 1              , 32)",
}] %}
{% set methods = methods + [{
    "project":          "Clipper",
    "selector":         "0x3b26e4eb",
    "name":             "transmitAndSwap",
    "event":            "0x4be05c8d54f5e056ab2cfa033e9f582057001268c3e28561bb999d35d2c8f2c8",
    "taker":            "substr(input   , 4 + 32*5 + 12 + 1         , 20)",
    "maker_asset":      "substr(input   , 4 + 32*1 + 12 + 1         , 20)",
    "taker_asset":      "substr(input   , 4 + 32*0 + 12 + 1         , 20)",
    "maker_max_amount": "substr(input   , 4 + 32*3 + 1              , 32)",
    "taker_max_amount": "substr(input   , 4 + 32*2 + 1              , 32)",
}] %}

-- Swaap --

{% set methods = methods + [{
    "project":          "Swaap",
    "selector":         "0x52bbbe29",
    "name":             "swap",
    "event":            "0x2170c741c41531aec20e7c107c24eecfdd15e69c9bb0a8dd37b1840b9e0b207b",
    "taker":            "substr(input   , 4 + 32*1 + 12 + 1         , 20)",
    "pool":             "substr(input   , 4 + 32*7 + 1              , 32)",
    "maker_asset":      "substr(input   , 4 + 32*10 + 12 + 1        , 20)",
    "taker_asset":      "substr(input   , 4 + 32*9 + 12 + 1         , 20)",
    "taker_max_amount": "substr(input   , 4 + 32*11 + 1             , 32)",
    "deadline":         "substr(input   , 4 + 32*6 + 1              , 32)",
}] %}

-- Paraswap --

{% set methods = methods + [{
    "project":          "Paraswap",
    "selector":         "0x98f9b46b",
    "name":             "fillOrder",
    "event":            "0x6621486d9c28838df4a87d2cca5007bc2aaf6a5b5de083b1db8faf709302c473",
    "maker":            "substr(input   , 4 + 32*4 + 12 + 1         , 20)",
    "maker_asset":      "substr(input   , 4 + 32*2 + 12 + 1         , 20)",
    "taker_asset":      "substr(input   , 4 + 32*3 + 12 + 1         , 20)",
    "maker_max_amount": "substr(input   , 4 + 32*6 + 1              , 32)",
    "taker_max_amount": "substr(input   , 4 + 32*7 + 1              , 32)",
}] %}
{% set methods = methods + [{
    "project":          "Paraswap",
    "selector":         "0xc88ae6dc",
    "name":             "partialFillOrder",
    "event":            "0x6621486d9c28838df4a87d2cca5007bc2aaf6a5b5de083b1db8faf709302c473",
    "maker":            "substr(input   , 4 + 32*4 + 12 + 1         , 20)",
    "maker_asset":      "substr(input   , 4 + 32*2 + 12 + 1         , 20)",
    "taker_asset":      "substr(input   , 4 + 32*3 + 12 + 1         , 20)",
    "maker_max_amount": "substr(input   , 4 + 32*6 + 1              , 32)",
    "taker_max_amount": "substr(input   , 4 + 32*7 + 1              , 32)",
    "making_amount":    "substr(output  , 32*0 + 1                  , 32)",
}] %}
{% set methods = methods + [{
    "project":          "Paraswap",
    "selector":         "0x24abf828",
    "name":             "partialFillOrderWithTarget",
    "event":            "0x6621486d9c28838df4a87d2cca5007bc2aaf6a5b5de083b1db8faf709302c473",
    "maker":            "substr(input   , 4 + 32*4 + 12 + 1         , 20)",
    "maker_asset":      "substr(input   , 4 + 32*2 + 12 + 1         , 20)",
    "taker_asset":      "substr(input   , 4 + 32*3 + 12 + 1         , 20)",
    "maker_max_amount": "substr(input   , 4 + 32*6 + 1              , 32)",
    "taker_max_amount": "substr(input   , 4 + 32*7 + 1              , 32)",
    "making_amount":    "substr(output  , 32*0 + 1                  , 32)",
}] %}
{% set methods = methods + [{
    "project":          "Paraswap",
    "selector":         "0xfbc69a30",
    "name":             "settleSwap",
    "auction":          "true",
    "event":            "0xece0e513cbaa408206d1b097118463feab4b263a1c43dd6896e8951cdb53e7d9",
    "maker":            "substr(input   , 4 + 32*10 + 12 + 1        , 20)",
    "receiver":         "substr(input   , 4 + 32*11 + 12 + 1        , 20)",
    "maker_asset":      "substr(input   , 4 + 32*12 + 12 + 1        , 20)",
    "taker_asset":      "substr(input   , 4 + 32*13 + 12 + 1        , 20)",
    "taker_min_amount": "substr(input   , 4 + 32*15 + 1             , 32)",
    "making_amount":    "substr(input   , 4 + 32*14 + 1             , 32)",
    "deadline":         "substr(input   , 4 + 32*16 + 1             , 32)",
    "end":              "cast(bytearray_to_uint256(substr(input, 4 + 32*17 + 1, 32)) / 1000 as varbinary)",
}] %}

-- CoWSwap --

{% set _beginning = "4 + bytearray_to_bigint(substr(input, 4 + 32*2 + 24 + 1, 8))" %}
{% set _order_beginning = _beginning ~ " + 32*1 + bytearray_to_bigint(substr(input, " ~ _beginning ~ " + 32*x + 24 + 1, 8))" %}
{% set _executed_amount = "substr(input, " ~ _order_beginning ~ " + 32*9 + 1, 32)" %}
{% set methods = methods + [{
    "project":          "CoWSwap",
    "selector":         "0x13d79a0b",
    "name":             "settle",
    "auction":          "true",
    "event":            "0xa07a543ab8a018198e99ca0184c93fe9050a79400a0a723441f84de1d972cc17",
    "number":           "coalesce(try(bytearray_to_bigint(substr(input, " ~ _beginning ~ " + 24 + 1, 8))), 1)",
    "maker_asset":      "substr(input, 4 + bytearray_to_bigint(substr(input, 4 + 24 + 1, 8)) + 32*(bytearray_to_bigint(substr(input, " ~ _order_beginning ~ " + 32*0 + 24 + 1, 8)) + 1) + 12 + 1, 20)",
    "taker_asset":      "substr(input, 4 + bytearray_to_bigint(substr(input, 4 + 24 + 1, 8)) + 32*(bytearray_to_bigint(substr(input, " ~ _order_beginning ~ " + 32*1 + 24 + 1, 8)) + 1) + 12 + 1, 20)",
    "receiver":         "substr(input, " ~ _order_beginning ~ " + 32*2 + 12 + 1, 20)",
    "maker_max_amount": "substr(input, " ~ _order_beginning ~ " + 32*3 + 1, 32)",
    "taker_max_amount": "substr(input, " ~ _order_beginning ~ " + 32*4 + 1, 32)",
    "deadline":         "substr(input, " ~ _order_beginning ~ " + 32*5 + 1, 32)",
    "fee_amount":       "substr(input, " ~ _order_beginning ~ " + 32*7 + 1, 32)",
    "making_amount":    "if(bitwise_and(bytearray_to_bigint(substr(input, " ~ _order_beginning ~ " + 32*8 + 31 + 1, 1)), bytearray_to_bigint(0x01)) = 0, " ~ _executed_amount ~ ")",
    "taking_amount":    "if(bitwise_and(bytearray_to_bigint(substr(input, " ~ _order_beginning ~ " + 32*8 + 31 + 1, 1)), bytearray_to_bigint(0x01)) > 0, " ~ _executed_amount ~ ")",
    "condition":        "bytearray_to_bigint(substr(input, " ~ _beginning ~ " + 24 + 1, 8)) > 0",
}] %}

-- Uniswap --

{% set _taker_data = "4 + 32*4 + bytearray_to_bigint(substr(input, 4 + 32*13 + 24 + 1, 8))" %}
{% set _recipients = "bytearray_to_bigint(substr(input, " ~ _taker_data ~ " + 32*1 + 24 + 1, 8))" %}
{% set methods = methods + [{
    "project":          "Uniswap",
    "selector":         "0x3f62192e",
    "tag":              "'UniswapXV1'",
    "name":             "execute",
    "auction":          "true",
    "event":            "0x78ad7ec0e9f89e74012afa58738b6b661c024cb0fd185ee2f616c0a28924bd66",
    "maker":            "substr(input, 4 + 32*15 + 12 + 1       , 20)",
    "receiver":         "substr(input, " ~ _taker_data ~ " + 32*5 + 12 + 1, 20)",
    "maker_asset":      "substr(input, 4 + 32*10 + 12 + 1       , 20)",
    "taker_asset":      "substr(input, " ~ _taker_data ~ " + 32*2 + 12 + 1, 20)",
    "maker_max_amount": "substr(input, 4 + 32*11 + 1            , 32)",
    "maker_min_amount": "substr(input, 4 + 32*12 + 1            , 32)",
    "taker_max_amount": "substr(input, " ~ _taker_data ~ " + 32*3 + 1, 32)",
    "taker_min_amount": "substr(input, " ~ _taker_data ~ " + 32*4 + 1, 32)",
    "start":            "substr(input, 4 + 32*6 + 1             , 32)",
    "end":              "substr(input, 4 + 32*7 + 1             , 32)",
    "deadline":         "cast(abs(bytearray_to_int256(substr(input, 4 + 32*17 + 1, 32))) as varbinary)",
    "nonce":            "substr(input, 4 + 32*16 + 1            , 32)",
    "fee_asset":        "if(" ~ _recipients ~ " > 1, substr(input, " ~ _taker_data ~ " + 32*6 + 12 + 1, 20))",
    "fee_max_amount":   "if(" ~ _recipients ~ " > 1, substr(input, " ~ _taker_data ~ " + 32*7 + 1, 32))",
    "fee_min_amount":   "if(" ~ _recipients ~ " > 1, substr(input, " ~ _taker_data ~ " + 32*8 + 1, 32))",
    "fee_receiver":     "if(" ~ _recipients ~ " > 1, substr(input, " ~ _taker_data ~ " + 32*9 + 12 + 1, 20))",
}] %}
{% set _taker_data = "4 + 32*5 + bytearray_to_bigint(substr(input, 4 + 32*14 + 24 + 1, 8))" %}
{% set _recipients = "bytearray_to_bigint(substr(input, " ~ _taker_data ~ " + 32*1 + 24 + 1, 8))" %}
{% set methods = methods + [{
    "project":          "Uniswap",
    "selector":         "0x0d335884",
    "tag":              "'UniswapXV1'",
    "name":             "executeWithCallback",
    "auction":          "true",
    "event":            "0x78ad7ec0e9f89e74012afa58738b6b661c024cb0fd185ee2f616c0a28924bd66",
    "maker":            "substr(input, 4 + 32*16 + 12 + 1       , 20)",
    "receiver":         "substr(input, " ~ _taker_data ~ " + 32*5 + 12 + 1, 20)",
    "maker_asset":      "substr(input, 4 + 32*11 + 12 + 1       , 20)",
    "taker_asset":      "substr(input, " ~ _taker_data ~ " + 32*2 + 12 + 1, 20)",
    "maker_max_amount": "substr(input, 4 + 32*12 + 1            , 32)",
    "maker_min_amount": "substr(input, 4 + 32*13 + 1            , 32)",
    "taker_max_amount": "substr(input, " ~ _taker_data ~ " + 32*3 + 1, 32)",
    "taker_min_amount": "substr(input, " ~ _taker_data ~ " + 32*4 + 1, 32)",
    "start":            "substr(input, 4 + 32*7 + 1             , 32)",
    "end":              "substr(input, 4 + 32*8 + 1             , 32)",
    "deadline":         "cast(abs(bytearray_to_int256(substr(input, 4 + 32*18 + 1, 32))) as varbinary)",
    "nonce":            "substr(input, 4 + 32*17 + 1            , 32)",
    "fee_asset":        "if(" ~ _recipients ~ " > 1, substr(input, " ~ _taker_data ~ " + 32*6 + 12 + 1, 20))",
    "fee_max_amount":   "if(" ~ _recipients ~ " > 1, substr(input, " ~ _taker_data ~ " + 32*7 + 1, 32))",
    "fee_min_amount":   "if(" ~ _recipients ~ " > 1, substr(input, " ~ _taker_data ~ " + 32*8 + 1, 32))",
    "fee_receiver":     "if(" ~ _recipients ~ " > 1, substr(input, " ~ _taker_data ~ " + 32*9 + 12 + 1, 20))",
}] %}
{% set _order_beginning = "4 + 32*2 + bytearray_to_bigint(substr(input, 4 + 32*2 + 32*(x - 1) + 24 + 1, 8))" %}
{% set _taker_data = "bytearray_to_bigint(substr(input, " ~ _order_beginning ~ " + 32*12 + 24 + 1, 8))" %}
{% set _recipients = "bytearray_to_bigint(substr(input, " ~ _order_beginning ~ " + " ~ _taker_data ~ " + 32*4 + 24 + 1, 8))" %}
{% set methods = methods + [{
    "project":          "Uniswap",
    "selector":         "0x0d7a16c3",
    "tag":              "'UniswapXV1'",
    "name":             "executeBatch",
    "auction":          "true",
    "event":            "0x78ad7ec0e9f89e74012afa58738b6b661c024cb0fd185ee2f616c0a28924bd66",
    "number":           "coalesce(try(bytearray_to_bigint(substr(input, 4 + bytearray_to_bigint(substr(input, 4 + 24 + 1, 8)) + 24 + 1, 8))), 1)",
    "maker":            "substr(input, " ~ _order_beginning ~ " + 32*14 + 12 + 1, 20)",
    "receiver":         "substr(input, " ~ _order_beginning ~ " + " ~ _taker_data ~ " + 32*8 + 12 + 1, 20)",
    "maker_asset":      "substr(input, " ~ _order_beginning ~ " + 32*9 + 12 + 1, 20)",
    "taker_asset":      "substr(input, " ~ _order_beginning ~ " + " ~ _taker_data ~ " + 32*5 + 12 + 1, 20)",
    "maker_max_amount": "substr(input, " ~ _order_beginning ~ " + 32*10 + 1, 32)",
    "maker_min_amount": "substr(input, " ~ _order_beginning ~ " + 32*11 + 1, 32)",
    "taker_max_amount": "substr(input, " ~ _order_beginning ~ " + " ~ _taker_data ~ " + 32*6 + 1, 32)",
    "taker_min_amount": "substr(input, " ~ _order_beginning ~ " + " ~ _taker_data ~ " + 32*7 + 1, 32)",
    "start":            "substr(input, " ~ _order_beginning ~ " + 32*5 + 1, 32)",
    "end":              "substr(input, " ~ _order_beginning ~ " + 32*6 + 1, 32)",
    "deadline":         "cast(abs(bytearray_to_int256(substr(input, " ~ _order_beginning ~ " + 32*16 + 1, 32))) as varbinary)",
    "nonce":            "substr(input, " ~ _order_beginning ~ " + 32*15 + 1, 32)",
    "fee_asset":        "if(" ~ _recipients ~ " > 1, substr(input, " ~ _order_beginning ~ " + " ~ _taker_data ~ " + 32*9  + 12 + 1, 20))",
    "fee_max_amount":   "if(" ~ _recipients ~ " > 1, substr(input, " ~ _order_beginning ~ " + " ~ _taker_data ~ " + 32*10 + 1, 32))",
    "fee_min_amount":   "if(" ~ _recipients ~ " > 1, substr(input, " ~ _order_beginning ~ " + " ~ _taker_data ~ " + 32*11 + 1, 32))",
    "fee_receiver":     "if(" ~ _recipients ~ " > 1, substr(input, " ~ _order_beginning ~ " + " ~ _taker_data ~ " + 32*12 + 12 + 1, 20))",
}] %}
{% set _beginning = "4 + bytearray_to_bigint(substr(input, 4 + 24 + 1, 8)) + 32*1" %}
{% set _order_beginning = _beginning ~ " + bytearray_to_bigint(substr(input, " ~ _beginning ~ " + 32*(x - 1) + 24 + 1, 8))" %}
{% set _taker_data = "bytearray_to_bigint(substr(input, " ~ _order_beginning ~ " + 32*12 + 24 + 1, 8))" %}
{% set _recipients = "bytearray_to_bigint(substr(input, " ~ _order_beginning ~ " + " ~ _taker_data ~ " + 32*4 + 24 + 1, 8))" %}
{% set methods = methods + [{
    "project":          "Uniswap",
    "selector":         "0x13fb72c7",
    "tag":              "'UniswapXV1'",
    "name":             "executeBatchWithCallback",
    "auction":          "true",
    "event":            "0x78ad7ec0e9f89e74012afa58738b6b661c024cb0fd185ee2f616c0a28924bd66",
    "number":           "coalesce(try(bytearray_to_bigint(substr(input, 4 + bytearray_to_bigint(substr(input, 4 + 24 + 1, 8)) + 24 + 1, 8))), 1)",
    "maker":            "substr(input, " ~ _order_beginning ~ " + 32*14 + 12 + 1, 20)",
    "receiver":         "substr(input, " ~ _order_beginning ~ " + " ~ _taker_data ~ " + 32*8 + 12 + 1, 20)",
    "maker_asset":      "substr(input, " ~ _order_beginning ~ " + 32*9 + 12 + 1, 20)",
    "taker_asset":      "substr(input, " ~ _order_beginning ~ " + " ~ _taker_data ~ " + 32*5 + 12 + 1, 20)",
    "maker_max_amount": "substr(input, " ~ _order_beginning ~ " + 32*10 + 1, 32)",
    "maker_min_amount": "substr(input, " ~ _order_beginning ~ " + 32*11 + 1, 32)",
    "taker_max_amount": "substr(input, " ~ _order_beginning ~ " + " ~ _taker_data ~ " + 32*6 + 1, 32)",
    "taker_min_amount": "substr(input, " ~ _order_beginning ~ " + " ~ _taker_data ~ " + 32*7 + 1, 32)",
    "start":            "substr(input, " ~ _order_beginning ~ " + 32*5 + 1, 32)",
    "end":              "substr(input, " ~ _order_beginning ~ " + 32*6 + 1, 32)",
    "deadline":         "cast(abs(bytearray_to_int256(substr(input, " ~ _order_beginning ~ " + 32*16 + 1, 32))) as varbinary)",
    "nonce":            "substr(input, " ~ _order_beginning ~ " + 32*15 + 1, 32)",
    "fee_asset":        "if(" ~ _recipients ~ " > 1, substr(input, " ~ _order_beginning ~ " + " ~ _taker_data ~ " + 32*9 + 12 + 1, 20))",
    "fee_max_amount":   "if(" ~ _recipients ~ " > 1, substr(input, " ~ _order_beginning ~ " + " ~ _taker_data ~ " + 32*10 + 1, 32))",
    "fee_min_amount":   "if(" ~ _recipients ~ " > 1, substr(input, " ~ _order_beginning ~ " + " ~ _taker_data ~ " + 32*11 + 1, 32))",
    "fee_receiver":     "if(" ~ _recipients ~ " > 1, substr(input, " ~ _order_beginning ~ " + " ~ _taker_data ~ " + 32*12 + 12 + 1, 20))",
}] %}
{% set _taker_data = "4 + 32*1 + bytearray_to_bigint(substr(input, 4 + 32*10 + 24 + 1, 8))" %}
{% set _recipients = "bytearray_to_bigint(substr(input, " ~ _taker_data ~ " + 32*4 + 24 + 1, 8))" %}
{% set methods = methods + [{
    "project":          "Uniswap",
    "selector":         "0x3f62192e",
    "tag":              "'UniswapXV2'",
    "name":             "execute",
    "auction":          "true",
    "event":            "0x78ad7ec0e9f89e74012afa58738b6b661c024cb0fd185ee2f616c0a28924bd66",
    "maker":            "substr(input, 4 + 32*14 + 12 + 1       , 20)",
    "receiver":         "substr(input, " ~ _taker_data ~ " + 32*8 + 12 + 1, 20)",
    "maker_asset":      "substr(input, 4 + 32*7  + 12 + 1       , 20)",
    "taker_asset":      "substr(input, " ~ _taker_data ~ " + 32*5 + 12 + 1, 20)",
    "maker_max_amount": "substr(input, 4 + 32*8  + 1            , 32)",
    "maker_min_amount": "substr(input, 4 + 32*9  + 1            , 32)",
    "taker_max_amount": "substr(input, " ~ _taker_data ~ " + 32*6 + 1, 32)",
    "taker_min_amount": "substr(input, " ~ _taker_data ~ " + 32*7 + 1, 32)",
    "start":            "substr(input, " ~ _taker_data ~ " + 32*4 + 32*1 + 32*4*(" ~ _recipients ~ ") + 1, 32)",
    "end":              "substr(input, " ~ _taker_data ~ " + 32*4 + 32*1 + 32*4*(" ~ _recipients ~ ") + 32*1 + 1, 32)",
    "deadline":         "cast(abs(bytearray_to_int256(substr(input, 4 + 32*16 + 1, 32))) as varbinary)",
    "nonce":            "substr(input, 4 + 32*15 + 1            , 32)",
    "fee_asset":        "if(" ~ _recipients ~ " > 1, substr(input, " ~ _taker_data ~ " + 32*9  + 12 + 1, 20))",
    "fee_max_amount":   "if(" ~ _recipients ~ " > 1, substr(input, " ~ _taker_data ~ " + 32*10 + 1, 32))",
    "fee_min_amount":   "if(" ~ _recipients ~ " > 1, substr(input, " ~ _taker_data ~ " + 32*11 + 1, 32))",
    "fee_receiver":     "if(" ~ _recipients ~ " > 1, substr(input, " ~ _taker_data ~ " + 32*12 + 12 + 1, 20))",
}] %}
{% set _taker_data = "4 + 32*2 + bytearray_to_bigint(substr(input, 4 + 32*11 + 24 + 1, 8))" %}
{% set _recipients = "bytearray_to_bigint(substr(input, " ~ _taker_data ~ " + 32*4 + 24 + 1, 8))" %}
{% set methods = methods + [{
    "project":          "Uniswap",
    "selector":         "0x0d335884",
    "tag":              "'UniswapXV2'",
    "name":             "executeWithCallback",
    "auction":          "true",
    "event":            "0x78ad7ec0e9f89e74012afa58738b6b661c024cb0fd185ee2f616c0a28924bd66",
    "maker":            "substr(input, 4 + 32*15 + 12 + 1       , 20)",
    "receiver":         "substr(input, " ~ _taker_data ~ " + 32*8 + 12 + 1, 20)",
    "maker_asset":      "substr(input, 4 + 32*8  + 12 + 1       , 20)",
    "taker_asset":      "substr(input, " ~ _taker_data ~ " + 32*5 + 12 + 1, 20)",
    "maker_max_amount": "substr(input, 4 + 32*9  + 1            , 32)",
    "maker_min_amount": "substr(input, 4 + 32*10 + 1            , 32)",
    "taker_max_amount": "substr(input, " ~ _taker_data ~ " + 32*6 + 1, 32)",
    "taker_min_amount": "substr(input, " ~ _taker_data ~ " + 32*7 + 1, 32)",
    "start":            "substr(input, " ~ _taker_data ~ " + 32*4 + 32*1 + 32*4*(" ~ _recipients ~ ") + 1, 32)",
    "end":              "substr(input, " ~ _taker_data ~ " + 32*4 + 32*1 + 32*4*(" ~ _recipients ~ ") + 32*1 + 1, 32)",
    "deadline":         "cast(abs(bytearray_to_int256(substr(input, 4 + 32*17 + 1, 32))) as varbinary)",
    "nonce":            "substr(input, 4 + 32*16 + 1            , 32)",
    "fee_asset":        "if(" ~ _recipients ~ " > 1, substr(input, " ~ _taker_data ~ " + 32*9  + 12 + 1, 20))",
    "fee_max_amount":   "if(" ~ _recipients ~ " > 1, substr(input, " ~ _taker_data ~ " + 32*10 + 1, 32))",
    "fee_min_amount":   "if(" ~ _recipients ~ " > 1, substr(input, " ~ _taker_data ~ " + 32*11 + 1, 32))",
    "fee_receiver":     "if(" ~ _recipients ~ " > 1, substr(input, " ~ _taker_data ~ " + 32*12 + 12 + 1, 20))",
}] %}
{% set _beginning = "4 + bytearray_to_bigint(substr(input, 4 + 24 + 1, 8)) + 32*1" %}
{% set _order_beginning = _beginning ~ " + bytearray_to_bigint(substr(input, " ~ _beginning ~ " + 32*(x - 1) + 24 + 1, 8))" %}
{% set _taker_data = "bytearray_to_bigint(substr(input, " ~ _order_beginning ~ " + 32*9 + 24 + 1, 8))" %}
{% set _recipients = "bytearray_to_bigint(substr(input, " ~ _order_beginning ~ " + " ~ _taker_data ~ " + 32*4 + 24 + 1, 8))" %}
{% set methods = methods + [{
    "project":          "Uniswap",
    "selector":         "0x13fb72c7",
    "tag":              "'UniswapXV2'",
    "name":             "executeBatchWithCallback",
    "auction":          "true",
    "event":            "0x78ad7ec0e9f89e74012afa58738b6b661c024cb0fd185ee2f616c0a28924bd66",
    "number":           "coalesce(try(bytearray_to_bigint(substr(input, 4 + bytearray_to_bigint(substr(input, 4 + 24 + 1, 8)) + 24 + 1, 8))), 1)",
    "maker":            "substr(input, " ~ _order_beginning ~ " + 32*13 + 12 + 1, 20)",
    "receiver":         "substr(input, " ~ _order_beginning ~ " + " ~ _taker_data ~ " + 32*8 + 12 + 1, 20)",
    "maker_asset":      "substr(input, " ~ _order_beginning ~ " + 32*6  + 12 + 1, 20)",
    "taker_asset":      "substr(input, " ~ _order_beginning ~ " + " ~ _taker_data ~ " + 32*5 + 12 + 1, 20)",
    "maker_max_amount": "substr(input, " ~ _order_beginning ~ " + 32*7 + 1, 32)",
    "maker_min_amount": "substr(input, " ~ _order_beginning ~ " + 32*8 + 1, 32)",
    "taker_max_amount": "substr(input, " ~ _order_beginning ~ " + " ~ _taker_data ~ " + 32*6 + 1, 32)",
    "taker_min_amount": "substr(input, " ~ _order_beginning ~ " + " ~ _taker_data ~ " + 32*7 + 1, 32)",
    "start":            "substr(input, " ~ _order_beginning ~ " + " ~ _taker_data ~ " + 32*4 + 32*1 + 32*4*(" ~ _recipients ~ ") + 1, 32)",
    "end":              "substr(input, " ~ _order_beginning ~ " + " ~ _taker_data ~ " + 32*4 + 32*1 + 32*4*(" ~ _recipients ~ ") + 32*1 + 1, 32)",
    "deadline":         "cast(abs(bytearray_to_int256(substr(input, " ~ _order_beginning ~ " + 32*15 + 1, 32))) as varbinary)",
    "nonce":            "substr(input, " ~ _order_beginning ~ " + 32*14 + 1, 32)",
    "fee_asset":        "if(" ~ _recipients ~ " > 1, substr(input, " ~ _order_beginning ~ " + " ~ _taker_data ~ " + 32*9  + 12 + 1, 20))",
    "fee_max_amount":   "if(" ~ _recipients ~ " > 1, substr(input, " ~ _order_beginning ~ " + " ~ _taker_data ~ " + 32*10 + 1, 32))",
    "fee_min_amount":   "if(" ~ _recipients ~ " > 1, substr(input, " ~ _order_beginning ~ " + " ~ _taker_data ~ " + 32*11 + 1, 32))",
    "fee_receiver":     "if(" ~ _recipients ~ " > 1, substr(input, " ~ _order_beginning ~ " + " ~ _taker_data ~ " + 32*12 + 12 + 1, 20))",
}] %}

-- Bebop --

{% set methods = methods + [{
    "project":          "Bebop",
    "selector":         "0x1a499026",
    "name":             "settleSingle",
    "auction":          "true",
    "event":            "0xadd7095becdaa725f0f33243630938c861b0bba83dfd217d4055701aa768ec2e",
    "maker":            "substr(input    , 4 + 32*2 + 12 + 1        , 20)",
    "taker":            "substr(input    , 4 + 32*1 + 12 + 1        , 20)",
    "receiver":         "substr(input    , 4 + 32*8 + 12 + 1        , 20)",
    "maker_asset":      "substr(input    , 4 + 32*5 + 12 + 1        , 20)",
    "taker_asset":      "substr(input    , 4 + 32*4 + 12 + 1        , 20)",
    "maker_max_amount": "substr(input    , 4 + 32*7 + 1             , 32)",
    "taker_max_amount": "substr(input    , 4 + 32*6 + 1             , 32)",
    "making_amount":    "substr(input    , 4 + 32*14 + 1            , 32)",
    "taking_amount":    "if(bytearray_to_bigint(substr(input, 4 + 32*13 + 24 + 1, 8)) = 1, substr(input, 4 + 32*6 + 1, 32), substr(input, 4 + 32*12 + 1, 32))",
    "deadline":         "substr(input    , 4 + 32*1 + 1             , 32)",
    "nonce":            "substr(input    , 4 + 32*3 + 1             , 32)",
    "order_hash":       "substr(input    , 4 + 32*10 + 1            , 16)",
}] %}
{% set methods = methods + [{
    "project":          "Bebop",
    "selector":         "0x38ec0211",
    "name":             "settleSingleAndSignPermit2",
    "auction":          "true",
    "event":            "0xadd7095becdaa725f0f33243630938c861b0bba83dfd217d4055701aa768ec2e",
    "maker":            "substr(input    , 4 + 32*2 + 12 + 1        , 20)",
    "taker":            "substr(input    , 4 + 32*1 + 12 + 1        , 20)",
    "receiver":         "substr(input    , 4 + 32*8 + 12 + 1        , 20)",
    "maker_asset":      "substr(input    , 4 + 32*5 + 12 + 1        , 20)",
    "taker_asset":      "substr(input    , 4 + 32*4 + 12 + 1        , 20)",
    "maker_max_amount": "substr(input    , 4 + 32*7 + 1             , 32)",
    "taker_max_amount": "substr(input    , 4 + 32*6 + 1             , 32)",
    "making_amount":    "substr(input    , 4 + 32*14 + 1            , 32)",
    "taking_amount":    "if(bytearray_to_bigint(substr(input, 4 + 32*13 + 24 + 1, 8)) = 1, substr(input, 4 + 32*6 + 1, 32), substr(input, 4 + 32*12 + 1, 32))",
    "deadline":         "substr(input    , 4 + 32*1 + 1             , 32)",
    "nonce":            "substr(input    , 4 + 32*3 + 1             , 32)",
    "order_hash":       "substr(input    , 4 + 32*10 + 1            , 16)",
}] %}
{% set _beginning = "4 + bytearray_to_bigint(substr(input, 4 + 24 + 1, 8))" %}
{% set _maker_data = "bytearray_to_bigint(substr(input, " ~ _beginning ~ " + 32*5 + 24 + 1, 8))" %}
{% set _taker_data = "bytearray_to_bigint(substr(input, " ~ _beginning ~ " + 32*4 + 24 + 1, 8))" %}
{% set _maker_parts = "bytearray_to_bigint(substr(input, " ~ _beginning ~ " + " ~ _maker_data ~ " + 24 + 1, 8))" %}
{% set _taker_parts = "bytearray_to_bigint(substr(input, " ~ _beginning ~ " + " ~ _taker_data ~ " + 24 + 1, 8))" %}
{% set methods = methods + [{
    "project":          "Bebop",
    "selector":         "0xefe34fe6",
    "name":             "settleMulti",
    "auction":          "true",
    "event":            "0xadd7095becdaa725f0f33243630938c861b0bba83dfd217d4055701aa768ec2e",
    "number":           "coalesce(try(greatest(" ~ _maker_parts ~ ", " ~ _taker_parts ~ ")), 1)",
    "_maker_parts":     "substr(input, " ~ _beginning ~ " + " ~ _maker_data ~ " + 24 + 1, 8)",
    "_taker_parts":     "substr(input, " ~ _beginning ~ " + " ~ _taker_data ~ " + 24 + 1, 8)",
    "maker":            "substr(input, " ~ _beginning ~ " + 32*2 + 12 + 1   , 20)",
    "taker":            "substr(input, " ~ _beginning ~ " + 32*1 + 12 + 1   , 20)",
    "receiver":         "substr(input, " ~ _beginning ~ " + 32*8 + 12 + 1   , 20)",
    "maker_asset":      "substr(input, " ~ _beginning ~ " + " ~ _maker_data ~ " + 32 * least(x, " ~ _maker_parts ~ ") + 12 + 1, 20)",
    "taker_asset":      "substr(input, " ~ _beginning ~ " + " ~ _taker_data ~ " + 32 * least(x, " ~ _taker_parts ~ ") + 12 + 1, 20)",
    "making_amount":    "substr(input, " ~ _beginning ~ " + bytearray_to_bigint(substr(input, " ~ _beginning ~ " + 32*7 + 24 + 1, 8)) + 32 * least(x, bytearray_to_bigint(substr(input, " ~ _beginning ~ " + bytearray_to_bigint(substr(input, " ~ _beginning ~ " + 32*7 + 24 + 1, 8)) + 24 + 1, 8))) + 1, 32)",
    "taking_amount":    "substr(input, " ~ _beginning ~ " + bytearray_to_bigint(substr(input, " ~ _beginning ~ " + 32*6 + 24 + 1, 8)) + 32 * least(x, bytearray_to_bigint(substr(input, " ~ _beginning ~ " + bytearray_to_bigint(substr(input, " ~ _beginning ~ " + 32*6 + 24 + 1, 8)) + 24 + 1, 8))) + 1, 32)",
    "deadline":         "substr(input, " ~ _beginning ~ " + 32*1 + 1        , 32)",
    "nonce":            "substr(input, " ~ _beginning ~ " + 32*3 + 1        , 32)",
    "order_hash":       "substr(input, " ~ _beginning ~ " + 32*10 + 1       , 16)",
}] %}

{{ return(methods) }}

{% endmacro %}