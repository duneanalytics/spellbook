{% macro oneinch_project_orders_macro(
    blockchain
    , date_from = '2019-01-01'
)%}

-- EVENTS CONFIG
{%
    set events = {
        "0x0bcc4c97732e47d9946f229edb95f5b6323f601300e4690de719993f3c371129": {
            "project": "ZeroEx",
            "name": "Fill",
            "maker":            "substr(topic1  , 12 + 1                , 20)",
            "taker":            "substr(data    , 12 + 1                , 20)",
            "maker_asset":      "if(substr(data, bytearray_to_bigint(substr(data, 32*6 + 24 + 1, 8)) + 32*1 + 1, 4) = 0xf47261b0, substr(data, bytearray_to_bigint(substr(data, 32*6 + 24 + 1, 8)) + 32*1 + 4 + 12 + 1, 20), 0x01)",
            "taker_asset":      "if(substr(data, bytearray_to_bigint(substr(data, 32*7 + 24 + 1, 8)) + 32*1 + 1, 4) = 0xf47261b0, substr(data, bytearray_to_bigint(substr(data, 32*7 + 24 + 1, 8)) + 32*1 + 4 + 12 + 1, 20), 0x01)",
            "making_amount":    "substr(data    , 32*2 + 1              , 32)",
            "taking_amount":    "substr(data    , 32*3 + 1              , 32)",
            "order_hash":       "substr(topic3  , 1                     , 32)",
        },
        "0x6869791f0a34781b29882982cc39e882768cf2c96995c2a110c577c53bc932d5": {
            "project": "ZeroEx",
            "name": "Fill",
            "maker":            "substr(topic1  , 12 + 1                , 20)",
            "taker":            "substr(data    , 32*4 + 12 + 1         , 20)",
            "maker_asset":      "if(substr(data, bytearray_to_bigint(substr(data, 32*0 + 24 + 1, 8)) + 32*1 + 1, 4) = 0xf47261b0, substr(data, bytearray_to_bigint(substr(data, 32*0 + 24 + 1, 8)) + 32*1 + 4 + 12 + 1, 20), 0x01)",
            "taker_asset":      "if(substr(data, bytearray_to_bigint(substr(data, 32*1 + 24 + 1, 8)) + 32*1 + 1, 4) = 0xf47261b0, substr(data, bytearray_to_bigint(substr(data, 32*1 + 24 + 1, 8)) + 32*1 + 4 + 12 + 1, 20), 0x01)",
            "making_amount":    "substr(data    , 32*6 + 1              , 32)",
            "taking_amount":    "substr(data    , 32*7 + 1              , 32)",
            "order_hash":       "substr(topic3  , 1                     , 32)",
        },
        "0xab614d2b738543c0ea21f56347cf696a3a0c42a7cbec3212a5ca22a4dcff2124": {
            "project": "ZeroEx",
            "name": "LimitOrderFilled",
            "maker":            "substr(data    , 32*1 + 12 + 1         , 20)",
            "taker":            "substr(data    , 32*2 + 12 + 1         , 20)",
            "maker_asset":      "substr(data    , 32*4 + 12 + 1         , 20)",
            "taker_asset":      "substr(data    , 32*5 + 12 + 1         , 20)",
            "making_amount":    "substr(data    , 32*7 + 1              , 32)",
            "taking_amount":    "substr(data    , 32*6 + 1              , 32)",
            "taker_fee_amount": "substr(data    , 32*8 + 1              , 32)",
            "order_hash":       "substr(data    , 1                     , 32)",
        },
        "0xac75f773e3a92f1a02b12134d65e1f47f8a14eabe4eaf1e24624918e6a8b269f": {
            "project": "ZeroEx",
            "name": "OtcOrderFilled",
            "maker":            "substr(data    , 32*1 + 12 + 1         , 20)",
            "taker":            "substr(data    , 32*2 + 12 + 1         , 20)",
            "maker_asset":      "substr(data    , 32*3 + 12 + 1         , 20)",
            "taker_asset":      "substr(data    , 32*4 + 12 + 1         , 20)",
            "making_amount":    "substr(data    , 32*5 + 1              , 32)",
            "taking_amount":    "substr(data    , 32*6 + 1              , 32)",
            "order_hash":       "substr(data    , 1                     , 32)",
        },
        "0x829fa99d94dc4636925b38632e625736a614c154d55006b7ab6bea979c210c32": {
            "project": "ZeroEx",
            "name": "RfqOrderFilled",
            "maker":            "substr(data    , 32*1 + 12 + 1         , 20)",
            "taker":            "substr(data    , 32*2 + 12 + 1         , 20)",
            "maker_asset":      "substr(data    , 32*3 + 12 + 1         , 20)",
            "taker_asset":      "substr(data    , 32*4 + 12 + 1         , 20)",
            "making_amount":    "substr(data    , 32*6 + 1              , 32)",
            "taking_amount":    "substr(data    , 32*5 + 1              , 32)",
            "order_hash":       "substr(data    , 1                     , 32)",
        },
        "0xb709ddcc6550418e9b89df1f4938071eeaa3f6376309904c77e15d46b16066f5": {
            "project": "Hashflow",
            "name": "Trade",
            "maker":            "substr(data    , 32*0 + 12 + 1         , 20)",
            "taker":            "substr(data    , 32*1 + 12 + 1         , 20)",
            "maker_asset":      "substr(data    , 32*4 + 12 + 1         , 20)",
            "taker_asset":      "substr(data    , 32*3 + 12 + 1         , 20)",
            "making_amount":    "substr(data    , 32*6 + 1              , 32)",
            "taking_amount":    "substr(data    , 32*5 + 1              , 32)",
        },
        "0x8cf3dec1929508e5677d7db003124e74802bfba7250a572205a9986d86ca9f1e": {
            "project": "Hashflow",
            "name": "Trade",
            "taker":            "substr(data    , 32*0 + 12 + 1         , 20)",
            "maker_asset":      "substr(data    , 32*3 + 12 + 1         , 20)",
            "taker_asset":      "substr(data    , 32*2 + 12 + 1         , 20)",
            "making_amount":    "substr(data    , 32*5 + 1              , 32)",
            "taking_amount":    "substr(data    , 32*4 + 1              , 32)",
        },
        "0x34f57786fb01682fb4eec88d340387ef01a168fe345ea5b76f709d4e560c10eb": {
            "project": "Hashflow",
            "name": "Trade",
            "taker":            "substr(data    , 32*0 + 12 + 1         , 20)",
            "maker_asset":      "substr(data    , 32*4 + 12 + 1         , 20)",
            "taker_asset":      "substr(data    , 32*3 + 12 + 1         , 20)",
            "making_amount":    "substr(data    , 32*6 + 1              , 32)",
            "taking_amount":    "substr(data    , 32*5 + 1              , 32)",
            "order_hash":       "substr(data    , 32*2 + 1              , 32)",
        },
        "0xe3a54b69726c85299f4e794bac96150af56af801be76cafd11947a1103b6308a": {
            "project": "Native",
            "name": "Swap",
            "taker":            "substr(topic1  , 12 + 1                , 20)",
            "maker_asset":      "substr(data    , 32*1 + 12 + 1         , 20)",
            "taker_asset":      "substr(data    , 32*0 + 12 + 1         , 20)",
            "making_amount":    "cast(abs(bytearray_to_int256(substr(data, 32*3 + 1, 32))) as varbinary)",
            "taking_amount":    "substr(data    , 32*2 + 1              , 32)",
            "order_hash":       "substr(data    , 32*5 + 1              , 16)",
        },
        "0x4be05c8d54f5e056ab2cfa033e9f582057001268c3e28561bb999d35d2c8f2c8": {
            "project": "Clipper",
            "name": "Swapped",
            "taker":            "substr(topic3  , 12 + 1                , 20)",
            "maker_asset":      "substr(topic2  , 12 + 1                , 20)",
            "taker_asset":      "substr(topic1  , 12 + 1                , 20)",
            "making_amount":    "substr(data    , 32*1 + 1              , 32)",
            "taking_amount":    "substr(data    , 32*0 + 1              , 32)",
        },
        "0x2170c741c41531aec20e7c107c24eecfdd15e69c9bb0a8dd37b1840b9e0b207b": {
            "project": "Swaap",
            "name": "Swap",
            "maker_asset":      "substr(topic3  , 12 + 1                , 20)",
            "taker_asset":      "substr(topic2  , 12 + 1                , 20)",
            "making_amount":    "substr(data    , 32*1 + 1              , 32)",
            "taking_amount":    "substr(data    , 32*0 + 1              , 32)",
        },
        "0x6621486d9c28838df4a87d2cca5007bc2aaf6a5b5de083b1db8faf709302c473": {
            "project": "Paraswap",
            "name": "OrderFilled",
            "maker":            "substr(topic2  , 12 + 1                , 20)",
            "taker":            "substr(topic3  , 12 + 1                , 20)",
            "maker_asset":      "substr(data    , 32*0 + 12 + 1         , 20)",
            "taker_asset":      "substr(data    , 32*2 + 12 + 1         , 20)",
            "making_amount":    "substr(data    , 32*1 + 1              , 32)",
            "taking_amount":    "substr(data    , 32*3 + 1              , 32)",
            "order_hash":       "substr(topic1  , 1                     , 32)",
        },
        "0xa07a543ab8a018198e99ca0184c93fe9050a79400a0a723441f84de1d972cc17": {
            "project": "CoWSwap",
            "name": "Trade",
            "maker":            "substr(topic1  , 12 + 1                , 20)",
            "maker_asset":      "substr(data    , 32*0 + 12 + 1         , 20)",
            "taker_asset":      "substr(data    , 32*1 + 12 + 1         , 20)",
            "making_amount":    "substr(data    , 32*2 + 1              , 32)",
            "taking_amount":    "substr(data    , 32*3 + 1              , 32)",
            "fee_asset":        "substr(data    , 32*0 + 12 + 1         , 20)",
            "fee_amount":       "substr(data    , 32*4 + 1              , 32)",
            "order_hash":       "substr(data    , 32*7 + 1              , 56)",
        },
        "0x78ad7ec0e9f89e74012afa58738b6b661c024cb0fd185ee2f616c0a28924bd66": {
            "project": "Uniswap",
            "name": "Fill",
            "maker":            "substr(topic3  , 12 + 1                , 20)",
            "taker":            "substr(topic2  , 12 + 1                , 20)",
            "order_hash":       "substr(topic1  , 1                     , 32)",
            "nonce":            "substr(data    , 1                     , 32)",
        },
        "0xadd7095becdaa725f0f33243630938c861b0bba83dfd217d4055701aa768ec2e": {
            "project": "Bebop",
            "name": "BebopOrder",
            "order_hash":       "substr(topic1  , 16 + 1                , 16)",
        }
    }
%}

-- METHODS CONFIG
{%
    set cfg = {
        "ZeroEx": {
            "0xb4be83d5": {
                "name": "fillOrder",
                "event": "0x0bcc4c97732e47d9946f229edb95f5b6323f601300e4690de719993f3c371129",
                "maker":            "substr(input   , 4 + 32*3 + 12 + 1         , 20)",
                "maker_asset":      "substr(input, 4 + bytearray_to_bigint(substr(input, 4 + 24 + 1, 8)) + bytearray_to_bigint(substr(input, 4 + bytearray_to_bigint(substr(input, 4 + 24 + 1, 8)) + 32*10 + 24 + 1, 8)) + 32*1 + 4 + 12 + 1, 20)",
                "taker_asset":      "substr(input, 4 + bytearray_to_bigint(substr(input, 4 + 24 + 1, 8)) + bytearray_to_bigint(substr(input, 4 + bytearray_to_bigint(substr(input, 4 + 24 + 1, 8)) + 32*11 + 24 + 1, 8)) + 32*1 + 4 + 12 + 1, 20)",
                "maker_max_amount": "substr(input   , 4 + 32*7 + 1              , 32)",
                "taker_max_amount": "substr(input   , 4 + 32*1 + 1              , 32)",
                "making_amount":    "substr(output  , 32*0 + 1                  , 32)",
                "taking_amount":    "substr(output  , 32*1 + 1                  , 32)",
            },
            "0xf6274f66": {
                "name": "fillLimitOrder",
                "event": "0xab614d2b738543c0ea21f56347cf696a3a0c42a7cbec3212a5ca22a4dcff2124",
                "maker":            "substr(input   , 4 + 32*5 + 12 + 1         , 20)",
                "maker_asset":      "substr(input   , 4 + 32*0 + 12 + 1         , 20)",
                "taker_asset":      "substr(input   , 4 + 32*1 + 12 + 1         , 20)",
                "maker_max_amount": "substr(input   , 4 + 32*2 + 1              , 32)",
                "taker_max_amount": "substr(input   , 4 + 32*3 + 1              , 32)",
                "making_amount":    "substr(output  , 32*1 + 1                  , 32)",
                "taking_amount":    "substr(output  , 32*0 + 1                  , 32)",
            },
            "0x414e4ccf": {
                "name": "_fillLimitOrder",
                "event": "0xab614d2b738543c0ea21f56347cf696a3a0c42a7cbec3212a5ca22a4dcff2124",
                "maker":            "substr(input   , 4 + 32*5 + 12 + 1         , 20)",
                "maker_asset":      "substr(input   , 4 + 32*0 + 12 + 1         , 20)",
                "taker_asset":      "substr(input   , 4 + 32*1 + 12 + 1         , 20)",
                "maker_max_amount": "substr(input   , 4 + 32*2 + 1              , 32)",
                "taker_max_amount": "substr(input   , 4 + 32*3 + 1              , 32)",
                "making_amount":    "substr(output  , 32*1 + 1                  , 32)",
                "taking_amount":    "substr(output  , 32*0 + 1                  , 32)",
            },
            "0x9240529c": {
                "name": "fillOrKillLimitOrder",
                "event": "0xab614d2b738543c0ea21f56347cf696a3a0c42a7cbec3212a5ca22a4dcff2124",
                "maker":            "substr(input   , 4 + 32*5 + 12 + 1         , 20)",
                "maker_asset":      "substr(input   , 4 + 32*0 + 12 + 1         , 20)",
                "taker_asset":      "substr(input   , 4 + 32*1 + 12 + 1         , 20)",
                "maker_max_amount": "substr(input   , 4 + 32*2 + 1              , 32)",
                "taker_max_amount": "substr(input   , 4 + 32*3 + 1              , 32)",
                "making_amount":    "substr(output  , 32*0 + 1                  , 32)",
            },
            "0xdac748d4": {
                "name": "fillOtcOrder",
                "event": "0xac75f773e3a92f1a02b12134d65e1f47f8a14eabe4eaf1e24624918e6a8b269f",
                "maker":            "substr(input   , 4 + 32*4 + 12 + 1         , 20)",
                "maker_asset":      "substr(input   , 4 + 32*0 + 12 + 1         , 20)",
                "taker_asset":      "substr(input   , 4 + 32*1 + 12 + 1         , 20)",
                "maker_max_amount": "substr(input   , 4 + 32*2 + 1              , 32)",
                "taker_max_amount": "substr(input   , 4 + 32*3 + 1              , 32)",
                "making_amount":    "substr(output  , 32*1 + 1                  , 32)",
                "taking_amount":    "substr(output  , 32*0 + 1                  , 32)",
            },
            "0xe4ba8439": {
                "name": "_fillOtcOrder",
                "event": "0xac75f773e3a92f1a02b12134d65e1f47f8a14eabe4eaf1e24624918e6a8b269f",
                "maker":            "substr(input   , 4 + 32*4 + 12 + 1         , 20)",
                "maker_asset":      "substr(input   , 4 + 32*0 + 12 + 1         , 20)",
                "taker_asset":      "substr(input   , 4 + 32*1 + 12 + 1         , 20)",
                "maker_max_amount": "substr(input   , 4 + 32*2 + 1              , 32)",
                "taker_max_amount": "substr(input   , 4 + 32*3 + 1              , 32)",
                "making_amount":    "substr(output  , 32*1 + 1                  , 32)",
                "taking_amount":    "substr(output  , 32*0 + 1                  , 32)",
            },
            "0xa578efaf": {
                "name": "fillOtcOrderForEth",
                "event": "0xac75f773e3a92f1a02b12134d65e1f47f8a14eabe4eaf1e24624918e6a8b269f",
                "maker":            "substr(input   , 4 + 32*4 + 12 + 1         , 20)",
                "maker_asset":      "substr(input   , 4 + 32*0 + 12 + 1         , 20)",
                "taker_asset":      "substr(input   , 4 + 32*1 + 12 + 1         , 20)",
                "maker_max_amount": "substr(input   , 4 + 32*2 + 1              , 32)",
                "taker_max_amount": "substr(input   , 4 + 32*3 + 1              , 32)",
                "making_amount":    "substr(output  , 32*1 + 1                  , 32)",
                "taking_amount":    "substr(output  , 32*0 + 1                  , 32)",
            },
            "0x706394d5": {
                "name": "fillOtcOrderWithEth",
                "event": "0xac75f773e3a92f1a02b12134d65e1f47f8a14eabe4eaf1e24624918e6a8b269f",
                "maker":            "substr(input   , 4 + 32*4 + 12 + 1         , 20)",
                "maker_asset":      "substr(input   , 4 + 32*0 + 12 + 1         , 20)",
                "taker_asset":      "substr(input   , 4 + 32*1 + 12 + 1         , 20)",
                "maker_max_amount": "substr(input   , 4 + 32*2 + 1              , 32)",
                "taker_max_amount": "substr(input   , 4 + 32*3 + 1              , 32)",
                "making_amount":    "substr(output  , 32*1 + 1                  , 32)",
                "taking_amount":    "substr(output  , 32*0 + 1                  , 32)",
            },
            "0x724d3953": {
                "name": "fillTakerSignedOtcOrderForEth",
                "event": "0xac75f773e3a92f1a02b12134d65e1f47f8a14eabe4eaf1e24624918e6a8b269f",
                "maker":            "substr(input   , 4 + 32*4 + 12 + 1         , 20)",
                "maker_asset":      "substr(input   , 4 + 32*0 + 12 + 1         , 20)",
                "taker_asset":      "substr(input   , 4 + 32*1 + 12 + 1         , 20)",
                "maker_max_amount": "substr(input   , 4 + 32*2 + 1              , 32)",
                "taker_max_amount": "substr(input   , 4 + 32*3 + 1              , 32)",
            },
            "0x4f948110": {
                "name": "fillTakerSignedOtcOrder",
                "event": "0xac75f773e3a92f1a02b12134d65e1f47f8a14eabe4eaf1e24624918e6a8b269f",
                "maker":            "substr(input   , 4 + 32*4 + 12 + 1         , 20)",
                "maker_asset":      "substr(input   , 4 + 32*0 + 12 + 1         , 20)",
                "taker_asset":      "substr(input   , 4 + 32*1 + 12 + 1         , 20)",
                "maker_max_amount": "substr(input   , 4 + 32*2 + 1              , 32)",
                "taker_max_amount": "substr(input   , 4 + 32*3 + 1              , 32)",
            },
            "0xaa77476c": {
                "name": "fillRfqOrder",
                "event": "0x829fa99d94dc4636925b38632e625736a614c154d55006b7ab6bea979c210c32",
                "maker":            "substr(input   , 4 + 32*4 + 12 + 1         , 20)",
                "maker_asset":      "substr(input   , 4 + 32*0 + 12 + 1         , 20)",
                "taker_asset":      "substr(input   , 4 + 32*1 + 12 + 1         , 20)",
                "maker_max_amount": "substr(input   , 4 + 32*2 + 1              , 32)",
                "taker_max_amount": "substr(input   , 4 + 32*3 + 1              , 32)",
                "making_amount":    "substr(output  , 32*1 + 1                  , 32)",
                "taking_amount":    "substr(output  , 32*0 + 1                  , 32)",
            },
            "0xaa6b21cd": {
                "name": "_fillRfqOrder",
                "event": "0x829fa99d94dc4636925b38632e625736a614c154d55006b7ab6bea979c210c32",
                "maker":            "substr(input   , 4 + 32*4 + 12 + 1         , 20)",
                "maker_asset":      "substr(input   , 4 + 32*0 + 12 + 1         , 20)",
                "taker_asset":      "substr(input   , 4 + 32*1 + 12 + 1         , 20)",
                "maker_max_amount": "substr(input   , 4 + 32*2 + 1              , 32)",
                "taker_max_amount": "substr(input   , 4 + 32*3 + 1              , 32)",
                "making_amount":    "substr(output  , 32*1 + 1                  , 32)",
                "taking_amount":    "substr(output  , 32*0 + 1                  , 32)",
            },
            "0xa656186b": {
                "name": "_fillRfqOrder",
                "event": "0x829fa99d94dc4636925b38632e625736a614c154d55006b7ab6bea979c210c32",
                "maker":            "substr(input   , 4 + 32*4 + 12 + 1         , 20)",
                "maker_asset":      "substr(input   , 4 + 32*0 + 12 + 1         , 20)",
                "taker_asset":      "substr(input   , 4 + 32*1 + 12 + 1         , 20)",
                "maker_max_amount": "substr(input   , 4 + 32*2 + 1              , 32)",
                "taker_max_amount": "substr(input   , 4 + 32*3 + 1              , 32)",
                "making_amount":    "substr(output  , 32*1 + 1                  , 32)",
                "taking_amount":    "substr(output  , 32*0 + 1                  , 32)",
            },
            "0x438cdfc5": {
                "name": "fillOrKillRfqOrder",
                "event": "0x829fa99d94dc4636925b38632e625736a614c154d55006b7ab6bea979c210c32",
                "maker":            "substr(input   , 4 + 32*4 + 12 + 1         , 20)",
                "maker_asset":      "substr(input   , 4 + 32*0 + 12 + 1         , 20)",
                "taker_asset":      "substr(input   , 4 + 32*1 + 12 + 1         , 20)",
                "maker_max_amount": "substr(input   , 4 + 32*2 + 1              , 32)",
                "taker_max_amount": "substr(input   , 4 + 32*3 + 1              , 32)",
                "making_amount":    "substr(output  , 32*0 + 1                  , 32)",
            },
            "0x3e228bae": {
                "name": "fillOrderNoThrow",
                "event": "0x0bcc4c97732e47d9946f229edb95f5b6323f601300e4690de719993f3c371129",
                "maker":            "substr(input   , 4 + 32*3 + 12 + 1         , 20)",
                "maker_asset":      "substr(input, 4 + bytearray_to_bigint(substr(input, 4 + 24 + 1, 8)) + bytearray_to_bigint(substr(input, 4 + bytearray_to_bigint(substr(input, 4 + 24 + 1, 8)) + 32*10 + 24 + 1, 8)) + 32*1 + 4 + 12 + 1, 20)",
                "taker_asset":      "substr(input, 4 + bytearray_to_bigint(substr(input, 4 + 24 + 1, 8)) + bytearray_to_bigint(substr(input, 4 + bytearray_to_bigint(substr(input, 4 + 24 + 1, 8)) + 32*11 + 24 + 1, 8)) + 32*1 + 4 + 12 + 1, 20)",
                "maker_max_amount": "substr(input   , 4 + 32*7 + 1              , 32)",
                "taker_max_amount": "substr(input   , 4 + 32*1 + 1              , 32)",
                "making_amount":    "substr(output  , 32*0 + 1                  , 32)",
                "taking_amount":    "substr(output  , 32*1 + 1                  , 32)",
            },
            "0x9b44d556": {
                "name": "fillOrder",
                "event": "0x6869791f0a34781b29882982cc39e882768cf2c96995c2a110c577c53bc932d5",
                "maker":            "substr(input   , 4 + 32*3 + 12 + 1         , 20)",
                "maker_asset":      "substr(input, 4 + bytearray_to_bigint(substr(input, 4 + 24 + 1, 8)) + bytearray_to_bigint(substr(input, 4 + bytearray_to_bigint(substr(input, 4 + 24 + 1, 8)) + 32*10 + 24 + 1, 8)) + 32*1 + 4 + 12 + 1, 20)",
                "taker_asset":      "substr(input, 4 + bytearray_to_bigint(substr(input, 4 + 24 + 1, 8)) + bytearray_to_bigint(substr(input, 4 + bytearray_to_bigint(substr(input, 4 + 24 + 1, 8)) + 32*11 + 24 + 1, 8)) + 32*1 + 4 + 12 + 1, 20)",
                "maker_max_amount": "substr(input   , 4 + 32*7 + 1              , 32)",
                "taker_max_amount": "substr(input   , 4 + 32*8 + 1              , 32)",
                "making_amount":    "substr(output  , 32*0 + 1                  , 32)",
                "taking_amount":    "substr(output  , 32*1 + 1                  , 32)",
            },
        },
        "Hashflow": {
            "0x1e9a2e92": {
                "name": "tradeSingleHop",
                "event": "0xb709ddcc6550418e9b89df1f4938071eeaa3f6376309904c77e15d46b16066f5",
                "maker":            "substr(input   , 4 + 32*2 + 12 + 1         , 20)",
                "maker_external":   "substr(input   , 4 + 32*3 + 12 + 1         , 20)",
                "maker_asset":      "substr(input   , 4 + 32*7 + 12 + 1         , 20)",
                "taker_asset":      "substr(input   , 4 + 32*6 + 12 + 1         , 20)",
                "maker_max_amount": "substr(input   , 4 + 32*10 + 1             , 32)",
                "taker_max_amount": "substr(input   , 4 + 32*9 + 1              , 32)",
            },
            "0xf0210929": {
                "name": "tradeSingleHop",
                "event": "0x8cf3dec1929508e5677d7db003124e74802bfba7250a572205a9986d86ca9f1e",
                "maker":            "substr(input   , 4 + 32*1 + 12 + 1         , 20)",
                "maker_external":   "substr(input   , 4 + 32*2 + 12 + 1         , 20)",
                "maker_asset":      "substr(input   , 4 + 32*6 + 12 + 1         , 20)",
                "taker_asset":      "substr(input   , 4 + 32*5 + 12 + 1         , 20)",
                "maker_max_amount": "substr(input   , 4 + 32*9 + 1              , 32)",
                "taker_max_amount": "substr(input   , 4 + 32*8 + 1              , 32)",
            },
            "0xc52ac720": {
                "name": "tradeRFQT",
                "event": "0x34f57786fb01682fb4eec88d340387ef01a168fe345ea5b76f709d4e560c10eb",
                "maker":            "substr(input   , 4 + 32*1 + 12 + 1         , 20)",
                "maker_external":   "substr(input   , 4 + 32*2 + 12 + 1         , 20)",
                "maker_asset":      "substr(input   , 4 + 32*6 + 12 + 1         , 20)",
                "taker_asset":      "substr(input   , 4 + 32*5 + 12 + 1         , 20)",
                "maker_max_amount": "substr(input   , 4 + 32*9 + 1              , 32)",
                "taker_max_amount": "substr(input   , 4 + 32*8 + 1              , 32)",
                "order_hash":       "substr(input   , 4 + 32*12 + 1             , 32)",
            },
        },
        "Native": {
            "0xd025fdfa": {
                "name": "swap",
                "event": "0xe3a54b69726c85299f4e794bac96150af56af801be76cafd11947a1103b6308a",
                "maker":            "substr(input   , 4 + 32*7 + 12 + 1         , 20)",
                "maker_asset":      "substr(input   , 4 + 32*10 + 12 + 1        , 20)",
                "taker_asset":      "substr(input   , 4 + 32*11 + 12 + 1        , 20)",
                "making_amount":    "cast(abs(bytearray_to_int256(substr(output, 32*0 + 1, 32))) as varbinary)",
                "taking_amount":    "substr(output  , 32*1 + 1                  , 32)",
            },
        },
        "Clipper": {
            "0x2b651a6c": {
                "name": "swap",
                "event": "0x4be05c8d54f5e056ab2cfa033e9f582057001268c3e28561bb999d35d2c8f2c8",
                "taker":            "substr(input   , 4 + 32*5 + 12 + 1         , 20)",
                "maker_asset":      "substr(input   , 4 + 32*1 + 12 + 1         , 20)",
                "taker_asset":      "substr(input   , 4 + 32*0 + 12 + 1         , 20)",
                "maker_max_amount": "substr(input   , 4 + 32*3 + 1              , 32)",
                "taker_max_amount": "substr(input   , 4 + 32*2 + 1              , 32)",
            },
            "0x4cb6864c": {
                "name": "sellTokenForEth",
                "event": "0x4be05c8d54f5e056ab2cfa033e9f582057001268c3e28561bb999d35d2c8f2c8",
                "taker":            "substr(input   , 4 + 32*4 + 12 + 1         , 20)",
                "taker_asset":      "substr(input   , 4 + 32*0 + 12 + 1         , 20)",
                "maker_max_amount": "substr(input   , 4 + 32*2 + 1              , 32)",
                "taker_max_amount": "substr(input   , 4 + 32*1 + 1              , 32)",
            },
            "0x27a9b424": {
                "name": "sellEthForToken",
                "event": "0x4be05c8d54f5e056ab2cfa033e9f582057001268c3e28561bb999d35d2c8f2c8",
                "taker":            "substr(input   , 4 + 32*4 + 12 + 1         , 20)",
                "maker_asset":      "substr(input   , 4 + 32*0 + 12 + 1         , 20)",
                "taker_max_amount": "substr(input   , 4 + 32*2 + 1              , 32)",
                "maker_max_amount": "substr(input   , 4 + 32*1 + 1              , 32)",
            },
            "0x3b26e4eb": {
                "name": "transmitAndSwap",
                "event": "0x4be05c8d54f5e056ab2cfa033e9f582057001268c3e28561bb999d35d2c8f2c8",
                "taker":            "substr(input   , 4 + 32*5 + 12 + 1         , 20)",
                "maker_asset":      "substr(input   , 4 + 32*1 + 12 + 1         , 20)",
                "taker_asset":      "substr(input   , 4 + 32*0 + 12 + 1         , 20)",
                "maker_max_amount": "substr(input   , 4 + 32*3 + 1              , 32)",
                "taker_max_amount": "substr(input   , 4 + 32*2 + 1              , 32)",
            },
        },
        "Swaap": {
            "0x52bbbe29": {
                "name": "swap",
                "event": "0x2170c741c41531aec20e7c107c24eecfdd15e69c9bb0a8dd37b1840b9e0b207b",
                "taker":            "substr(input   , 4 + 32*3 + 12 + 1         , 20)",
                "maker_asset":      "substr(input   , 4 + 32*10 + 12 + 1        , 20)",
                "taker_asset":      "substr(input   , 4 + 32*9 + 12 + 1         , 20)",
                "taker_max_amount": "substr(input   , 4 + 32*11 + 1             , 32)",
                "making_amount":    "cast(abs(bytearray_to_int256(substr(output, 32*0 + 1, 32))) as varbinary)",
                "taking_amount":    "if(length(output) > 32, substr(output, 32 * 1 + 1, 32))",
            },
        },
        "Paraswap": {
            "0x98f9b46b": {
                "name": "fillOrder",
                "event": "0x6621486d9c28838df4a87d2cca5007bc2aaf6a5b5de083b1db8faf709302c473",
                "maker":            "substr(input   , 4 + 32*4 + 12 + 1         , 20)",
                "maker_asset":      "substr(input   , 4 + 32*2 + 12 + 1         , 20)",
                "taker_asset":      "substr(input   , 4 + 32*3 + 12 + 1         , 20)",
                "maker_max_amount": "substr(input   , 4 + 32*6 + 1              , 32)",
                "taker_max_amount": "substr(input   , 4 + 32*7 + 1              , 32)",
            },
            "0xc88ae6dc": {
                "name": "partialFillOrder",
                "event": "0x6621486d9c28838df4a87d2cca5007bc2aaf6a5b5de083b1db8faf709302c473",
                "maker":            "substr(input   , 4 + 32*4 + 12 + 1         , 20)",
                "maker_asset":      "substr(input   , 4 + 32*2 + 12 + 1         , 20)",
                "taker_asset":      "substr(input   , 4 + 32*3 + 12 + 1         , 20)",
                "maker_max_amount": "substr(input   , 4 + 32*6 + 1              , 32)",
                "taker_max_amount": "substr(input   , 4 + 32*7 + 1              , 32)",
                "making_amount":    "substr(output  , 32*0 + 1                  , 32)",
            },
            "0x24abf828": {
                "name": "partialFillOrderWithTarget",
                "event": "0x6621486d9c28838df4a87d2cca5007bc2aaf6a5b5de083b1db8faf709302c473",
                "maker":            "substr(input   , 4 + 32*4 + 12 + 1         , 20)",
                "maker_asset":      "substr(input   , 4 + 32*2 + 12 + 1         , 20)",
                "taker_asset":      "substr(input   , 4 + 32*3 + 12 + 1         , 20)",
                "maker_max_amount": "substr(input   , 4 + 32*6 + 1              , 32)",
                "taker_max_amount": "substr(input   , 4 + 32*7 + 1              , 32)",
                "making_amount":    "substr(output  , 32*0 + 1                  , 32)",
            },
        },
        "CoWSwap": {
            "0x13d79a0b": {
                "name": "settle",
                "event": "0xa07a543ab8a018198e99ca0184c93fe9050a79400a0a723441f84de1d972cc17",
                "number": "coalesce(try(bytearray_to_bigint(substr(input, 4 + bytearray_to_bigint(substr(input, 4 + 32*2 + 24 + 1, 8)) + 24 + 1, 8))), 1)",
                "_order_beginning": "4 + bytearray_to_bigint(substr(input, 4 + 32*2 + 24 + 1, 8)) + 32*1 + bytearray_to_bigint(substr(input, 4 + bytearray_to_bigint(substr(input, 4 + 32*2 + 24 + 1, 8)) + 32*x + 24 + 1, 8))",
                "maker_asset":      "substr(input, 4 + bytearray_to_bigint(substr(input, 4 + 24 + 1, 8)) + 32*(bytearray_to_bigint(substr(input, 4 + bytearray_to_bigint(substr(input, 4 + 32*2 + 24 + 1, 8)) + 32*1 + bytearray_to_bigint(substr(input, 4 + bytearray_to_bigint(substr(input, 4 + 32*2 + 24 + 1, 8)) + 32*x + 24 + 1, 8)) + 32*0 + 24 + 1, 8)) + 1) + 12 + 1, 20)",
                "taker_asset":      "substr(input, 4 + bytearray_to_bigint(substr(input, 4 + 24 + 1, 8)) + 32*(bytearray_to_bigint(substr(input, 4 + bytearray_to_bigint(substr(input, 4 + 32*2 + 24 + 1, 8)) + 32*1 + bytearray_to_bigint(substr(input, 4 + bytearray_to_bigint(substr(input, 4 + 32*2 + 24 + 1, 8)) + 32*x + 24 + 1, 8)) + 32*1 + 24 + 1, 8)) + 1) + 12 + 1, 20)",
                "receiver":         "substr(input, 4 + bytearray_to_bigint(substr(input, 4 + 32*2 + 24 + 1, 8)) + 32*1 + bytearray_to_bigint(substr(input, 4 + bytearray_to_bigint(substr(input, 4 + 32*2 + 24 + 1, 8)) + 32*x + 24 + 1, 8)) + 32*2 + 12 + 1, 20)",
                "maker_max_amount": "substr(input, 4 + bytearray_to_bigint(substr(input, 4 + 32*2 + 24 + 1, 8)) + 32*1 + bytearray_to_bigint(substr(input, 4 + bytearray_to_bigint(substr(input, 4 + 32*2 + 24 + 1, 8)) + 32*x + 24 + 1, 8)) + 32*3 + 1, 32)",
                "taker_max_amount": "substr(input, 4 + bytearray_to_bigint(substr(input, 4 + 32*2 + 24 + 1, 8)) + 32*1 + bytearray_to_bigint(substr(input, 4 + bytearray_to_bigint(substr(input, 4 + 32*2 + 24 + 1, 8)) + 32*x + 24 + 1, 8)) + 32*4 + 1, 32)",
                "deadline":         "substr(input, 4 + bytearray_to_bigint(substr(input, 4 + 32*2 + 24 + 1, 8)) + 32*1 + bytearray_to_bigint(substr(input, 4 + bytearray_to_bigint(substr(input, 4 + 32*2 + 24 + 1, 8)) + 32*x + 24 + 1, 8)) + 32*5 + 24, 8)",
                "fee_amount":       "substr(input, 4 + bytearray_to_bigint(substr(input, 4 + 32*2 + 24 + 1, 8)) + 32*1 + bytearray_to_bigint(substr(input, 4 + bytearray_to_bigint(substr(input, 4 + 32*2 + 24 + 1, 8)) + 32*x + 24 + 1, 8)) + 32*7 + 1, 32)",
                "making_amount":    "substr(input, 4 + bytearray_to_bigint(substr(input, 4 + 32*2 + 24 + 1, 8)) + 32*1 + bytearray_to_bigint(substr(input, 4 + bytearray_to_bigint(substr(input, 4 + 32*2 + 24 + 1, 8)) + 32*x + 24 + 1, 8)) + 32*9 + 1, 32)",
            },
        },
        "UniswapX": {
            "0x3f62192e": {
                "name": "execute",
                "event": "0x78ad7ec0e9f89e74012afa58738b6b661c024cb0fd185ee2f616c0a28924bd66",
                "maker":            "substr(input    , 4 + 32*15 + 12 + 1       , 20)",
                "receiver":         "substr(input, 4 + 32*4 + bytearray_to_bigint(substr(input, 4 + 32*13 + 24 + 1, 8)) + 32*5 + 12 + 1, 20)",
                "maker_asset":      "substr(input    , 4 + 32*10 + 12 + 1       , 20)",
                "taker_asset":      "substr(input, 4 + 32*4 + bytearray_to_bigint(substr(input, 4 + 32*13 + 24 + 1, 8)) + 32*2 + 12 + 1, 20)",
                "maker_max_amount": "substr(input    , 4 + 32*11 + 1            , 32)",
                "maker_min_amount": "substr(input    , 4 + 32*12 + 1            , 32)",
                "taker_max_amount": "substr(input, 4 + 32*4 + bytearray_to_bigint(substr(input, 4 + 32*13 + 24 + 1, 8)) + 32*3 + 1, 32)",
                "taker_min_amount": "substr(input, 4 + 32*4 + bytearray_to_bigint(substr(input, 4 + 32*13 + 24 + 1, 8)) + 32*4 + 1, 32)",
                "start":            "substr(input    , 4 + 32*6 + 1             , 32)",
                "end":              "substr(input    , 4 + 32*7 + 1             , 32)",
                "deadline":         "cast(abs(bytearray_to_int256(substr(input, 4 + 32*17 + 1, 32))) as varbinary)",
                "nonce":            "substr(input    , 4 + 32*16 + 1            , 32)",
                "fee_asset":        "if(bytearray_to_bigint(substr(input, 4 + 32*4 + bytearray_to_bigint(substr(input, 4 + 32*13 + 24 + 1, 8)) + 32*1 + 24 + 1, 8)) > 1, substr(input, 4 + 32*4 + bytearray_to_bigint(substr(input, 4 + 32*13 + 24 + 1, 8)) + 32*6 + 12 + 1, 20))",
                "fee_max_amount":   "if(bytearray_to_bigint(substr(input, 4 + 32*4 + bytearray_to_bigint(substr(input, 4 + 32*13 + 24 + 1, 8)) + 32*1 + 24 + 1, 8)) > 1, substr(input, 4 + 32*4 + bytearray_to_bigint(substr(input, 4 + 32*13 + 24 + 1, 8)) + 32*7 + 1, 32))",
                "fee_min_amount":   "if(bytearray_to_bigint(substr(input, 4 + 32*4 + bytearray_to_bigint(substr(input, 4 + 32*13 + 24 + 1, 8)) + 32*1 + 24 + 1, 8)) > 1, substr(input, 4 + 32*4 + bytearray_to_bigint(substr(input, 4 + 32*13 + 24 + 1, 8)) + 32*8 + 1, 32))",
                "fee_receiver":     "if(bytearray_to_bigint(substr(input, 4 + 32*4 + bytearray_to_bigint(substr(input, 4 + 32*13 + 24 + 1, 8)) + 32*1 + 24 + 1, 8)) > 1, substr(input, 4 + 32*4 + bytearray_to_bigint(substr(input, 4 + 32*13 + 24 + 1, 8)) + 32*9 + 12 + 1, 20))",
            },
            "0x0d335884": {
                "name": "executeWithCallback",
                "event": "0x78ad7ec0e9f89e74012afa58738b6b661c024cb0fd185ee2f616c0a28924bd66",
                "maker":            "substr(input    , 4 + 32*16 + 12 + 1       , 20)",
                "receiver":         "substr(input, 4 + 32*5 + bytearray_to_bigint(substr(input, 4 + 32*14 + 24 + 1, 8)) + 32*5 + 12 + 1, 20)",
                "maker_asset":      "substr(input    , 4 + 32*11 + 12 + 1       , 20)",
                "taker_asset":      "substr(input, 4 + 32*5 + bytearray_to_bigint(substr(input, 4 + 32*14 + 24 + 1, 8)) + 32*2 + 12 + 1, 20)",
                "maker_max_amount": "substr(input    , 4 + 32*12 + 1            , 32)",
                "maker_min_amount": "substr(input    , 4 + 32*13 + 1            , 32)",
                "taker_max_amount": "substr(input, 4 + 32*5 + bytearray_to_bigint(substr(input, 4 + 32*14 + 24 + 1, 8)) + 32*3 + 1, 32)",
                "taker_min_amount": "substr(input, 4 + 32*5 + bytearray_to_bigint(substr(input, 4 + 32*14 + 24 + 1, 8)) + 32*4 + 1, 32)",
                "start":            "substr(input    , 4 + 32*7 + 1             , 32)",
                "end":              "substr(input    , 4 + 32*8 + 1             , 32)",
                "deadline":         "cast(abs(bytearray_to_int256(substr(input, 4 + 32*18 + 1, 32))) as varbinary)",
                "nonce":            "substr(input    , 4 + 32*17 + 1            , 32)",
                "fee_asset":        "if(bytearray_to_bigint(substr(input, 4 + 32*5 + bytearray_to_bigint(substr(input, 4 + 32*14 + 24 + 1, 8)) + 32*1 + 24 + 1, 8)) > 1, substr(input, 4 + 32*5 + bytearray_to_bigint(substr(input, 4 + 32*14 + 24 + 1, 8)) + 32*6 + 12 + 1, 20))",
                "fee_max_amount":   "if(bytearray_to_bigint(substr(input, 4 + 32*5 + bytearray_to_bigint(substr(input, 4 + 32*14 + 24 + 1, 8)) + 32*1 + 24 + 1, 8)) > 1, substr(input, 4 + 32*5 + bytearray_to_bigint(substr(input, 4 + 32*14 + 24 + 1, 8)) + 32*7 + 1, 32))",
                "fee_min_amount":   "if(bytearray_to_bigint(substr(input, 4 + 32*5 + bytearray_to_bigint(substr(input, 4 + 32*14 + 24 + 1, 8)) + 32*1 + 24 + 1, 8)) > 1, substr(input, 4 + 32*5 + bytearray_to_bigint(substr(input, 4 + 32*14 + 24 + 1, 8)) + 32*8 + 1, 32))",
                "fee_receiver":     "if(bytearray_to_bigint(substr(input, 4 + 32*5 + bytearray_to_bigint(substr(input, 4 + 32*14 + 24 + 1, 8)) + 32*1 + 24 + 1, 8)) > 1, substr(input, 4 + 32*5 + bytearray_to_bigint(substr(input, 4 + 32*14 + 24 + 1, 8)) + 32*9 + 12 + 1, 20))",
            },
            "0x0d7a16c3": {
                "name": "executeBatch",
                "event": "0x78ad7ec0e9f89e74012afa58738b6b661c024cb0fd185ee2f616c0a28924bd66",
                "number":           "coalesce(try(bytearray_to_bigint(substr(input, 4 + bytearray_to_bigint(substr(input, 4 + 24 + 1, 8)) + 24 + 1, 8))), 1)",
                "_order_beginning": "4 + 32*2 + bytearray_to_bigint(substr(input, 4 + 32*2 + 32*(x - 1) + 24 + 1, 8))",
                "maker":            "substr(input, 4 + 32*2 + bytearray_to_bigint(substr(input, 4 + 32*2 + 32*(x - 1) + 24 + 1, 8)) + 32*14 + 12 + 1, 20)",
                "receiver":         "substr(input, 4 + 32*2 + bytearray_to_bigint(substr(input, 4 + 32*2 + 32*(x - 1) + 24 + 1, 8)) + bytearray_to_bigint(substr(input, 4 + 32*2 + bytearray_to_bigint(substr(input, 4 + 32*2 + 32*(x - 1) + 24 + 1, 8)) + 32*12 + 24 + 1, 8)) + 32*8 + 12 + 1, 20)",
                "maker_asset":      "substr(input, 4 + 32*2 + bytearray_to_bigint(substr(input, 4 + 32*2 + 32*(x - 1) + 24 + 1, 8)) + 32*9 + 12 + 1, 20)",
                "taker_asset":      "substr(input, 4 + 32*2 + bytearray_to_bigint(substr(input, 4 + 32*2 + 32*(x - 1) + 24 + 1, 8)) + bytearray_to_bigint(substr(input, 4 + 32*2 + bytearray_to_bigint(substr(input, 4 + 32*2 + 32*(x - 1) + 24 + 1, 8)) + 32*12 + 24 + 1, 8)) + 32*5 + 12 + 1, 20)",
                "maker_max_amount": "substr(input, 4 + 32*2 + bytearray_to_bigint(substr(input, 4 + 32*2 + 32*(x - 1) + 24 + 1, 8)) + 32*10 + 1, 32)",
                "maker_min_amount": "substr(input, 4 + 32*2 + bytearray_to_bigint(substr(input, 4 + 32*2 + 32*(x - 1) + 24 + 1, 8)) + 32*11 + 1, 32)",
                "taker_max_amount": "substr(input, 4 + 32*2 + bytearray_to_bigint(substr(input, 4 + 32*2 + 32*(x - 1) + 24 + 1, 8)) + bytearray_to_bigint(substr(input, 4 + 32*2 + bytearray_to_bigint(substr(input, 4 + 32*2 + 32*(x - 1) + 24 + 1, 8)) + 32*12 + 24 + 1, 8)) + 32*6 + 1, 32)",
                "taker_min_amount": "substr(input, 4 + 32*2 + bytearray_to_bigint(substr(input, 4 + 32*2 + 32*(x - 1) + 24 + 1, 8)) + bytearray_to_bigint(substr(input, 4 + 32*2 + bytearray_to_bigint(substr(input, 4 + 32*2 + 32*(x - 1) + 24 + 1, 8)) + 32*12 + 24 + 1, 8)) + 32*7 + 1, 32)",
                "start":            "substr(input, 4 + 32*2 + bytearray_to_bigint(substr(input, 4 + 32*2 + 32*(x - 1) + 24 + 1, 8)) + 32*5 + 1, 20)",
                "end":              "substr(input, 4 + 32*2 + bytearray_to_bigint(substr(input, 4 + 32*2 + 32*(x - 1) + 24 + 1, 8)) + 32*6 + 1, 20)",
                "deadline":         "cast(abs(bytearray_to_int256(substr(input, 4 + 32*2 + bytearray_to_bigint(substr(input, 4 + 32*2 + 32*(x - 1) + 24 + 1, 8)) + 32*16 + 1, 32))) as varbinary)",
                "nonce":            "substr(input, 4 + 32*2 + bytearray_to_bigint(substr(input, 4 + 32*2 + 32*(x - 1) + 24 + 1, 8)) + 32*15 + 1, 32)",
                "fee_asset":        "if(bytearray_to_bigint(substr(input, 4 + 32*2 + bytearray_to_bigint(substr(input, 4 + 32*2 + 32*(x - 1) + 24 + 1, 8)) + bytearray_to_bigint(substr(input, 4 + 32*2 + bytearray_to_bigint(substr(input, 4 + 32*2 + 32*(x - 1) + 24 + 1, 8)) + 32*12 + 24 + 1, 8)) + 32*4 + 24 + 1, 8)) > 1, substr(input, 4 + 32*2 + bytearray_to_bigint(substr(input, 4 + 32*2 + 32*(x - 1) + 24 + 1, 8)) + bytearray_to_bigint(substr(input, 4 + 32*2 + bytearray_to_bigint(substr(input, 4 + 32*2 + 32*(x - 1) + 24 + 1, 8)) + 32*12 + 24 + 1, 8)) + 32*9 + 12 + 1, 20))",
                "fee_max_amount":   "if(bytearray_to_bigint(substr(input, 4 + 32*2 + bytearray_to_bigint(substr(input, 4 + 32*2 + 32*(x - 1) + 24 + 1, 8)) + bytearray_to_bigint(substr(input, 4 + 32*2 + bytearray_to_bigint(substr(input, 4 + 32*2 + 32*(x - 1) + 24 + 1, 8)) + 32*12 + 24 + 1, 8)) + 32*4 + 24 + 1, 8)) > 1, substr(input, 4 + 32*2 + bytearray_to_bigint(substr(input, 4 + 32*2 + 32*(x - 1) + 24 + 1, 8)) + bytearray_to_bigint(substr(input, 4 + 32*2 + bytearray_to_bigint(substr(input, 4 + 32*2 + 32*(x - 1) + 24 + 1, 8)) + 32*12 + 24 + 1, 8)) + 32*10 + 1, 32))",
                "fee_min_amount":   "if(bytearray_to_bigint(substr(input, 4 + 32*2 + bytearray_to_bigint(substr(input, 4 + 32*2 + 32*(x - 1) + 24 + 1, 8)) + bytearray_to_bigint(substr(input, 4 + 32*2 + bytearray_to_bigint(substr(input, 4 + 32*2 + 32*(x - 1) + 24 + 1, 8)) + 32*12 + 24 + 1, 8)) + 32*4 + 24 + 1, 8)) > 1, substr(input, 4 + 32*2 + bytearray_to_bigint(substr(input, 4 + 32*2 + 32*(x - 1) + 24 + 1, 8)) + bytearray_to_bigint(substr(input, 4 + 32*2 + bytearray_to_bigint(substr(input, 4 + 32*2 + 32*(x - 1) + 24 + 1, 8)) + 32*12 + 24 + 1, 8)) + 32*11 + 1, 32))",
                "fee_receiver":     "if(bytearray_to_bigint(substr(input, 4 + 32*2 + bytearray_to_bigint(substr(input, 4 + 32*2 + 32*(x - 1) + 24 + 1, 8)) + bytearray_to_bigint(substr(input, 4 + 32*2 + bytearray_to_bigint(substr(input, 4 + 32*2 + 32*(x - 1) + 24 + 1, 8)) + 32*12 + 24 + 1, 8)) + 32*4 + 24 + 1, 8)) > 1, substr(input, 4 + 32*2 + bytearray_to_bigint(substr(input, 4 + 32*2 + 32*(x - 1) + 24 + 1, 8)) + bytearray_to_bigint(substr(input, 4 + 32*2 + bytearray_to_bigint(substr(input, 4 + 32*2 + 32*(x - 1) + 24 + 1, 8)) + 32*12 + 24 + 1, 8)) + 32*12 + 12 + 1, 20))",
            },
            "0x13fb72c7": {
                "name": "executeBatchWithCallback",
                "event": "0x78ad7ec0e9f89e74012afa58738b6b661c024cb0fd185ee2f616c0a28924bd66",
                "number":           "coalesce(try(bytearray_to_bigint(substr(input, 4 + bytearray_to_bigint(substr(input, 4 + 24 + 1, 8)) + 24 + 1, 8))), 1)",
                "_order_beginning": "4 + bytearray_to_bigint(substr(input, 4 + 24 + 1, 8)) + 32*1 + bytearray_to_bigint(substr(input, 4 + bytearray_to_bigint(substr(input, 4 + 24 + 1, 8)) + 32*1 + 32*(x - 1) + 24 + 1, 8))",
                "maker":            "substr(input, 4 + bytearray_to_bigint(substr(input, 4 + 24 + 1, 8)) + 32*1 + bytearray_to_bigint(substr(input, 4 + bytearray_to_bigint(substr(input, 4 + 24 + 1, 8)) + 32*1 + 32*(x - 1) + 24 + 1, 8)) + 32*14 + 12 + 1, 20)",
                "receiver":         "substr(input, 4 + bytearray_to_bigint(substr(input, 4 + 24 + 1, 8)) + 32*1 + bytearray_to_bigint(substr(input, 4 + bytearray_to_bigint(substr(input, 4 + 24 + 1, 8)) + 32*1 + 32*(x - 1) + 24 + 1, 8)) + bytearray_to_bigint(substr(input, 4 + bytearray_to_bigint(substr(input, 4 + 24 + 1, 8)) + 32*1 + bytearray_to_bigint(substr(input, 4 + bytearray_to_bigint(substr(input, 4 + 24 + 1, 8)) + 32*1 + 32*(x - 1) + 24 + 1, 8)) + 32*12 + 24 + 1, 8)) + 32*8 + 12 + 1, 20)",
                "maker_asset":      "substr(input, 4 + bytearray_to_bigint(substr(input, 4 + 24 + 1, 8)) + 32*1 + bytearray_to_bigint(substr(input, 4 + bytearray_to_bigint(substr(input, 4 + 24 + 1, 8)) + 32*1 + 32*(x - 1) + 24 + 1, 8)) + 32*9 + 12 + 1, 20)",
                "taker_asset":      "substr(input, 4 + bytearray_to_bigint(substr(input, 4 + 24 + 1, 8)) + 32*1 + bytearray_to_bigint(substr(input, 4 + bytearray_to_bigint(substr(input, 4 + 24 + 1, 8)) + 32*1 + 32*(x - 1) + 24 + 1, 8)) + bytearray_to_bigint(substr(input, 4 + bytearray_to_bigint(substr(input, 4 + 24 + 1, 8)) + 32*1 + bytearray_to_bigint(substr(input, 4 + bytearray_to_bigint(substr(input, 4 + 24 + 1, 8)) + 32*1 + 32*(x - 1) + 24 + 1, 8)) + 32*12 + 24 + 1, 8)) + 32*5 + 12 + 1, 20)",
                "maker_max_amount": "substr(input, 4 + bytearray_to_bigint(substr(input, 4 + 24 + 1, 8)) + 32*1 + bytearray_to_bigint(substr(input, 4 + bytearray_to_bigint(substr(input, 4 + 24 + 1, 8)) + 32*1 + 32*(x - 1) + 24 + 1, 8)) + 32*10 + 1, 32)",
                "maker_min_amount": "substr(input, 4 + bytearray_to_bigint(substr(input, 4 + 24 + 1, 8)) + 32*1 + bytearray_to_bigint(substr(input, 4 + bytearray_to_bigint(substr(input, 4 + 24 + 1, 8)) + 32*1 + 32*(x - 1) + 24 + 1, 8)) + 32*11 + 1, 32)",
                "taker_max_amount": "substr(input, 4 + bytearray_to_bigint(substr(input, 4 + 24 + 1, 8)) + 32*1 + bytearray_to_bigint(substr(input, 4 + bytearray_to_bigint(substr(input, 4 + 24 + 1, 8)) + 32*1 + 32*(x - 1) + 24 + 1, 8)) + bytearray_to_bigint(substr(input, 4 + bytearray_to_bigint(substr(input, 4 + 24 + 1, 8)) + 32*1 + bytearray_to_bigint(substr(input, 4 + bytearray_to_bigint(substr(input, 4 + 24 + 1, 8)) + 32*1 + 32*(x - 1) + 24 + 1, 8)) + 32*12 + 24 + 1, 8)) + 32*6 + 1, 32)",
                "taker_min_amount": "substr(input, 4 + bytearray_to_bigint(substr(input, 4 + 24 + 1, 8)) + 32*1 + bytearray_to_bigint(substr(input, 4 + bytearray_to_bigint(substr(input, 4 + 24 + 1, 8)) + 32*1 + 32*(x - 1) + 24 + 1, 8)) + bytearray_to_bigint(substr(input, 4 + bytearray_to_bigint(substr(input, 4 + 24 + 1, 8)) + 32*1 + bytearray_to_bigint(substr(input, 4 + bytearray_to_bigint(substr(input, 4 + 24 + 1, 8)) + 32*1 + 32*(x - 1) + 24 + 1, 8)) + 32*12 + 24 + 1, 8)) + 32*7 + 1, 32)",
                "start":            "substr(input, 4 + bytearray_to_bigint(substr(input, 4 + 24 + 1, 8)) + 32*1 + bytearray_to_bigint(substr(input, 4 + bytearray_to_bigint(substr(input, 4 + 24 + 1, 8)) + 32*1 + 32*(x - 1) + 24 + 1, 8)) + 32*5 + 1, 20)",
                "end":              "substr(input, 4 + bytearray_to_bigint(substr(input, 4 + 24 + 1, 8)) + 32*1 + bytearray_to_bigint(substr(input, 4 + bytearray_to_bigint(substr(input, 4 + 24 + 1, 8)) + 32*1 + 32*(x - 1) + 24 + 1, 8)) + 32*6 + 1, 20)",
                "deadline":         "cast(abs(bytearray_to_int256(substr(input, 4 + bytearray_to_bigint(substr(input, 4 + 24 + 1, 8)) + 32*1 + bytearray_to_bigint(substr(input, 4 + bytearray_to_bigint(substr(input, 4 + 24 + 1, 8)) + 32*1 + 32*(x - 1) + 24 + 1, 8)) + 32*16 + 1, 32))) as varbinary)",
                "nonce":            "substr(input, 4 + bytearray_to_bigint(substr(input, 4 + 24 + 1, 8)) + 32*1 + bytearray_to_bigint(substr(input, 4 + bytearray_to_bigint(substr(input, 4 + 24 + 1, 8)) + 32*1 + 32*(x - 1) + 24 + 1, 8)) + 32*15 + 1, 32)",
                "fee_asset":        "if(bytearray_to_bigint(substr(input, 4 + bytearray_to_bigint(substr(input, 4 + 24 + 1, 8)) + 32*1 + bytearray_to_bigint(substr(input, 4 + bytearray_to_bigint(substr(input, 4 + 24 + 1, 8)) + 32*1 + 32*(x - 1) + 24 + 1, 8)) + bytearray_to_bigint(substr(input, 4 + bytearray_to_bigint(substr(input, 4 + 24 + 1, 8)) + 32*1 + bytearray_to_bigint(substr(input, 4 + bytearray_to_bigint(substr(input, 4 + 24 + 1, 8)) + 32*1 + 32*(x - 1) + 24 + 1, 8)) + 32*12 + 24 + 1, 8)) + 32*4 + 24 + 1, 8)) > 1, substr(input, 4 + bytearray_to_bigint(substr(input, 4 + 24 + 1, 8)) + 32*1 + bytearray_to_bigint(substr(input, 4 + bytearray_to_bigint(substr(input, 4 + 24 + 1, 8)) + 32*1 + 32*(x - 1) + 24 + 1, 8)) + bytearray_to_bigint(substr(input, 4 + bytearray_to_bigint(substr(input, 4 + 24 + 1, 8)) + 32*1 + bytearray_to_bigint(substr(input, 4 + bytearray_to_bigint(substr(input, 4 + 24 + 1, 8)) + 32*1 + 32*(x - 1) + 24 + 1, 8)) + 32*12 + 24 + 1, 8)) + 32*9 + 12 + 1, 20))",
                "fee_max_amount":   "if(bytearray_to_bigint(substr(input, 4 + bytearray_to_bigint(substr(input, 4 + 24 + 1, 8)) + 32*1 + bytearray_to_bigint(substr(input, 4 + bytearray_to_bigint(substr(input, 4 + 24 + 1, 8)) + 32*1 + 32*(x - 1) + 24 + 1, 8)) + bytearray_to_bigint(substr(input, 4 + bytearray_to_bigint(substr(input, 4 + 24 + 1, 8)) + 32*1 + bytearray_to_bigint(substr(input, 4 + bytearray_to_bigint(substr(input, 4 + 24 + 1, 8)) + 32*1 + 32*(x - 1) + 24 + 1, 8)) + 32*12 + 24 + 1, 8)) + 32*4 + 24 + 1, 8)) > 1, substr(input, 4 + bytearray_to_bigint(substr(input, 4 + 24 + 1, 8)) + 32*1 + bytearray_to_bigint(substr(input, 4 + bytearray_to_bigint(substr(input, 4 + 24 + 1, 8)) + 32*1 + 32*(x - 1) + 24 + 1, 8)) + bytearray_to_bigint(substr(input, 4 + bytearray_to_bigint(substr(input, 4 + 24 + 1, 8)) + 32*1 + bytearray_to_bigint(substr(input, 4 + bytearray_to_bigint(substr(input, 4 + 24 + 1, 8)) + 32*1 + 32*(x - 1) + 24 + 1, 8)) + 32*12 + 24 + 1, 8)) + 32*10 + 1, 32))",
                "fee_min_amount":   "if(bytearray_to_bigint(substr(input, 4 + bytearray_to_bigint(substr(input, 4 + 24 + 1, 8)) + 32*1 + bytearray_to_bigint(substr(input, 4 + bytearray_to_bigint(substr(input, 4 + 24 + 1, 8)) + 32*1 + 32*(x - 1) + 24 + 1, 8)) + bytearray_to_bigint(substr(input, 4 + bytearray_to_bigint(substr(input, 4 + 24 + 1, 8)) + 32*1 + bytearray_to_bigint(substr(input, 4 + bytearray_to_bigint(substr(input, 4 + 24 + 1, 8)) + 32*1 + 32*(x - 1) + 24 + 1, 8)) + 32*12 + 24 + 1, 8)) + 32*4 + 24 + 1, 8)) > 1, substr(input, 4 + bytearray_to_bigint(substr(input, 4 + 24 + 1, 8)) + 32*1 + bytearray_to_bigint(substr(input, 4 + bytearray_to_bigint(substr(input, 4 + 24 + 1, 8)) + 32*1 + 32*(x - 1) + 24 + 1, 8)) + bytearray_to_bigint(substr(input, 4 + bytearray_to_bigint(substr(input, 4 + 24 + 1, 8)) + 32*1 + bytearray_to_bigint(substr(input, 4 + bytearray_to_bigint(substr(input, 4 + 24 + 1, 8)) + 32*1 + 32*(x - 1) + 24 + 1, 8)) + 32*12 + 24 + 1, 8)) + 32*11 + 1, 32))",
                "fee_receiver":     "if(bytearray_to_bigint(substr(input, 4 + bytearray_to_bigint(substr(input, 4 + 24 + 1, 8)) + 32*1 + bytearray_to_bigint(substr(input, 4 + bytearray_to_bigint(substr(input, 4 + 24 + 1, 8)) + 32*1 + 32*(x - 1) + 24 + 1, 8)) + bytearray_to_bigint(substr(input, 4 + bytearray_to_bigint(substr(input, 4 + 24 + 1, 8)) + 32*1 + bytearray_to_bigint(substr(input, 4 + bytearray_to_bigint(substr(input, 4 + 24 + 1, 8)) + 32*1 + 32*(x - 1) + 24 + 1, 8)) + 32*12 + 24 + 1, 8)) + 32*4 + 24 + 1, 8)) > 1, substr(input, 4 + bytearray_to_bigint(substr(input, 4 + 24 + 1, 8)) + 32*1 + bytearray_to_bigint(substr(input, 4 + bytearray_to_bigint(substr(input, 4 + 24 + 1, 8)) + 32*1 + 32*(x - 1) + 24 + 1, 8)) + bytearray_to_bigint(substr(input, 4 + bytearray_to_bigint(substr(input, 4 + 24 + 1, 8)) + 32*1 + bytearray_to_bigint(substr(input, 4 + bytearray_to_bigint(substr(input, 4 + 24 + 1, 8)) + 32*1 + 32*(x - 1) + 24 + 1, 8)) + 32*12 + 24 + 1, 8)) + 32*12 + 12 + 1, 20))",
            },
        },
        "Bebop": {
            "0x1a499026": {
                "name": "settleSingle",
                "event": "0xadd7095becdaa725f0f33243630938c861b0bba83dfd217d4055701aa768ec2e",
                "maker":            "substr(input    , 4 + 32*2 + 12 + 1        , 32)",
                "taker":            "substr(input    , 4 + 32*1 + 12 + 1        , 32)",
                "receiver":         "substr(input    , 4 + 32*8 + 12 + 1        , 32)",
                "maker_asset":      "substr(input    , 4 + 32*5 + 12 + 1        , 32)",
                "taker_asset":      "substr(input    , 4 + 32*4 + 12 + 1        , 32)",
                "maker_max_amount": "substr(input    , 4 + 32*7 + 1             , 32)",
                "taker_max_amount": "substr(input    , 4 + 32*6 + 1             , 32)",
                "making_amount":    "substr(input    , 4 + 32*14 + 1            , 32)",
                "taking_amount":    "if(bytearray_to_bigint(substr(input, 4 + 32*13 + 24 + 1, 8)) = 1, substr(input, 4 + 32*6 + 1, 32), substr(input, 4 + 32*12 + 1, 32))",
                "deadline":         "substr(input    , 4 + 32*1 + 24 + 1        , 8)",
                "nonce":            "substr(input    , 4 + 32*3 + 1             , 32)",
                "order_hash":       "substr(input    , 4 + 32*10 + 1            , 16)",
            },
            "0x38ec0211": {
                "name": "settleSingleAndSignPermit2",
                "event": "0xadd7095becdaa725f0f33243630938c861b0bba83dfd217d4055701aa768ec2e",
                "maker":            "substr(input    , 4 + 32*2 + 12 + 1        , 32)",
                "taker":            "substr(input    , 4 + 32*1 + 12 + 1        , 32)",
                "receiver":         "substr(input    , 4 + 32*8 + 12 + 1        , 32)",
                "maker_asset":      "substr(input    , 4 + 32*5 + 12 + 1        , 32)",
                "taker_asset":      "substr(input    , 4 + 32*4 + 12 + 1        , 32)",
                "maker_max_amount": "substr(input    , 4 + 32*7 + 1             , 32)",
                "taker_max_amount": "substr(input    , 4 + 32*6 + 1             , 32)",
                "making_amount":    "substr(input    , 4 + 32*14 + 1            , 32)",
                "taking_amount":    "if(bytearray_to_bigint(substr(input, 4 + 32*13 + 24 + 1, 8)) = 1, substr(input, 4 + 32*6 + 1, 32), substr(input, 4 + 32*12 + 1, 32))",
                "deadline":         "substr(input    , 4 + 32*1 + 24 + 1        , 8)",
                "nonce":            "substr(input    , 4 + 32*3 + 1             , 32)",
                "order_hash":       "substr(input    , 4 + 32*10 + 1            , 16)",
            },
            "0xefe34fe6": {
                "name": "settleMulti",
                "event": "0xadd7095becdaa725f0f33243630938c861b0bba83dfd217d4055701aa768ec2e",
                "_order_beginning": "4 + bytearray_to_bigint(substr(input, 4 + 24 + 1, 8))",
                "number":           "coalesce(try(greatest(bytearray_to_bigint(substr(input, 4 + bytearray_to_bigint(substr(input, 4 + 24 + 1, 8)) + 32*4 + 24 + 1, 8)), bytearray_to_bigint(substr(input, 4 + bytearray_to_bigint(substr(input, 4 + 24 + 1, 8)) + 32*5 + 24 + 1, 8)))), 1)",
                "maker":            "substr(input, 4 + bytearray_to_bigint(substr(input, 4 + 24 + 1, 8)) + 32*2 + 12 + 1, 32)",
                "taker":            "substr(input, 4 + bytearray_to_bigint(substr(input, 4 + 24 + 1, 8)) + 32*1 + 12 + 1, 32)",
                "receiver":         "substr(input, 4 + bytearray_to_bigint(substr(input, 4 + 24 + 1, 8)) + 32*8 + 12 + 1, 32)",
                "maker_asset":      "substr(input, 4 + bytearray_to_bigint(substr(input, 4 + 24 + 1, 8)) + bytearray_to_bigint(substr(input, 4 + bytearray_to_bigint(substr(input, 4 + 24 + 1, 8)) + 32*5 + 24 + 1, 8)) + 32 * least(x, bytearray_to_bigint(substr(input, 4 + bytearray_to_bigint(substr(input, 4 + 24 + 1, 8)) + 32*5 + 24 + 1, 8))) + 12 + 1, 20)",
                "taker_asset":      "substr(input, 4 + bytearray_to_bigint(substr(input, 4 + 24 + 1, 8)) + bytearray_to_bigint(substr(input, 4 + bytearray_to_bigint(substr(input, 4 + 24 + 1, 8)) + 32*4 + 24 + 1, 8)) + 32 * least(x, bytearray_to_bigint(substr(input, 4 + bytearray_to_bigint(substr(input, 4 + 24 + 1, 8)) + 32*4 + 24 + 1, 8))) + 12 + 1, 20)",
                "making_amount":    "substr(input, 4 + bytearray_to_bigint(substr(input, 4 + 24 + 1, 8)) + bytearray_to_bigint(substr(input, 4 + bytearray_to_bigint(substr(input, 4 + 24 + 1, 8)) + 32*7 + 24 + 1, 8)) + 32 * least(x, bytearray_to_bigint(substr(input, 4 + bytearray_to_bigint(substr(input, 4 + 24 + 1, 8)) + 32*7 + 24 + 1, 8))) + 1, 32)",
                "taking_amount":    "substr(input, 4 + bytearray_to_bigint(substr(input, 4 + 24 + 1, 8)) + bytearray_to_bigint(substr(input, 4 + bytearray_to_bigint(substr(input, 4 + 24 + 1, 8)) + 32*6 + 24 + 1, 8)) + 32 * least(x, bytearray_to_bigint(substr(input, 4 + bytearray_to_bigint(substr(input, 4 + 24 + 1, 8)) + 32*6 + 24 + 1, 8))) + 1, 32)",
                "deadline":         "substr(input, 4 + bytearray_to_bigint(substr(input, 4 + 24 + 1, 8)) + 32*1 + 24 + 1, 8)",
                "nonce":            "substr(input, 4 + bytearray_to_bigint(substr(input, 4 + 24 + 1, 8)) + 32*3 + 1, 32)",
                "order_hash":       "substr(input, 4 + bytearray_to_bigint(substr(input, 4 + 24 + 1, 8)) + 32*10 + 1, 16)",
            }
        },
    }
%}

{% set wrapping = 'array[0x0000000000000000000000000000000000000000, 0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee, wrapped_native_token_address]' %}

with

logs as (
{% for event, event_data in events.items() %}
    select
        block_number
        , tx_hash
        , index
        , contract_address
        , topic0
        , '{{event_data["name"]}}' as event
        , {{ event_data.get("maker", "null") }} as log_maker
        , {{ event_data.get("taker", "null") }} as log_taker
        , {{ event_data.get("receiver", "null") }} as log_receiver
        , {{ event_data.get("maker_asset", "null") }} as log_maker_asset
        , {{ event_data.get("taker_asset", "null") }} as log_taker_asset
        , try(bytearray_to_uint256({{ event_data.get("maker_max_amount", "null") }})) as log_maker_max_amount
        , try(bytearray_to_uint256({{ event_data.get("taker_max_amount", "null") }})) as log_taker_max_amount
        , try(bytearray_to_uint256({{ event_data.get("maker_min_amount", "null") }})) as log_maker_min_amount
        , try(bytearray_to_uint256({{ event_data.get("taker_min_amount", "null") }})) as log_taker_min_amount
        , try(bytearray_to_uint256({{ event_data.get("making_amount", "null") }})) as log_making_amount
        , try(bytearray_to_uint256({{ event_data.get("taking_amount", "null") }})) as log_taking_amount
        , try(bytearray_to_uint256({{ event_data.get("log_start", "null") }})) as log_start
        , try(bytearray_to_uint256({{ event_data.get("log_end", "null") }})) as log_end
        , try(bytearray_to_uint256({{ event_data.get("log_deadline", "null") }})) as log_deadline
        , try(bytearray_to_uint256({{ event_data.get("maker_fee_amount", "null") }})) as log_maker_fee_amount
        , try(bytearray_to_uint256({{ event_data.get("taker_fee_amount", "null") }})) as log_taker_fee_amount
        , {{ event_data.get("fee_asset", "null") }} as log_fee_asset
        , try(bytearray_to_uint256({{ event_data.get("fee_max_amount", "null") }})) as log_fee_max_amount
        , try(bytearray_to_uint256({{ event_data.get("fee_min_amount", "null") }})) as log_fee_min_amount
        , try(bytearray_to_uint256({{ event_data.get("fee_amount", "null") }})) as log_fee_amount
        , {{ event_data.get("fee_receiver", "null") }} as log_fee_receiver
        , {{ event_data.get("nonce", "null") }} as log_nonce
        , {{ event_data.get("order_hash", "null") }} as log_order_hash
        , topic1
        , topic2
        , topic3
        , data
        , row_number() over(partition by block_number, tx_hash order by index) as log_counter
    from {{ source(blockchain, 'logs') }}
    where
        topic0 = {{ event }}
        {% if is_incremental() %}
            and {{ incremental_predicate('block_time') }}
        {% else %}
            and block_time >= timestamp '{{date_from}}'
        {% endif %}
    {% if not loop.last %}union all{% endif %}
{% endfor %}
)

, calls as (
    select *, row_number() over(partition by block_number, tx_hash order by call_trace_address, call_trade) as call_trade_counter
    from (
        select
            blockchain
            , project
            , tag
            , flags
            , block_number
            , block_time
            , tx_hash
            , tx_success
            , call_from
            , call_to
            , call_trace_address
            , call_success
            , call_selector
            , call_gas_used
            , call_type
            , call_error
            , method
            , topic0
            , trade['trade'] as call_trade
            , trade['maker'] as call_maker
            , trade['taker'] as call_taker
            , trade['receiver'] as call_receiver
            , trade['maker_asset'] as call_maker_asset
            , trade['taker_asset'] as call_taker_asset
            , try(bytearray_to_uint256(trade['maker_max_amount'])) as call_maker_max_amount
            , try(bytearray_to_uint256(trade['taker_max_amount'])) as call_taker_max_amount
            , try(bytearray_to_uint256(trade['maker_min_amount'])) as call_maker_min_amount
            , try(bytearray_to_uint256(trade['taker_min_amount'])) as call_taker_min_amount
            , try(bytearray_to_uint256(trade['making_amount'])) as call_making_amount
            , try(bytearray_to_uint256(trade['taking_amount'])) as call_taking_amount
            , try(bytearray_to_uint256(substr(trade['start'], 24 + 1, 8))) as call_start
            , try(bytearray_to_uint256(substr(trade['end'], 24 + 1, 8))) as call_end
            , try(bytearray_to_uint256(substr(trade['deadline'], 24 + 1, 8))) as call_deadline
            , try(bytearray_to_uint256(trade['maker_fee_amount'])) as call_maker_fee_amount
            , try(bytearray_to_uint256(trade['taker_fee_amount'])) as call_taker_fee_amount
            , trade['fee_asset'] as call_fee_asset
            , try(bytearray_to_uint256(trade['fee_max_amount'])) as call_fee_max_amount
            , try(bytearray_to_uint256(trade['fee_min_amount'])) as call_fee_min_amount
            , try(bytearray_to_uint256(trade['fee_amount'])) as call_fee_amount
            , trade['fee_receiver'] as call_fee_receiver
            , trade['nonce'] as call_nonce
            , trade['order_hash'] as call_order_hash
            , input
            , output
        from (
        {% for project, selectors in cfg.items() %}
        {% for selector, method_data in selectors.items() %}
            select
                blockchain
                , project
                , tag
                , flags
                , block_number
                , block_time
                , tx_hash
                , tx_success
                , "from" as call_from
                , "to" as call_to
                , trace_address as call_trace_address
                , success as call_success
                , substr(input, 1, 4) as call_selector
                , gas_used as call_gas_used
                , call_type
                , error as call_error
                , '{{ method_data["name"] }}' as method
                , {{ method_data["event"] }} as topic0
                , transform(sequence(1, {{ method_data.get("number", "1") }}), x -> map_from_entries(array[
                      ('trade',             try(to_big_endian_64(x)))
                    , ('maker',             {{ method_data.get("maker", "null") }})
                    , ('taker',             {{ method_data.get("taker", "null") }})
                    , ('receiver',          {{ method_data.get("receiver", "null") }})
                    , ('maker_asset',       {{ method_data.get("maker_asset", "null") }})
                    , ('taker_asset',       {{ method_data.get("taker_asset", "null") }})
                    , ('maker_max_amount',  {{ method_data.get("maker_max_amount", "null") }})
                    , ('taker_max_amount',  {{ method_data.get("taker_max_amount", "null") }})
                    , ('maker_min_amount',  {{ method_data.get("maker_min_amount", "null") }})
                    , ('taker_min_amount',  {{ method_data.get("taker_min_amount", "null") }})
                    , ('making_amount',     {{ method_data.get("making_amount", "null") }})
                    , ('taking_amount',     {{ method_data.get("taking_amount", "null") }})
                    , ('start',             {{ method_data.get("start", "null") }})
                    , ('end',               {{ method_data.get("end", "null") }})
                    , ('deadline',          {{ method_data.get("deadline", "null") }})
                    , ('fee_asset',         {{ method_data.get("fee_asset", "null") }})
                    , ('fee_max_amount',    {{ method_data.get("fee_max_amount", "null") }})
                    , ('fee_min_amount',    {{ method_data.get("fee_min_amount", "null") }})
                    , ('fee_amount',        {{ method_data.get("fee_amount", "null") }})
                    , ('fee_receiver',      {{ method_data.get("fee_receiver", "null") }})
                    , ('nonce',             {{ method_data.get("nonce", "null") }})
                    , ('order_hash',        {{ method_data.get("order_hash", "null") }})
                ])) as trades
                , input
                , output
            from {{ source(blockchain, 'traces') }}
            join (
                select *, address as "to"
                from {{ ref('oneinch_' + blockchain + '_mapped_contracts') }}
                where
                    blockchain = '{{ blockchain }}'
                    and '{{ project }}' in (project, tag)
            ) using("to")
            where
                {% if is_incremental() %}{{ incremental_predicate('block_time') }}
                {% else %}block_time > greatest(first_created_at, timestamp '{{date_from}}'){% endif %}
                and substr(input, 1, 4) = {{ selector }}

            {% if not loop.last %}union all{% endif %}
        {% endfor %}
            {% if not loop.last %}union all{% endif %}
        {% endfor %}
        ), unnest(trades) as trades(trade)
    )
)

, joined as (
    select
        *
        , coalesce(log_maker, call_maker, call_to) as maker
        , coalesce(log_taker, call_taker) as taker
        , coalesce(log_receiver, call_receiver) as receiver
        , coalesce(log_maker_asset, call_maker_asset) as maker_asset
        , coalesce(log_taker_asset, call_taker_asset) as taker_asset
        , coalesce(log_maker_max_amount, call_maker_max_amount) as maker_max_amount
        , coalesce(log_taker_max_amount, call_taker_max_amount) as taker_max_amount
        , coalesce(log_maker_min_amount, call_maker_min_amount) as maker_min_amount
        , coalesce(log_taker_min_amount, call_taker_min_amount) as taker_min_amount
        , coalesce(log_making_amount, call_making_amount) as making_amount
        , coalesce(log_taking_amount, call_taking_amount) as taking_amount
        , coalesce(log_start, call_start) as order_start
        , coalesce(log_end, call_end) as order_end
        , coalesce(log_deadline, call_deadline) as order_deadline
        , coalesce(log_maker_fee_amount, call_maker_fee_amount) as maker_fee_amount
        , coalesce(log_taker_fee_amount, call_taker_fee_amount) as taker_fee_amount
        , coalesce(log_fee_asset, call_fee_asset) as fee_asset
        , coalesce(log_fee_max_amount, call_fee_max_amount) as fee_max_amount
        , coalesce(log_fee_min_amount, call_fee_min_amount) as fee_min_amount
        , coalesce(log_fee_amount, call_fee_amount) as fee_amount
        , coalesce(log_fee_receiver, call_fee_receiver) as fee_receiver
        , coalesce(log_nonce, call_nonce) as order_nonce
        , coalesce(log_order_hash, call_order_hash, concat(tx_hash, to_big_endian_32(cast(call_trade_counter as int)))) as order_hash
        , count(*) over(partition by blockchain, block_number, tx_hash, call_trace_address, call_trade) as trades
    from calls
    full join logs using(block_number, tx_hash, topic0)
    join ({{ oneinch_blockchain_macro(blockchain) }}) using(blockchain)
    where
            (call_maker = log_maker or call_maker is null or log_maker is null)
        and (call_taker = log_taker or call_taker is null or log_taker is null)
        and (call_receiver = log_receiver or call_receiver is null or log_receiver is null)
        and (call_maker_asset = log_maker_asset or call_maker_asset is null or log_maker_asset is null or cardinality(array_intersect({{wrapping}}, array[call_maker_asset, log_maker_asset])) = 2)
        and (call_taker_asset = log_taker_asset or call_taker_asset is null or log_taker_asset is null or cardinality(array_intersect({{wrapping}}, array[call_taker_asset, log_taker_asset])) = 2)
        and (call_maker_max_amount = log_maker_max_amount or call_maker_max_amount is null or log_maker_max_amount is null)
        and (call_taker_max_amount = log_taker_max_amount or call_taker_max_amount is null or log_taker_max_amount is null)
        and (call_maker_min_amount = log_maker_min_amount or call_maker_min_amount is null or log_maker_min_amount is null)
        and (call_taker_min_amount = log_taker_min_amount or call_taker_min_amount is null or log_taker_min_amount is null)
        and (call_making_amount = log_making_amount or call_making_amount is null or log_making_amount is null)
        and (call_taking_amount = log_taking_amount or call_taking_amount is null or log_taking_amount is null)
        and (call_start = log_start or call_start is null or log_start is null)
        and (call_end = log_end or call_end is null or log_end is null)
        and (call_deadline = log_deadline or call_deadline is null or log_deadline is null)
        and (call_nonce = log_nonce or call_nonce is null or log_nonce is null)
        and (call_order_hash = log_order_hash or call_order_hash is null or log_order_hash is null)
)

-- output --

select
    blockchain
    , project
    , tag
    , map_concat(flags, map_from_entries(array[
        ('auction', coalesce(order_start, uint256 '0') > uint256 '0' or project in ('CoWSwap', 'Bebop'))
    ])) as flags
    , block_number
    , block_time
    , tx_hash
    , tx_success
    , call_from
    , call_to
    , call_trace_address
    , call_success
    , call_selector
    , call_gas_used
    , call_type
    , call_error
    , call_trade
    , method
    , maker
    , taker
    , receiver
    , maker_asset
    , taker_asset
    , maker_max_amount
    , taker_max_amount
    , maker_min_amount
    , taker_min_amount
    , coalesce(making_amount, if(order_start = uint256 '0' or order_start = order_end, maker_max_amount, maker_max_amount - cast(to_unixtime(block_time) - order_start as double) / (order_end - order_start) * (cast(maker_max_amount as double) - cast(maker_min_amount as double))), maker_max_amount, maker_min_amount) as making_amount
    , coalesce(taking_amount, if(order_start = uint256 '0' or order_start = order_end, taker_max_amount, taker_max_amount - cast(to_unixtime(block_time) - order_start as double) / (order_end - order_start) * (cast(taker_max_amount as double) - cast(taker_min_amount as double))), taker_max_amount, taker_min_amount) as taking_amount
    , order_start
    , order_end
    , order_deadline
    , maker_fee_amount
    , taker_fee_amount
    , fee_asset
    , fee_max_amount
    , fee_min_amount
    , fee_amount
    , fee_receiver
    , order_nonce
    , order_hash
    , array[input] as call_input
    , array[output] as call_output
    , index as event_index
    , contract_address as event_contract_address
    , topic1 as event_topic1
    , topic2 as event_topic2
    , topic3 as event_topic3
    , array[data] as event_data
    , to_unixtime(block_time) as block_unixtime
    , date(date_trunc('month', block_time)) as block_month
from joined
where
    trades = 1
    or call_trade_counter = log_counter
    or log_counter is null

{% endmacro %}