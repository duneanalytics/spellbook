{% macro call_entity(source) %}if(params['{{source}}'].source = 'input', input, output){% endmacro %}
{% macro log_entity(source) %}if(params['{{source}}'].source = 'topic1', topic1, if(params['{{source}}'].source = 'topic2', topic2, if(params['{{source}}'].source = 'topic3', topic3, data))){% endmacro %}
{% macro binary(entity, source) %}substr({% if entity == 'call' %}{{ call_entity(source) }}{% else %}{{ log_entity(source) }}{% endif %}, params['{{source}}'].start + 1, params['{{source}}'].offset){% endmacro %}
{% macro amount(entity, source) %}if(params['{{source}}'].sign = 'u', bytearray_to_uint256({{ binary(entity, source) }}), -1 * bytearray_to_int256({{ binary(entity, source) }})){% endmacro %}

{% macro
    oneinch_project_orders_macro(
        blockchain
        , date_from = '2019-01-01'
    ) 
%}

-- EVENTS CONFIG
{%
    set events = {
        "0x0bcc4c97732e47d9946f229edb95f5b6323f601300e4690de719993f3c371129": {
            "name": "Fill",
            "maker":            ("topic1"   , 12            , 20, 'b'),
            "taker":            ("data"     , 12            , 20, 'b'),
            "maker_asset":      ("data"     , 32*9  + 4 + 12, 20, 'b'),
            "taker_asset":      ("data"     , 32*12 + 4 + 12, 20, 'b'),
            "making_amount":    ("data"     , 32*2          , 32, 'u'),
            "taking_amount":    ("data"     , 32*3          , 32, 'u'),
            "order_hash":       ("topic3"   , 0             , 32, 'b'),
        },
        "0xab614d2b738543c0ea21f56347cf696a3a0c42a7cbec3212a5ca22a4dcff2124": {
            "name": "LimitOrderFilled",
            "maker":            ("data"     , 32*1 + 12     , 20, 'b'),
            "taker":            ("data"     , 32*2 + 12     , 20, 'b'),
            "maker_asset":      ("data"     , 32*4 + 12     , 20, 'b'),
            "taker_asset":      ("data"     , 32*5 + 12     , 20, 'b'),
            "making_amount":    ("data"     , 32*7          , 32, 'u'),
            "taking_amount":    ("data"     , 32*6          , 32, 'u'),
            "taker_fee_amount": ("data"     , 32*8          , 32, 'u'),
            "order_hash":       ("data"     , 0             , 32, 'b'),
        },
        "0xac75f773e3a92f1a02b12134d65e1f47f8a14eabe4eaf1e24624918e6a8b269f": {
            "name": "OtcOrderFilled",
            "maker":            ("data"     , 32*1 + 12     , 20, 'b'),
            "taker":            ("data"     , 32*2 + 12     , 20, 'b'),
            "maker_asset":      ("data"     , 32*3 + 12     , 20, 'b'),
            "taker_asset":      ("data"     , 32*4 + 12     , 20, 'b'),
            "making_amount":    ("data"     , 32*5          , 32, 'u'),
            "taking_amount":    ("data"     , 32*6          , 32, 'u'),
            "order_hash":       ("data"     , 0             , 32, 'b'),
        },
        "0x829fa99d94dc4636925b38632e625736a614c154d55006b7ab6bea979c210c32": {
            "name": "RfqOrderFilled",
            "maker":            ("data"     , 32*1 + 12     , 20, 'b'),
            "taker":            ("data"     , 32*2 + 12     , 20, 'b'),
            "maker_asset":      ("data"     , 32*3 + 12     , 20, 'b'),
            "taker_asset":      ("data"     , 32*4 + 12     , 20, 'b'),
            "making_amount":    ("data"     , 32*6          , 32, 'u'),
            "taking_amount":    ("data"     , 32*5          , 32, 'u'),
            "order_hash":       ("data"     , 0             , 32, 'b'),
        },
        "0xb709ddcc6550418e9b89df1f4938071eeaa3f6376309904c77e15d46b16066f5": {
            "name": "Trade",
            "maker":            ("data"     , 32*0 + 12     , 20, 'b'),
            "taker":            ("data"     , 32*1 + 12     , 20, 'b'),
            "maker_asset":      ("data"     , 32*4 + 12     , 20, 'b'),
            "taker_asset":      ("data"     , 32*3 + 12     , 20, 'b'),
            "making_amount":    ("data"     , 32*6          , 32, 'u'),
            "taking_amount":    ("data"     , 32*5          , 32, 'u'),
        },
        "0x8cf3dec1929508e5677d7db003124e74802bfba7250a572205a9986d86ca9f1e": {
            "name": "Trade",
            "taker":            ("data"     , 32*0 + 12     , 20, 'b'),
            "maker_asset":      ("data"     , 32*3 + 12     , 20, 'b'),
            "taker_asset":      ("data"     , 32*2 + 12     , 20, 'b'),
            "making_amount":    ("data"     , 32*5          , 32, 'u'),
            "taking_amount":    ("data"     , 32*4          , 32, 'u'),
        },
        "0x34f57786fb01682fb4eec88d340387ef01a168fe345ea5b76f709d4e560c10eb": {
            "name": "Trade",
            "taker":            ("data"     , 32*0 + 12     , 20, 'b'),
            "maker_asset":      ("data"     , 32*4 + 12     , 20, 'b'),
            "taker_asset":      ("data"     , 32*3 + 12     , 20, 'b'),
            "making_amount":    ("data"     , 32*6          , 32, 'u'),
            "taking_amount":    ("data"     , 32*5          , 32, 'u'),
            "order_hash":       ("data"     , 32*2          , 32, 'b'),
        },
        "0xe3a54b69726c85299f4e794bac96150af56af801be76cafd11947a1103b6308a": {
            "name": "Swap",
            "taker":            ("topic1"   , 12            , 20, 'b'),
            "maker_asset":      ("data"     , 32*1 + 12     , 20, 'b'),
            "taker_asset":      ("data"     , 32*0 + 12     , 20, 'b'),
            "making_amount":    ("data"     , 32*3          , 32, 's'),
            "taking_amount":    ("data"     , 32*2          , 32, 'u'),
            "order_hash":       ("data"     , 32*5          , 16, 'b'),
        },
        "0x4be05c8d54f5e056ab2cfa033e9f582057001268c3e28561bb999d35d2c8f2c8": {
            "name": "Swapped",
            "taker":            ("topic3"   , 12            , 20, 'b'),
            "maker_asset":      ("topic2"   , 12            , 20, 'b'),
            "taker_asset":      ("topic1"   , 12            , 20, 'b'),
            "making_amount":    ("data"     , 32*1          , 32, 'u'),
            "taking_amount":    ("data"     , 32*0          , 32, 'u'),
        },
        "0x2170c741c41531aec20e7c107c24eecfdd15e69c9bb0a8dd37b1840b9e0b207b": {
            "name": "Swap",
            "maker_asset":      ("topic3"   , 12            , 20, 'b'),
            "taker_asset":      ("topic2"   , 12            , 20, 'b'),
            "making_amount":    ("data"     , 32*1          , 32, 'u'),
            "taking_amount":    ("data"     , 32*0          , 32, 'u'),
        },
    }
%}

-- METHODS CONFIG
{%
    set cfg = {
        "ZeroEx": {
            "0xb4be83d5": {
                "name": "fillOrder",
                "event": "0x0bcc4c97732e47d9946f229edb95f5b6323f601300e4690de719993f3c371129",
                "maker":            ("input"    , 4 + 32*3 + 12     , 20, 'b'),
                "maker_asset":      ("input"    , 4 + 32*16 + 4 + 12, 20, 'b'),
                "taker_asset":      ("input"    , 4 + 32*19 + 4 + 12, 20, 'b'),
                "maker_max_amount": ("input"    , 4 + 32*7          , 32, 'u'),
                "taker_max_amount": ("input"    , 4 + 32*1          , 32, 'u'),
                "making_amount":    ("output"   , 32*0              , 32, 'u'),
                "taking_amount":    ("output"   , 32*1              , 32, 'u'),
            },
            "0xf6274f66": {
                "name": "fillLimitOrder",
                "event": "0xab614d2b738543c0ea21f56347cf696a3a0c42a7cbec3212a5ca22a4dcff2124",
                "maker":            ("input"    , 4 + 32*5 + 12     , 20, 'b'),
                "maker_asset":      ("input"    , 4 + 32*0 + 12     , 20, 'b'),
                "taker_asset":      ("input"    , 4 + 32*1 + 12     , 20, 'b'),
                "maker_max_amount": ("input"    , 4 + 32*2          , 32, 'u'),
                "taker_max_amount": ("input"    , 4 + 32*3          , 32, 'u'),
                "making_amount":    ("output"   , 32*1              , 32, 'u'),
                "taking_amount":    ("output"   , 32*0              , 32, 'u'),
            },
            "0x414e4ccf": {
                "name": "_fillLimitOrder",
                "event": "0xab614d2b738543c0ea21f56347cf696a3a0c42a7cbec3212a5ca22a4dcff2124",
                "maker":            ("input"    , 4 + 32*5 + 12     , 20, 'b'),
                "maker_asset":      ("input"    , 4 + 32*0 + 12     , 20, 'b'),
                "taker_asset":      ("input"    , 4 + 32*1 + 12     , 20, 'b'),
                "maker_max_amount": ("input"    , 4 + 32*2          , 32, 'u'),
                "taker_max_amount": ("input"    , 4 + 32*3          , 32, 'u'),
                "making_amount":    ("output"   , 32*1              , 32, 'u'),
                "taking_amount":    ("output"   , 32*0              , 32, 'u'),
            },
            "0x9240529c": {
                "name": "fillOrKillLimitOrder",
                "event": "0xab614d2b738543c0ea21f56347cf696a3a0c42a7cbec3212a5ca22a4dcff2124",
                "maker":            ("input"    , 4 + 32*5 + 12     , 20, 'b'),
                "maker_asset":      ("input"    , 4 + 32*0 + 12     , 20, 'b'),
                "taker_asset":      ("input"    , 4 + 32*1 + 12     , 20, 'b'),
                "maker_max_amount": ("input"    , 4 + 32*2          , 32, 'u'),
                "taker_max_amount": ("input"    , 4 + 32*3          , 32, 'u'),
                "making_amount":    ("output"   , 32*0              , 32, 'u'),
            },
            "0xdac748d4": {
                "name": "fillOtcOrder",
                "event": "0xac75f773e3a92f1a02b12134d65e1f47f8a14eabe4eaf1e24624918e6a8b269f",
                "maker":            ("input"    , 4 + 32*4 + 12     , 20, 'b'),
                "maker_asset":      ("input"    , 4 + 32*0 + 12     , 20, 'b'),
                "taker_asset":      ("input"    , 4 + 32*1 + 12     , 20, 'b'),
                "maker_max_amount": ("input"    , 4 + 32*2          , 32, 'u'),
                "taker_max_amount": ("input"    , 4 + 32*3          , 32, 'u'),
                "making_amount":    ("output"   , 32*1              , 32, 'u'),
                "taking_amount":    ("output"   , 32*0              , 32, 'u'),
            },
            "0xe4ba8439": {
                "name": "_fillOtcOrder",
                "event": "0xac75f773e3a92f1a02b12134d65e1f47f8a14eabe4eaf1e24624918e6a8b269f",
                "maker":            ("input"    , 4 + 32*4 + 12     , 20, 'b'),
                "maker_asset":      ("input"    , 4 + 32*0 + 12     , 20, 'b'),
                "taker_asset":      ("input"    , 4 + 32*1 + 12     , 20, 'b'),
                "maker_max_amount": ("input"    , 4 + 32*2          , 32, 'u'),
                "taker_max_amount": ("input"    , 4 + 32*3          , 32, 'u'),
                "making_amount":    ("output"   , 32*1              , 32, 'u'),
                "taking_amount":    ("output"   , 32*0              , 32, 'u'),
            },
            "0xa578efaf": {
                "name": "fillOtcOrderForEth",
                "event": "0xac75f773e3a92f1a02b12134d65e1f47f8a14eabe4eaf1e24624918e6a8b269f",
                "maker":            ("input"    , 4 + 32*4 + 12     , 20, 'b'),
                "maker_asset":      ("input"    , 4 + 32*0 + 12     , 20, 'b'),
                "taker_asset":      ("input"    , 4 + 32*1 + 12     , 20, 'b'),
                "maker_max_amount": ("input"    , 4 + 32*2          , 32, 'u'),
                "taker_max_amount": ("input"    , 4 + 32*3          , 32, 'u'),
                "making_amount":    ("output"   , 32*1              , 32, 'u'),
                "taking_amount":    ("output"   , 32*0              , 32, 'u'),
            },
            "0x706394d5": {
                "name": "fillOtcOrderWithEth",
                "event": "0xac75f773e3a92f1a02b12134d65e1f47f8a14eabe4eaf1e24624918e6a8b269f",
                "maker":            ("input"    , 4 + 32*4 + 12     , 20, 'b'),
                "maker_asset":      ("input"    , 4 + 32*0 + 12     , 20, 'b'),
                "taker_asset":      ("input"    , 4 + 32*1 + 12     , 20, 'b'),
                "maker_max_amount": ("input"    , 4 + 32*2          , 32, 'u'),
                "taker_max_amount": ("input"    , 4 + 32*3          , 32, 'u'),
                "making_amount":    ("output"   , 32*1              , 32, 'u'),
                "taking_amount":    ("output"   , 32*0              , 32, 'u'),
            },
            "0x724d3953": {
                "name": "fillTakerSignedOtcOrderForEth",
                "event": "0xac75f773e3a92f1a02b12134d65e1f47f8a14eabe4eaf1e24624918e6a8b269f",
                "maker":            ("input"    , 4 + 32*4 + 12     , 20, 'b'),
                "maker_asset":      ("input"    , 4 + 32*0 + 12     , 20, 'b'),
                "taker_asset":      ("input"    , 4 + 32*1 + 12     , 20, 'b'),
                "maker_max_amount": ("input"    , 4 + 32*2          , 32, 'u'),
                "taker_max_amount": ("input"    , 4 + 32*3          , 32, 'u'),
            },
            "0x4f948110": {
                "name": "fillTakerSignedOtcOrder",
                "event": "0xac75f773e3a92f1a02b12134d65e1f47f8a14eabe4eaf1e24624918e6a8b269f",
                "maker":            ("input"    , 4 + 32*4 + 12     , 20, 'b'),
                "maker_asset":      ("input"    , 4 + 32*0 + 12     , 20, 'b'),
                "taker_asset":      ("input"    , 4 + 32*1 + 12     , 20, 'b'),
                "maker_max_amount": ("input"    , 4 + 32*2          , 32, 'u'),
                "taker_max_amount": ("input"    , 4 + 32*3          , 32, 'u'),
            },
            "0xaa77476c": {
                "name": "fillRfqOrder",
                "event": "0x829fa99d94dc4636925b38632e625736a614c154d55006b7ab6bea979c210c32",
                "maker":            ("input"    , 4 + 32*4 + 12     , 20, 'b'),
                "maker_asset":      ("input"    , 4 + 32*0 + 12     , 20, 'b'),
                "taker_asset":      ("input"    , 4 + 32*1 + 12     , 20, 'b'),
                "maker_max_amount": ("input"    , 4 + 32*2          , 32, 'u'),
                "taker_max_amount": ("input"    , 4 + 32*3          , 32, 'u'),
                "making_amount":    ("output"   , 32*1              , 32, 'u'),
                "taking_amount":    ("output"   , 32*0              , 32, 'u'),
            },
            "0xaa6b21cd": {
                "name": "_fillRfqOrder",
                "event": "0x829fa99d94dc4636925b38632e625736a614c154d55006b7ab6bea979c210c32",
                "maker":            ("input"    , 4 + 32*4 + 12     , 20, 'b'),
                "maker_asset":      ("input"    , 4 + 32*0 + 12     , 20, 'b'),
                "taker_asset":      ("input"    , 4 + 32*1 + 12     , 20, 'b'),
                "maker_max_amount": ("input"    , 4 + 32*2          , 32, 'u'),
                "taker_max_amount": ("input"    , 4 + 32*3          , 32, 'u'),
                "making_amount":    ("output"   , 32*1              , 32, 'u'),
                "taking_amount":    ("output"   , 32*0              , 32, 'u'),
            },
            "0xa656186b": {
                "name": "_fillRfqOrder",
                "event": "0x829fa99d94dc4636925b38632e625736a614c154d55006b7ab6bea979c210c32",
                "maker":            ("input"    , 4 + 32*4 + 12     , 20, 'b'),
                "maker_asset":      ("input"    , 4 + 32*0 + 12     , 20, 'b'),
                "taker_asset":      ("input"    , 4 + 32*1 + 12     , 20, 'b'),
                "maker_max_amount": ("input"    , 4 + 32*2          , 32, 'u'),
                "taker_max_amount": ("input"    , 4 + 32*3          , 32, 'u'),
                "making_amount":    ("output"   , 32*1              , 32, 'u'),
                "taking_amount":    ("output"   , 32*0              , 32, 'u'),
            },
            "0x438cdfc5": {
                "name": "fillOrKillRfqOrder",
                "event": "0x829fa99d94dc4636925b38632e625736a614c154d55006b7ab6bea979c210c32",
                "maker":            ("input"    , 4 + 32*4 + 12     , 20, 'b'),
                "maker_asset":      ("input"    , 4 + 32*0 + 12     , 20, 'b'),
                "taker_asset":      ("input"    , 4 + 32*1 + 12     , 20, 'b'),
                "maker_max_amount": ("input"    , 4 + 32*2          , 32, 'u'),
                "taker_max_amount": ("input"    , 4 + 32*3          , 32, 'u'),
                "making_amount":    ("output"   , 32*0              , 32, 'u'),
            },
        },
        "Hashflow": {
            "0x1e9a2e92": {
                "name": "tradeSingleHop",
                "event": "0xb709ddcc6550418e9b89df1f4938071eeaa3f6376309904c77e15d46b16066f5",
                "maker":            ("input"    , 4 + 32*2 + 12     , 20, 'b'),
                "maker_external":   ("input"    , 4 + 32*3 + 12     , 20, 'b'),
                "maker_asset":      ("input"    , 4 + 32*7 + 12     , 20, 'b'),
                "taker_asset":      ("input"    , 4 + 32*6 + 12     , 20, 'b'),
                "maker_max_amount": ("input"    , 4 + 32*10         , 32, 'u'),
                "taker_max_amount": ("input"    , 4 + 32*9          , 32, 'u'),
            },
            "0xf0210929": {
                "name": "tradeSingleHop",
                "event": "0x8cf3dec1929508e5677d7db003124e74802bfba7250a572205a9986d86ca9f1e",
                "maker":            ("input"    , 4 + 32*1 + 12     , 20, 'b'),
                "maker_external":   ("input"    , 4 + 32*2 + 12     , 20, 'b'),
                "maker_asset":      ("input"    , 4 + 32*6 + 12     , 20, 'b'),
                "taker_asset":      ("input"    , 4 + 32*5 + 12     , 20, 'b'),
                "maker_max_amount": ("input"    , 4 + 32*9          , 32, 'u'),
                "taker_max_amount": ("input"    , 4 + 32*8          , 32, 'u'),
            },
            "0xc52ac720": {
                "name": "tradeRFQT",
                "event": "0x34f57786fb01682fb4eec88d340387ef01a168fe345ea5b76f709d4e560c10eb",
                "maker":            ("input"    , 4 + 32*1 + 12     , 20, 'b'),
                "maker_external":   ("input"    , 4 + 32*2 + 12     , 20, 'b'),
                "maker_asset":      ("input"    , 4 + 32*6 + 12     , 20, 'b'),
                "taker_asset":      ("input"    , 4 + 32*5 + 12     , 20, 'b'),
                "maker_max_amount": ("input"    , 4 + 32*9          , 32, 'u'),
                "taker_max_amount": ("input"    , 4 + 32*8          , 32, 'u'),
                "order_hash":       ("input"    , 4 + 32*12         , 32, 'u'),
            },
        },
        "Native": {
            "0xd025fdfa": {
                "name": "swap",
                "event": "0xe3a54b69726c85299f4e794bac96150af56af801be76cafd11947a1103b6308a",
                "maker":            ("input"    , 4 + 32*7 + 12     , 20, 'b'),
                "maker_asset":      ("input"    , 4 + 32*10 + 12    , 20, 'b'),
                "taker_asset":      ("input"    , 4 + 32*11 + 12    , 20, 'b'),
                "making_amount":    ("output"   , 32*0              , 32, 's'),
                "taking_amount":    ("output"   , 32*1              , 32, 'u'),
            },
        },
        "Clipper": {
            "0x2b651a6c": {
                "name": "swap",
                "event": "0x4be05c8d54f5e056ab2cfa033e9f582057001268c3e28561bb999d35d2c8f2c8",
                "taker":            ("input"    , 4 + 32*5 + 12     , 20, 'b'),
                "maker_asset":      ("input"    , 4 + 32*1 + 12     , 20, 'b'),
                "taker_asset":      ("input"    , 4 + 32*0 + 12     , 20, 'b'),
                "maker_max_amount": ("input"    , 4 + 32*3          , 32, 'u'),
                "taker_max_amount": ("input"    , 4 + 32*2          , 32, 'u'),
            },
            "0x4cb6864c": {
                "name": "sellTokenForEth",
                "event": "0x4be05c8d54f5e056ab2cfa033e9f582057001268c3e28561bb999d35d2c8f2c8",
                "taker":            ("input"    , 4 + 32*4 + 12     , 20, 'b'),
                "taker_asset":      ("input"    , 4 + 32*0 + 12     , 20, 'b'),
                "maker_max_amount": ("input"    , 4 + 32*2          , 32, 'u'),
                "taker_max_amount": ("input"    , 4 + 32*1          , 32, 'u'),
            },
            "0x27a9b424": {
                "name": "sellEthForToken",
                "event": "0x4be05c8d54f5e056ab2cfa033e9f582057001268c3e28561bb999d35d2c8f2c8",
                "taker":            ("input"    , 4 + 32*4 + 12     , 20, 'b'),
                "maker_asset":      ("input"    , 4 + 32*0 + 12     , 20, 'b'),
                "taker_max_amount": ("input"    , 4 + 32*2          , 32, 'u'),
                "maker_max_amount": ("input"    , 4 + 32*1          , 32, 'u'),
            },
            "0x3b26e4eb": {
                "name": "transmitAndSwap",
                "event": "0x4be05c8d54f5e056ab2cfa033e9f582057001268c3e28561bb999d35d2c8f2c8",
                "taker":            ("input"    , 4 + 32*5 + 12     , 20, 'b'),
                "maker_asset":      ("input"    , 4 + 32*1 + 12     , 20, 'b'),
                "taker_asset":      ("input"    , 4 + 32*0 + 12     , 20, 'b'),
                "maker_max_amount": ("input"    , 4 + 32*3          , 32, 'u'),
                "taker_max_amount": ("input"    , 4 + 32*2          , 32, 'u'),
            },
        },
        "Swaap": {
            "0x52bbbe29": {
                "name": "swap",
                "event": "0x2170c741c41531aec20e7c107c24eecfdd15e69c9bb0a8dd37b1840b9e0b207b",
                "taker":            ("input"    , 4 + 32*3 + 12     , 20, 'b'),
                "maker_asset":      ("input"    , 4 + 32*10 + 12    , 20, 'b'),
                "taker_asset":      ("input"    , 4 + 32*9 + 12     , 20, 'b'),
                "taker_max_amount": ("input"    , 4 + 32*11         , 32, 'u'),
                "making_amount":    ("output"   , 32*0              , 32, 'u'),
            },
        },
    }
%}

{% set columns = 'row(source varchar, start int, offset int, sign varchar)' %}
{% set wrapping = 'array[0x0000000000000000000000000000000000000000, 0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee, wrapped_native_token_address]' %}

with

contracts as (
    select
        blockchain
        , address
        , project
        , tag
        , flags
    from {{ ref('oneinch_' + blockchain + '_mapped_contracts') }}
    where
        flags['lop']
        and project <> '1inch'
)

, events(event, name, params) as (values
    {% for event, event_data in events.items() %}
        {% if not loop.first %}, {% endif %} (
              {{event}}
            , '{{event_data["name"]}}'
            , map_from_entries(array[
                  ('maker',            cast({{ event_data.get("maker", "null") }} as {{columns}}))
                , ('taker',            cast({{ event_data.get("taker", "null") }} as {{columns}}))
                , ('receiver',         cast({{ event_data.get("receiver", "null") }} as {{columns}}))
                , ('maker_asset',      cast({{ event_data.get("maker_asset", "null") }} as {{columns}}))
                , ('taker_asset',      cast({{ event_data.get("taker_asset", "null") }} as {{columns}}))
                , ('maker_max_amount', cast({{ event_data.get("maker_max_amount", "null") }} as {{columns}}))
                , ('taker_max_amount', cast({{ event_data.get("taker_max_amount", "null") }} as {{columns}}))
                , ('making_amount',    cast({{ event_data.get("making_amount", "null") }} as {{columns}}))
                , ('taking_amount',    cast({{ event_data.get("taking_amount", "null") }} as {{columns}}))
                , ('maker_fee_amount', cast({{ event_data.get("maker_fee_amount", "null") }} as {{columns}}))
                , ('taker_fee_amount', cast({{ event_data.get("taker_fee_amount", "null") }} as {{columns}}))
                , ('order_hash',       cast({{ event_data.get("order_hash", "null") }} as {{columns}}))
            ])
        )
    {% endfor %}
)

, logs as (
    select
        block_number
        , tx_hash
        , log_contract_address
        , event
        , try({{ binary('event', 'maker') }}) as log_maker
        , try({{ binary('event', 'taker') }}) as log_taker
        , try({{ binary('event', 'receiver') }}) as log_receiver
        , try({{ binary('event', 'maker_asset') }}) as log_maker_asset
        , try({{ binary('event', 'taker_asset') }}) as log_taker_asset
        , try({{ amount('event', 'maker_max_amount') }}) as log_maker_max_amount
        , try({{ amount('event', 'taker_max_amount') }}) as log_taker_max_amount
        , try({{ amount('event', 'making_amount') }}) as log_making_amount
        , try({{ amount('event', 'taking_amount') }}) as log_taking_amount
        , try({{ amount('event', 'maker_fee_amount') }}) as log_maker_fee_amount
        , try({{ amount('event', 'taker_fee_amount') }}) as log_taker_fee_amount
        , try({{ binary('event', 'order_hash') }}) as log_order_hash
        , topic1
        , topic2
        , topic3
        , data
        , row_number() over(partition by block_number, tx_hash order by index) as log_counter
    from (
        select
            block_number
            , tx_hash
            , contract_address as log_contract_address
            , topic0 as event
            , topic1
            , topic2
            , topic3
            , data
            , index
        from {{ source(blockchain, 'logs') }}
        where
            {% if is_incremental() %}
                {{ incremental_predicate('block_time') }}
            {% else %}
                block_time >= timestamp '{{date_from}}'
            {% endif %}
    )
    join events using(event)
)

, selectors(project, selector, method, event, params) as (values
    {% for project, selectors in cfg.items() %}
            {% if not loop.first %}, {% endif %}
        {% for selector, method_data in selectors.items() %}
            {% if not loop.first %}, {% endif %}(
                  '{{ project }}'
                , {{ selector }}
                , '{{ method_data["name"] }}'
                , {{ method_data["event"] }}
                , map_from_entries(array[
                      ('maker',            cast({{ method_data.get("maker", "null") }} as {{columns}}))
                    , ('taker',            cast({{ method_data.get("taker", "null") }} as {{columns}}))
                    , ('receiver',         cast({{ method_data.get("receiver", "null") }} as {{columns}}))
                    , ('maker_asset',      cast({{ method_data.get("maker_asset", "null") }} as {{columns}}))
                    , ('taker_asset',      cast({{ method_data.get("taker_asset", "null") }} as {{columns}}))
                    , ('maker_max_amount', cast({{ method_data.get("maker_max_amount", "null") }} as {{columns}}))
                    , ('taker_max_amount', cast({{ method_data.get("taker_max_amount", "null") }} as {{columns}}))
                    , ('making_amount',    cast({{ method_data.get("making_amount", "null") }} as {{columns}}))
                    , ('taking_amount',    cast({{ method_data.get("taking_amount", "null") }} as {{columns}}))
                    , ('maker_fee_amount', cast({{ method_data.get("maker_fee_amount", "null") }} as {{columns}}))
                    , ('taker_fee_amount', cast({{ method_data.get("taker_fee_amount", "null") }} as {{columns}}))
                    , ('order_hash',       cast({{ method_data.get("order_hash", "null") }} as {{columns}}))
                ])
            )
        {% endfor %}
    {% endfor %}
)

, calls as (
    select
        blockchain
        , address
        , project
        , tag
        , flags
        , block_number
        , block_time
        , tx_hash
        , tx_success
        , call_from
        , call_trace_address
        , call_success
        , selector as call_selector
        , method
        , event
        , try({{ binary('call', 'maker') }}) as call_maker
        , try({{ binary('call', 'taker') }}) as call_taker
        , try({{ binary('call', 'receiver') }}) as call_receiver
        , try({{ binary('call', 'maker_asset') }}) as call_maker_asset
        , try({{ binary('call', 'taker_asset') }}) as call_taker_asset
        , try({{ amount('call', 'maker_max_amount') }}) as call_maker_max_amount
        , try({{ amount('call', 'taker_max_amount') }}) as call_taker_max_amount
        , try({{ amount('call', 'making_amount') }}) as call_making_amount
        , try({{ amount('call', 'taking_amount') }}) as call_taking_amount
        , try({{ amount('call', 'maker_fee_amount') }}) as call_maker_fee_amount
        , try({{ amount('call', 'taker_fee_amount') }}) as call_taker_fee_amount
        , try({{ binary('call', 'order_hash') }}) as call_order_hash
        , input
        , output
        , row_number() over(partition by block_number, tx_hash order by call_trace_address) as call_counter
    from (
        select
            block_number
            , block_time
            , tx_hash
            , tx_success
            , "from" as call_from
            , "to" as address
            , trace_address as call_trace_address
            , success as call_success
            , substr(input, 1, 4) as selector
            , input
            , output
        from {{ source(blockchain, 'traces') }}
        where
            {% if is_incremental() %}
                {{ incremental_predicate('block_time') }}
            {% else %}
                block_time >= timestamp '{{date_from}}'
            {% endif %}
    )
    join contracts using(address)
    join selectors using(project, selector)
)

, joined as (
    select
        *
        , count(*) over(partition by blockchain, block_number, tx_hash, call_trace_address) as rows
    from calls
    left join logs using(block_number, tx_hash, event)
    join ({{ oneinch_blockchain_macro(blockchain) }}) using(blockchain)
    where
        (call_maker = log_maker or call_maker is null or log_maker is null)
        and (call_taker = log_taker or call_taker is null or log_taker is null)
        and (call_receiver = log_receiver or call_receiver is null or log_receiver is null)
        and (call_maker_asset = log_maker_asset or call_maker_asset is null or log_maker_asset is null or cardinality(array_intersect({{wrapping}}, array[call_maker_asset, log_maker_asset])) = 2)
        and (call_taker_asset = log_taker_asset or call_taker_asset is null or log_taker_asset is null or cardinality(array_intersect({{wrapping}}, array[call_taker_asset, log_taker_asset])) = 2)
        and (call_maker_max_amount = log_maker_max_amount or call_maker_max_amount is null or log_maker_max_amount is null)
        and (call_taker_max_amount = log_taker_max_amount or call_taker_max_amount is null or log_taker_max_amount is null)
        and (call_making_amount = log_making_amount or call_making_amount is null or log_making_amount is null)
        and (call_taking_amount = log_taking_amount or call_taking_amount is null or log_taking_amount is null)
        and (call_maker_fee_amount = log_maker_fee_amount or call_maker_fee_amount is null or log_maker_fee_amount is null)
        and (call_taker_fee_amount = log_taker_fee_amount or call_taker_fee_amount is null or log_taker_fee_amount is null)
        and (call_order_hash = log_order_hash or call_order_hash is null or log_order_hash is null)
)

-- output --

select
    blockchain
    , address
    , log_contract_address
    , project
    , tag
    , flags
    , block_number
    , block_time
    , tx_hash
    , tx_success
    , call_from
    , call_trace_address
    , call_success
    , call_selector
    , coalesce(coalesce(call_maker, log_maker), address) as maker
    , coalesce(call_taker, log_taker) as taker
    , coalesce(call_receiver, log_receiver) as receiver
    , coalesce(call_maker_asset, log_maker_asset) as maker_asset
    , coalesce(call_taker_asset, log_taker_asset) as taker_asset
    , coalesce(call_maker_max_amount, log_maker_max_amount) as maker_max_amount
    , coalesce(call_taker_max_amount, log_taker_max_amount) as taker_max_amount
    , coalesce(call_making_amount, log_making_amount) as making_amount
    , coalesce(call_taking_amount, log_taking_amount) as taking_amount
    , coalesce(call_maker_fee_amount, log_maker_fee_amount) as maker_fee_amount
    , coalesce(call_taker_fee_amount, log_taker_fee_amount) as taker_fee_amount
    , coalesce(call_order_hash, log_order_hash) as order_hash
    , array[input] as call_input
    , array[output] as call_output
    , topic1 as event_topic1
    , topic2 as event_topic2
    , topic3 as event_topic3
    , array[data] as event_data
    , date(date_trunc('month', block_time)) as block_month
from joined
where
    rows = 1
    or call_counter = log_counter

{% endmacro %}