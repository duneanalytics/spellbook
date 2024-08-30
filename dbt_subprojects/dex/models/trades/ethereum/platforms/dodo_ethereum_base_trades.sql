{{
    config(
        schema = 'dodo_ethereum',
        alias = 'base_trades',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['tx_hash', 'evt_index'],
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    )
}}

{% set config_markets %}
    WITH dodo_view_markets (market_contract_address, base_token_symbol, quote_token_symbol, base_token_address, quote_token_address) AS 
    (
        VALUES
        (0x75c23271661d9d143dcb617222bc4bec783eff34, 'WETH', 'USDC', 0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2, 0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48),
        (0x562c0b218cc9ba06d9eb42f3aef54c54cc5a4650, 'LINK', 'USDC', 0x514910771af9ca656af840dff83e8264ecf986ca, 0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48),
        (0x0d04146b2fe5d267629a7eb341fb4388dcdbd22f, 'COMP', 'USDC', 0xc00e94cb662c3520282e6f5717214004a7f26888, 0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48),
        (0xca7b0632bd0e646b0f823927d3d2e61b00fe4d80, 'SNX', 'USDC',  0xc011a73ee8576fb46f5e1c5751ca3b9fe0af2a6f, 0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48),
        (0xc226118fcd120634400ce228d61e1538fb21755f, 'LEND', 'USDC', 0x80fb784b7ed66730e8b1dbd9820afd29931aab03, 0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48),
        (0x2109f78b46a789125598f5ad2b7f243751c2934d, 'WBTC', 'USDC', 0x2260fac5e5542a773aa44fbcfedf7c193bc2c599, 0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48),
        (0x1b7902a66f133d899130bf44d7d879da89913b2e, 'YFI', 'USDC',  0x0bc529c00c6401aef6d220be8c6ea1667f6ad93e, 0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48),
        (0x1a7fe5d6f0bb2d071e16bdd52c863233bbfd38e9, 'WETH', 'USDT', 0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2, 0xdac17f958d2ee523a2206206994597c13d831ec7),
        (0xc9f93163c99695c6526b799ebca2207fdf7d61ad, 'USDT', 'USDC', 0xdac17f958d2ee523a2206206994597c13d831ec7, 0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48),
        (0xd4a36b0acfe2931cf922ea3d91063ddfe4aff01f, 'sUSD', 'USDT', 0x57ab1ec28d129707052df4df418d58a2d46d5f51, 0xdac17f958d2ee523a2206206994597c13d831ec7),
        (0x8876819535b48b551c9e97ebc07332c7482b4b2d, 'DODO', 'USDT', 0x43dfc4159d86f3a37a5a4b3d4580b888ad7d4ddd, 0xdac17f958d2ee523a2206206994597c13d831ec7),
        (0x9d9793e1e18cdee6cf63818315d55244f73ec006, 'FIN', 'USDT',  0x054f76beed60ab6dbeb23502178c52d6c5debe40, 0xdac17f958d2ee523a2206206994597c13d831ec7),
        (0x94512fd4fb4feb63a6c0f4bedecc4a00ee260528, 'AAVE', 'USDC', 0x7fc66500c84a76ad7e9c93437bfc5ac33e2ddae9, 0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48),
        (0x85f9569b69083c3e6aeffd301bb2c65606b5d575, 'wCRESt','USDT',0xa0afaa285ce85974c3c881256cb7f225e3a1178a, 0xdac17f958d2ee523a2206206994597c13d831ec7),
        (0x181D93EA28023bf40C8bB94796c55138719803B4, 'WOO','USDT', 0x4691937a7508860F876c9c0a2a617E7d9E945D4B, 0xdAC17F958D2ee523a2206206994597C13D831ec7),
        (0xd48c86156D53c0F775f40883391a113fC0D690d0, 'ibEUR','USDT', 0x96E61422b6A9bA0e068B6c5ADd4fFaBC6a4aae27, 0xdAC17F958D2ee523a2206206994597C13D831ec7)
    )
    SELECT * FROM dodo_view_markets
{% endset %}

{%
    set config_other_sources = [
        {'version': '2_dvm', 'source': 'DVM_evt_DODOSwap'},
        {'version': '2_dpp', 'source': 'DPP_evt_DODOSwap'},
        {'version': '2_dsp', 'source': 'DSP_evt_DODOSwap'},
    ]
%}

{{
    dodo_compatible_trades(
        blockchain = 'ethereum',
        project = 'dodo',
        markets = config_markets,
        sell_base_token_source = 'DODO_evt_SellBaseToken',
        buy_base_token_source = 'DODO_evt_BuyBaseToken',
        other_sources = config_other_sources
    )
}}
