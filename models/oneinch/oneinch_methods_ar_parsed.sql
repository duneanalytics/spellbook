{{
    config(
        schema = 'oneinch',
        alias = alias('methods_ar_parsed'),
        materialized = 'table',
        file_format = 'delta',
        unique_key = ['contract_address', 'blockchain', 'selector'],
        tags = ['dunesql']
    )
}}

with
    
    methods as (
        
        select
            selector
            , protocol
            , topic0
            , method
            , params
            , contract_id
        from (values
            
            -- template for parsed
            -- method // project protocol // ethereum example: ...
              (0x01, 'T', 0x00, 'method'
                , map_from_entries(array[
			          ('wallet'             , map_from_entries(array[('source', 'tx-from'       ), ('start', cast(0 as varchar)), ('flag', '0x'), ('instance', '1'), ('offset', cast(0 as varchar))]))
			        , ('src_token'          , map_from_entries(array[('source', 'trace-input'   ), ('start', cast(0 as varchar)), ('flag', '0x'), ('instance', '1'), ('offset', cast(0 as varchar))]))
			        , ('dst_token'          , map_from_entries(array[('source', 'trace-input'   ), ('start', cast(0 as varchar)), ('flag', '0x'), ('instance', '1'), ('offset', cast(0 as varchar))]))
			        , ('src_amount'         , map_from_entries(array[('source', 'trace-input'   ), ('start', cast(0 as varchar)), ('flag', '0x'), ('instance', '1'), ('offset', cast(0 as varchar))]))
			        , ('dst_result_amount'  , map_from_entries(array[('source', 'trace-output'  ), ('start', cast(0 as varchar)), ('flag', '0x'), ('instance', '1'), ('offset', cast(0 as varchar))]))
			        , ('min_return_amount'  , map_from_entries(array[('source', 'trace-input'   ), ('start', cast(0 as varchar)), ('flag', '0x'), ('instance', '1'), ('offset', cast(0 as varchar))]))
			    ]), array[0x0000])
			    
			-- template pure
			-- method // project protocol // ethereum example: ...
            , (0x02, 'T', 0x00, 'method', map_from_entries(array[('wallet', map_from_entries(array[('source', 'tx-from'), ('start', cast(0 as varchar)), ('flag', ''), ('instance', ''), ('offset', cast(0 as varchar))]))]), array[0x0000])
            
			-- parsed methods
			
			-- swap // 1inch AR V1 // ethereum example: 0x26f1c14cc968d9a38ba9578b5d01a266097475116f0b3a4a87e2fb256ea3b604
            , (0xf88309d7, 'AR', 0xe2cee3f6836059820b673943853afebd9b3026125dab0d774284e6f28a4855be, 'swap'
                , map_from_entries(array[
			          ('wallet'             , map_from_entries(array[('source', 'tx-from'       ), ('start', cast(0 as varchar)), ('flag', '0x'), ('instance', '1'), ('offset', cast(0 as varchar))]))
			        , ('src_token'          , map_from_entries(array[('source', 'trace-input'   ), ('start', cast(4 + 0*32 + 12 as varchar)), ('flag', '0x'), ('instance', '1'), ('offset', cast(0 as varchar))]))
			        , ('dst_token'          , map_from_entries(array[('source', 'trace-input'   ), ('start', cast(4 + 1*32 + 12 as varchar)), ('flag', '0x'), ('instance', '1'), ('offset', cast(0 as varchar))]))
			        , ('src_amount'         , map_from_entries(array[('source', 'trace-input'   ), ('start', cast(4 + 2*32 as varchar)), ('flag', '0x'), ('instance', '1'), ('offset', cast(0 as varchar))]))
			        , ('dst_amount'         , map_from_entries(array[('source', 'trace-input'   ), ('start', cast(4 + 4*32 as varchar)), ('flag', '0x'), ('instance', '1'), ('offset', cast(0 as varchar))]))
			        , ('dst_result_amount'  , map_from_entries(array[('source', 'trace-output'  ), ('start', cast(0*32 as varchar)), ('flag', '0x'), ('instance', '1'), ('offset', cast(0 as varchar))]))
			        , ('min_return_amount'  , map_from_entries(array[('source', 'trace-input'   ), ('start', cast(4 + 3*32 as varchar)), ('flag', '0x'), ('instance', '1'), ('offset', cast(0 as varchar))]))
			    ]), array[0xa48b])
			
			-- swap // 1inch AR V2 // ethereum example: 0xd4c36cacc18a38523cb7b32589010b2ff516de4958d2f4f5964e84f0447e0116 (?)
            , (0x90411a32, 'AR', 0x76af224a143865a50b41496e1a73622698692c565c1214bc862f18e22d829c5e, 'swap'
                , map_from_entries(array[
			          ('wallet'             , map_from_entries(array[('source', 'tx-from'       ), ('start', cast(0 as varchar)), ('flag', '0x'), ('instance', '1'), ('offset', cast(0 as varchar))]))
			        , ('src_token'          , map_from_entries(array[('source', 'trace-input'   ), ('start', cast(4 + 3*32 + 12 as varchar)), ('flag', '0x'), ('instance', '1'), ('offset', cast(0 as varchar))]))
			        , ('dst_token'          , map_from_entries(array[('source', 'trace-input'   ), ('start', cast(4 + 4*32 + 12 as varchar)), ('flag', '0x'), ('instance', '1'), ('offset', cast(0 as varchar))]))
			        , ('src_amount'         , map_from_entries(array[('source', 'trace-input'   ), ('start', cast(4 + 7*32 as varchar)), ('flag', '0x'), ('instance', '1'), ('offset', cast(0 as varchar))]))
			        , ('dst_amount'         , map_from_entries(array[('source', 'trace-input'   ), ('start', cast(4 + 9*32 as varchar)), ('flag', '0x'), ('instance', '1'), ('offset', cast(0 as varchar))]))
			        , ('dst_result_amount'  , map_from_entries(array[('source', 'trace-output'  ), ('start', cast(0*32 as varchar)), ('flag', '0x'), ('instance', '1'), ('offset', cast(0 as varchar))]))
			        , ('min_return_amount'  , map_from_entries(array[('source', 'trace-input'   ), ('start', cast(4 + 8*32 as varchar)), ('flag', '0x'), ('instance', '1'), ('offset', cast(0 as varchar))]))
			    ]), array[0xf3ae, 0xf4a1])
			
			-- discountedSwap // 1inch AR V2 // ethereum example: 0x7b95dd13f2ce15a3040e569328b23e4e16b53a5bfdbf85fdfe374023ef439cf5
            , (0x34b0793b, 'AR', 0x76af224a143865a50b41496e1a73622698692c565c1214bc862f18e22d829c5e, 'discountedSwap'
                , map_from_entries(array[
			          ('wallet'             , map_from_entries(array[('source', 'tx-from'       ), ('start', cast(0 as varchar)), ('flag', '0x'), ('instance', '1'), ('offset', cast(0 as varchar))]))
			        , ('src_token'          , map_from_entries(array[('source', 'trace-input'   ), ('start', cast(4 + 3*32 + 12 as varchar)), ('flag', '0x'), ('instance', '1'), ('offset', cast(0 as varchar))]))
			        , ('dst_token'          , map_from_entries(array[('source', 'trace-input'   ), ('start', cast(4 + 4*32 + 12 as varchar)), ('flag', '0x'), ('instance', '1'), ('offset', cast(0 as varchar))]))
			        , ('src_amount'         , map_from_entries(array[('source', 'trace-input'   ), ('start', cast(4 + 7*32 as varchar)), ('flag', '0x'), ('instance', '1'), ('offset', cast(0 as varchar))]))
			        , ('dst_amount'         , map_from_entries(array[('source', 'trace-input'   ), ('start', cast(4 + 9*32 as varchar)), ('flag', '0x'), ('instance', '1'), ('offset', cast(0 as varchar))]))
			        , ('dst_result_amount'  , map_from_entries(array[('source', 'trace-output'  ), ('start', cast(0*32 as varchar)), ('flag', '0x'), ('instance', '1'), ('offset', cast(0 as varchar))]))
			        , ('min_return_amount'  , map_from_entries(array[('source', 'trace-input'   ), ('start', cast(4 + 8*32 as varchar)), ('flag', '0x'), ('instance', '1'), ('offset', cast(0 as varchar))]))
			    ]), array[0xf3ae, 0xf4a1])
			
			-- swap // 1inch AR V3, V4 // ethereum example: 0x745eac933cdf39c5438488869ce8c03fe2038eb948ad211a963f571f68bad949
            , (0x7c025200, 'AR', 0xd6d4f5681c246c9f42c203e287975af1601f8df8035a9251f79aab5c8f09e2f8, 'swap'
                , map_from_entries(array[
			          ('wallet'             , map_from_entries(array[('source', 'tx-from'       ), ('start', cast(0 as varchar)), ('flag', '0x'), ('instance', '1'), ('offset', cast(0 as varchar))]))
			        , ('src_token'          , map_from_entries(array[('source', 'trace-input'   ), ('start', cast(4 + 3*32 + 12 as varchar)), ('flag', '0x'), ('instance', '1'), ('offset', cast(0 as varchar))]))
			        , ('dst_token'          , map_from_entries(array[('source', 'trace-input'   ), ('start', cast(4 + 4*32 + 12 as varchar)), ('flag', '0x'), ('instance', '1'), ('offset', cast(0 as varchar))]))
			        , ('src_amount'         , map_from_entries(array[('source', 'trace-input'   ), ('start', cast(4 + 7*32 as varchar)), ('flag', '0x'), ('instance', '1'), ('offset', cast(0 as varchar))]))
			        , ('dst_result_amount'  , map_from_entries(array[('source', 'trace-output'  ), ('start', cast(0*32 as varchar)), ('flag', '0x'), ('instance', '1'), ('offset', cast(0 as varchar))]))
			        , ('min_return_amount'  , map_from_entries(array[('source', 'trace-input'   ), ('start', cast(4 + 8*32 as varchar)), ('flag', '0x'), ('instance', '1'), ('offset', cast(0 as varchar))]))
			    ]), array[0xaa26, 0x097d, 0xd199])
			
			-- unoswap // 1inch AR V3, V4 // ethereum example: 0xfeb8ea41abd59716b1decc1f6b890b962853e7748090f05e94930ab8eb4c7bd4
            , (0x2e95b6c8, 'AR', 0x00, 'unoswap'
                , map_from_entries(array[
			          ('wallet'             , map_from_entries(array[('source', 'tx-from'       ), ('start', cast(0 as varchar)), ('flag', '0x'), ('instance', '1'), ('offset', cast(0 as varchar))]))
			        , ('src_token'          , map_from_entries(array[('source', 'trace-input'   ), ('start', cast(4 + 0*32 + 12 as varchar)), ('flag', '0x'), ('instance', '1'), ('offset', cast(0 as varchar))]))
			        , ('dst_token'          , map_from_entries(array[('source', 'not'           ), ('start', cast(0 as varchar)), ('flag', '0x'), ('instance', '1'), ('offset', cast(0 as varchar))]))
			        , ('src_amount'         , map_from_entries(array[('source', 'trace-input'   ), ('start', cast(4 + 1*32 as varchar)), ('flag', '0x'), ('instance', '1'), ('offset', cast(0 as varchar))]))
			        , ('dst_result_amount'  , map_from_entries(array[('source', 'trace-output'  ), ('start', cast(0*32 as varchar)), ('flag', '0x'), ('instance', '1'), ('offset', cast(0 as varchar))]))
			        , ('min_return_amount'  , map_from_entries(array[('source', 'trace-input'   ), ('start', cast(4 + 2*32 as varchar)), ('flag', '0x'), ('instance', '1'), ('offset', cast(0 as varchar))]))
			    ]), array[0xaa26, 0x097d, 0xd199])
			
			-- discountedSwap // 1inch AR V3 // ethereum example: 0x4e2b32285087b42d516cb30390e57cca735cd7c427a451ddea52f62c0927a4b3
            , (0x6c4a483e, 'AR', 0x00, 'discountedSwap'
                , map_from_entries(array[
			          ('wallet'             , map_from_entries(array[('source', 'tx-from'       ), ('start', cast(0 as varchar)), ('flag', '0x'), ('instance', '1'), ('offset', cast(0 as varchar))]))
			        , ('src_token'          , map_from_entries(array[('source', 'trace-input'   ), ('start', cast(4 + 3*32 + 12 as varchar)), ('flag', '0x'), ('instance', '1'), ('offset', cast(0 as varchar))]))
			        , ('dst_token'          , map_from_entries(array[('source', 'trace-input'   ), ('start', cast(4 + 4*32 + 12 as varchar)), ('flag', '0x'), ('instance', '1'), ('offset', cast(0 as varchar))]))
			        , ('src_amount'         , map_from_entries(array[('source', 'trace-input'   ), ('start', cast(4 + 7*32 as varchar)), ('flag', '0x'), ('instance', '1'), ('offset', cast(0 as varchar))]))
			        , ('dst_result_amount'  , map_from_entries(array[('source', 'trace-output'  ), ('start', cast(0*32 as varchar)), ('flag', '0x'), ('instance', '1'), ('offset', cast(0 as varchar))]))
			        , ('min_return_amount'  , map_from_entries(array[('source', 'trace-input'   ), ('start', cast(4 + 8*32 as varchar)), ('flag', '0x'), ('instance', '1'), ('offset', cast(0 as varchar))]))
			    ]), array[0xaa26])
            
            -- unoswapWithPermit // 1inch AR V3, V4 // ethereum example: 0xd28d4947afe6aa2602f0a8af77410385f342f5ccba186ac2672a2865ed11bb67
            , (0xa1251d75, 'AR', 0x00, 'unoswapWithPermit'
                , map_from_entries(array[
			          ('wallet'             , map_from_entries(array[('source', 'tx-from'       ), ('start', cast(0 as varchar)), ('flag', '0x'), ('instance', '1'), ('offset', cast(0 as varchar))]))
			        , ('src_token'          , map_from_entries(array[('source', 'trace-input'   ), ('start', cast(4 + 0*32 + 12 as varchar)), ('flag', '0x'), ('instance', '1'), ('offset', cast(0 as varchar))]))
			        , ('dst_token'          , map_from_entries(array[('source', 'not'           ), ('start', cast(0 as varchar)), ('flag', '0x'), ('instance', '1'), ('offset', cast(0 as varchar))]))
			        , ('src_amount'         , map_from_entries(array[('source', 'trace-input'   ), ('start', cast(4 + 1*32 as varchar)), ('flag', '0x'), ('instance', '1'), ('offset', cast(0 as varchar))]))
			        , ('dst_result_amount'  , map_from_entries(array[('source', 'trace-output'  ), ('start', cast(0*32 as varchar)), ('flag', '0x'), ('instance', '1'), ('offset', cast(0 as varchar))]))
			        , ('min_return_amount'  , map_from_entries(array[('source', 'trace-input'   ), ('start', cast(4 + 2*32 as varchar)), ('flag', '0x'), ('instance', '1'), ('offset', cast(0 as varchar))]))
			    ]), array[0xaa26, 0x097d, 0xd199])
			
			-- uniswapV3Swap // 1inch AR V4, V5 // ethereum example: 0x729737489150884f674b93ca521e2edf83a6ed8c90e49096b0c47350b242827b
            , (0xe449022e, 'AR', 0x00, 'uniswapV3Swap'
                , map_from_entries(array[
			          ('wallet'             , map_from_entries(array[('source', 'tx-from'       ), ('start', cast(0 as varchar)), ('flag', '0x'), ('instance', '1'), ('offset', cast(0 as varchar))]))
			        , ('src_token'          , map_from_entries(array[('source', 'not'           ), ('start', cast(0 as varchar)), ('flag', '0x'), ('instance', '1'), ('offset', cast(0 as varchar))]))
			        , ('dst_token'          , map_from_entries(array[('source', 'not'           ), ('start', cast(0 as varchar)), ('flag', '0x'), ('instance', '1'), ('offset', cast(0 as varchar))]))
			        , ('src_amount'         , map_from_entries(array[('source', 'trace-input'   ), ('start', cast(4 + 0*32 as varchar)), ('flag', '0x'), ('instance', '1'), ('offset', cast(0 as varchar))]))
			        , ('dst_result_amount'  , map_from_entries(array[('source', 'trace-output'  ), ('start', cast(0*32 as varchar)), ('flag', '0x'), ('instance', '1'), ('offset', cast(0 as varchar))]))
			        , ('min_return_amount'  , map_from_entries(array[('source', 'trace-input'   ), ('start', cast(4 + 1*32 as varchar)), ('flag', '0x'), ('instance', '1'), ('offset', cast(0 as varchar))]))
			    ]), array[0x097d, 0xd199, 0x0582])
			
			-- uniswapV3SwapTo // 1inch AR V4 // ethereum example: 0xc762bcf147d16df3bd2b8a319b0d33a4d2e31f59b523a0d64acf5d00bd92c65d
            , (0xbc80f1a8, 'AR', 0x00, 'uniswapV3SwapTo'
                , map_from_entries(array[
			          ('wallet'             , map_from_entries(array[('source', 'tx-from'       ), ('start', cast(0 as varchar)), ('flag', '0x'), ('instance', '1'), ('offset', cast(0 as varchar))]))
			        , ('receiver'           , map_from_entries(array[('source', 'trace-input'   ), ('start', cast(4 + 0*32 + 12 as varchar)), ('flag', '0x'), ('instance', '1'), ('offset', cast(0 as varchar))]))
			        , ('src_token'          , map_from_entries(array[('source', 'not'           ), ('start', cast(0 as varchar)), ('flag', '0x'), ('instance', '1'), ('offset', cast(0 as varchar))]))
			        , ('dst_token'          , map_from_entries(array[('source', 'not'           ), ('start', cast(0 as varchar)), ('flag', '0x'), ('instance', '1'), ('offset', cast(0 as varchar))]))
			        , ('src_amount'         , map_from_entries(array[('source', 'trace-input'   ), ('start', cast(4 + 1*32 as varchar)), ('flag', '0x'), ('instance', '1'), ('offset', cast(0 as varchar))]))
			        , ('dst_result_amount'  , map_from_entries(array[('source', 'trace-output'  ), ('start', cast(0*32 as varchar)), ('flag', '0x'), ('instance', '1'), ('offset', cast(0 as varchar))]))
			        , ('min_return_amount'  , map_from_entries(array[('source', 'trace-input'   ), ('start', cast(4 + 2*32 as varchar)), ('flag', '0x'), ('instance', '1'), ('offset', cast(0 as varchar))]))
			    ]), array[0x097d, 0xd199])
			
			-- uniswapV3SwapToWithPermit // 1inch AR V4 // ethereum example: 0x1b251d13fd530ddf2d4125631c71ee07b56568c1a6cf55a8e53a29a599b81e92, 0x1b251d13fd530ddf2d4125631c71ee07b56568c1a6cf55a8e53a29a599b81e92
            , (0x2521b930, 'AR', 0x00, 'uniswapV3SwapToWithPermit'
                , map_from_entries(array[
			          ('wallet'             , map_from_entries(array[('source', 'tx-from'       ), ('start', cast(0 as varchar)), ('flag', '0x'), ('instance', '1'), ('offset', cast(0 as varchar))]))
			        , ('receiver'           , map_from_entries(array[('source', 'trace-input'   ), ('start', cast(4 + 0*32 + 12 as varchar)), ('flag', '0x'), ('instance', '1'), ('offset', cast(0 as varchar))]))
			        , ('src_token'          , map_from_entries(array[('source', 'trace-input'   ), ('start', cast(4 + 1*32 + 12 as varchar)), ('flag', '0x'), ('instance', '1'), ('offset', cast(0 as varchar))]))
			        , ('dst_token'          , map_from_entries(array[('source', 'not'           ), ('start', cast(0 as varchar)), ('flag', '0x'), ('instance', '1'), ('offset', cast(0 as varchar))]))
			        , ('src_amount'         , map_from_entries(array[('source', 'trace-input'   ), ('start', cast(4 + 2*32 as varchar)), ('flag', '0x'), ('instance', '1'), ('offset', cast(0 as varchar))]))
			        , ('dst_result_amount'  , map_from_entries(array[('source', 'trace-output'  ), ('start', cast(0*32 as varchar)), ('flag', '0x'), ('instance', '1'), ('offset', cast(0 as varchar))]))
			        , ('min_return_amount'  , map_from_entries(array[('source', 'trace-input'   ), ('start', cast(4 + 3*32 as varchar)), ('flag', '0x'), ('instance', '1'), ('offset', cast(0 as varchar))]))
			    ]), array[0x097d, 0xd199])
            
            -- swap // 1inch AR V5 // ethereum example: 0x571d46da95231ba558d87cb27c67efd50c7adca076b17c7f20a1cdbcb1c802ab, 0xd54c3c7b71972c668d758ba9651b1e3c7a3c21534e59444c15100a10ef36913e
            , (0x12aa3caf, 'AR', 0x00, 'swap'
                , map_from_entries(array[
			          ('wallet'             , map_from_entries(array[('source', 'tx-from'       ), ('start', cast(0 as varchar)), ('flag', '0x'), ('instance', '1'), ('offset', cast(0 as varchar))]))
			        , ('receiver'           , map_from_entries(array[('source', 'trace-input'   ), ('start', cast(4 + 4*32 + 12 as varchar)), ('flag', '0x'), ('instance', '1'), ('offset', cast(0 as varchar))]))
			        , ('src_token'          , map_from_entries(array[('source', 'trace-input'   ), ('start', cast(4 + 1*32 + 12 as varchar)), ('flag', '0x'), ('instance', '1'), ('offset', cast(0 as varchar))]))
			        , ('dst_token'          , map_from_entries(array[('source', 'trace-input'   ), ('start', cast(4 + 2*32 + 12 as varchar)), ('flag', '0x'), ('instance', '1'), ('offset', cast(0 as varchar))]))
			        , ('src_amount'         , map_from_entries(array[('source', 'trace-input'   ), ('start', cast(4 + 5*32 as varchar)), ('flag', '0x'), ('instance', '1'), ('offset', cast(0 as varchar))]))
			        , ('dst_result_amount'  , map_from_entries(array[('source', 'trace-output'  ), ('start', cast(0*32 as varchar)), ('flag', '0x'), ('instance', '1'), ('offset', cast(0 as varchar))]))
			        , ('min_return_amount'  , map_from_entries(array[('source', 'trace-input'   ), ('start', cast(4 + 6*32 as varchar)), ('flag', '0x'), ('instance', '1'), ('offset', cast(0 as varchar))]))
			    ]), array[0x0582])
            
            -- unoswap // 1inch AR V5 // ethereum example: 0x14828e8bf205e308dcb9d0d082b775b28bc7b3ebb2d132f24a294bb98516f108
            , (0x0502b1c5, 'AR', 0x00, 'unoswap'
                , map_from_entries(array[
			          ('wallet'             , map_from_entries(array[('source', 'tx-from'       ), ('start', cast(0 as varchar)), ('flag', '0x'), ('instance', '1'), ('offset', cast(0 as varchar))]))
			        , ('src_token'          , map_from_entries(array[('source', 'trace-input'   ), ('start', cast(4 + 0*32 + 12 as varchar)), ('flag', '0x'), ('instance', '1'), ('offset', cast(0 as varchar))]))
			        , ('dst_token'          , map_from_entries(array[('source', 'not'           ), ('start', cast(0 as varchar)), ('flag', '0x'), ('instance', '1'), ('offset', cast(0 as varchar))]))
			        , ('src_amount'         , map_from_entries(array[('source', 'trace-input'   ), ('start', cast(4 + 1*32 as varchar)), ('flag', '0x'), ('instance', '1'), ('offset', cast(0 as varchar))]))
			        , ('dst_result_amount'  , map_from_entries(array[('source', 'trace-output'  ), ('start', cast(0*32 as varchar)), ('flag', '0x'), ('instance', '1'), ('offset', cast(0 as varchar))]))
			        , ('min_return_amount'  , map_from_entries(array[('source', 'trace-input'   ), ('start', cast(4 + 2*32 as varchar)), ('flag', '0x'), ('instance', '1'), ('offset', cast(0 as varchar))]))
			    ]), array[0x0582])
            
            -- unoswapTo // 1inch AR V5 // ethereum example: 0xeed7a552a94ca17eb839c7ba609c28ffa79a799613a67467e64e1b7f6a9c6d47
            , (0xf78dc253, 'AR', 0x00, 'unoswapTo'
                , map_from_entries(array[
			          ('wallet'             , map_from_entries(array[('source', 'tx-from'       ), ('start', cast(0 as varchar)), ('flag', '0x'), ('instance', '1'), ('offset', cast(0 as varchar))]))
			        , ('receiver'           , map_from_entries(array[('source', 'trace-input'   ), ('start', cast(4 + 0*32 + 12 as varchar)), ('flag', '0x'), ('instance', '1'), ('offset', cast(0 as varchar))]))
			        , ('src_token'          , map_from_entries(array[('source', 'trace-input'   ), ('start', cast(4 + 1*32 + 12 as varchar)), ('flag', '0x'), ('instance', '1'), ('offset', cast(0 as varchar))]))
			        , ('dst_token'          , map_from_entries(array[('source', 'not'           ), ('start', cast(0 as varchar)), ('flag', '0x'), ('instance', '1'), ('offset', cast(0 as varchar))]))
			        , ('src_amount'         , map_from_entries(array[('source', 'trace-input'   ), ('start', cast(4 + 2*32 as varchar)), ('flag', '0x'), ('instance', '1'), ('offset', cast(0 as varchar))]))
			        , ('dst_result_amount'  , map_from_entries(array[('source', 'trace-output'  ), ('start', cast(0*32 as varchar)), ('flag', '0x'), ('instance', '1'), ('offset', cast(0 as varchar))]))
			        , ('min_return_amount'  , map_from_entries(array[('source', 'trace-input'   ), ('start', cast(4 + 3*32 as varchar)), ('flag', '0x'), ('instance', '1'), ('offset', cast(0 as varchar))]))
			    ]), array[0x0582])
			
			-- unoswapToWithPermit // 1inch AR V5 // ethereum example: 0x9decc61aede10c0905b05344e4264d29d5b1f0934bcb1c8f20b2995b8888da0d
            , (0x3c15fd91, 'AR', 0x00, 'unoswapToWithPermit'
                , map_from_entries(array[
			          ('wallet'             , map_from_entries(array[('source', 'tx-from'       ), ('start', cast(0 as varchar)), ('flag', '0x'), ('instance', '1'), ('offset', cast(0 as varchar))]))
			        , ('receiver'           , map_from_entries(array[('source', 'trace-input'   ), ('start', cast(4 + 0*32 + 12 as varchar)), ('flag', '0x'), ('instance', '1'), ('offset', cast(0 as varchar))]))
			        , ('src_token'          , map_from_entries(array[('source', 'trace-input'   ), ('start', cast(4 + 1*32 + 12 as varchar)), ('flag', '0x'), ('instance', '1'), ('offset', cast(0 as varchar))]))
			        , ('dst_token'          , map_from_entries(array[('source', 'not'           ), ('start', cast(0 as varchar)), ('flag', '0x'), ('instance', '1'), ('offset', cast(0 as varchar))]))
			        , ('src_amount'         , map_from_entries(array[('source', 'trace-input'   ), ('start', cast(4 + 2*32 as varchar)), ('flag', '0x'), ('instance', '1'), ('offset', cast(0 as varchar))]))
			        , ('dst_result_amount'  , map_from_entries(array[('source', 'trace-output'  ), ('start', cast(0*32 as varchar)), ('flag', '0x'), ('instance', '1'), ('offset', cast(0 as varchar))]))
			        , ('min_return_amount'  , map_from_entries(array[('source', 'trace-input'   ), ('start', cast(4 + 3*32 as varchar)), ('flag', '0x'), ('instance', '1'), ('offset', cast(0 as varchar))]))
			    ]), array[0x0582])
            
            -- ######### ######### ######### ######### ######### ######### ######### ######### --
            -- ######### ######### ######### ######### ######### ######### ######### ######### --
            -- ######### ######### ######### ######### ######### ######### ######### ######### --
            -- ######### ######### ######### ######### ######### ######### ######### ######### --
            
            -- proxiedSwap // ZeroEx AR // ethereum example: 0xfb9489a4d0da999c440913a368de87e1def655f759f10e6d53d187dc9893c6ac
            , (0x5cf54026, 'AR', 0x00, 'proxiedSwap'
			    , map_from_entries(array[
			          ('wallet'             , map_from_entries(array[('source', 'tx-from'       ), ('start', cast(0    as varchar)), ('flag', '0x'), ('instance', '1'), ('offset', cast(0 as varchar))]))
			        , ('src_token'          , map_from_entries(array[('source', 'trace-input'   ), ('start', cast(4 + 2*32 + 12 as varchar)), ('flag', '0x'), ('instance', '1'), ('offset', cast(0 as varchar))]))
			        , ('dst_token'          , map_from_entries(array[('source', 'trace-input'   ), ('start', cast(4 + 4*32 + 12 as varchar)), ('flag', '0x'), ('instance', '1'), ('offset', cast(0 as varchar))]))
			        , ('fee_token'          , map_from_entries(array[('source', 'trace-input'   ), ('start', cast(4 + 1*32 + 12 as varchar)), ('flag', '0x'), ('instance', '1'), ('offset', cast(0 as varchar))]))
			        , ('src_amount'         , map_from_entries(array[('source', 'trace-input'   ), ('start', cast(4 + 3*32 as varchar)), ('flag', '0x'), ('instance', '1'), ('offset', cast(0 as varchar))]))
			        , ('fee_amount'         , map_from_entries(array[('source', 'trace-input'   ), ('start', cast(4 + 5*32 as varchar)), ('flag', '0x'), ('instance', '1'), ('offset', cast(0 as varchar))]))
			        , ('dst_result_amount'  , map_from_entries(array[('source', 'trace-output'  ), ('start', cast(0*32 as varchar)), ('flag', '0x'), ('instance', '1'), ('offset', cast(0 as varchar))]))
			        , ('min_return_amount'  , map_from_entries(array[('source', 'trace-input'   ), ('start', cast(4 + 2*32 as varchar)), ('flag', '0xd9627aa4'), ('instance', '1'), ('offset', cast(0 as varchar))]))
			    ]), array[0x3750])
			
			-- optimalSwap // ZeroEx AR // ethereum example: 0xa0de4e94b85f9e0df011ef4cefcc95a923bb934721033d670ca84826c866de96
            , (0xfe53d0b6, 'AR', 0x00, 'optimalSwap'
			    , map_from_entries(array[
			          ('wallet'             , map_from_entries(array[('source', 'tx-from'       ), ('start', cast(0    as varchar)), ('flag', '0x'), ('instance', '1'), ('offset', cast(0 as varchar))]))
			        , ('src_token'          , map_from_entries(array[('source', 'trace-input'   ), ('start', cast(4 + 5*32 + 12 as varchar)), ('flag', '0xd9627aa4'), ('instance', '1'), ('offset', cast(0 as varchar))]))
			        , ('dst_token'          , map_from_entries(array[('source', 'trace-input'   ), ('start', cast(4 + 6*32 + 12 as varchar)), ('flag', '0xd9627aa4'), ('instance', '1'), ('offset', cast(0 as varchar))]))
			        , ('fee_token'          , map_from_entries(array[('source', 'trace-input'   ), ('start', cast(4 + 1*32 + 12 as varchar)), ('flag', '0x'), ('instance', '1'), ('offset', cast(0 as varchar))]))
			        , ('src_amount'         , map_from_entries(array[('source', 'trace-input'   ), ('start', cast(4 + 1*32 as varchar)), ('flag', '0xd9627aa4'), ('instance', '1'), ('offset', cast(0 as varchar))]))
			        , ('fee_amount'         , map_from_entries(array[('source', 'trace-input'   ), ('start', cast(4 + 2*32 as varchar)), ('flag', '0x'), ('instance', '1'), ('offset', cast(0 as varchar))]))
			        , ('dst_result_amount'  , map_from_entries(array[('source', 'trace-output'  ), ('start', cast(0*32 as varchar)), ('flag', '0x'), ('instance', '1'), ('offset', cast(0 as varchar))]))
			        , ('min_return_amount'  , map_from_entries(array[('source', 'trace-input'   ), ('start', cast(4 + 2*32 as varchar)), ('flag', '0xd9627aa4'), ('instance', '1'), ('offset', cast(0 as varchar))]))
			    ]), array[0x3750])
		    
		    -- batchFill // ZeroEx AR // ethereum example: 0xe980122156844b44f4c6c4f7fa0cef941faa4f7d28865051189bd0b36f70a35d, 0xe9d1af55ebd9db595a07b3ed8bf18ce74d80cbe57bf0f5780748cebd9b4b4971
            , (0xafc6728e, 'AR', 0x00, 'batchFill'
			    , map_from_entries(array[
			          ('wallet'             , map_from_entries(array[('source', 'tx-from'       ), ('start', cast(0    as varchar)), ('flag', '0x'), ('instance', '1'), ('offset', cast(0 as varchar))]))
			        , ('src_token'          , map_from_entries(array[('source', 'trace-input'   ), ('start', cast(4 + 2*32 + 12 as varchar)), ('flag', '0x'), ('instance', '1'), ('offset', cast(0 as varchar))]))
			        , ('dst_token'          , map_from_entries(array[('source', 'trace-input'   ), ('start', cast(4 + 3*32 + 12 as varchar)), ('flag', '0x'), ('instance', '1'), ('offset', cast(0 as varchar))]))
			        , ('src_amount'         , map_from_entries(array[('source', 'trace-input'   ), ('start', cast(4 + 4*32 as varchar)), ('flag', '0x'), ('instance', '1'), ('offset', cast(0 as varchar))]))
			        , ('dst_result_amount'  , map_from_entries(array[('source', 'trace-output'  ), ('start', cast(0*32 as varchar)), ('flag', '0x'), ('instance', '1'), ('offset', cast(0 as varchar))]))
			        , ('min_return_amount'  , map_from_entries(array[('source', 'trace-input'   ), ('start', cast(4 + 1*32 as varchar)), ('flag', '0x'), ('instance', '1'), ('offset', cast(0 as varchar))]))
			    ]), array[0x5eff])
		    
		    -- sellToUniswap // ZeroEx AR // ethereum example: 0x667052490845fbc6273e39de07430013a9491dcebd37b8fb000306cf51d81a42
            , (0xd9627aa4, 'AR', 0x00, 'sellToUniswap'
			    , map_from_entries(array[
			          ('wallet'             , map_from_entries(array[('source', 'tx-from'       ), ('start', cast(0    as varchar)), ('flag', '0x'), ('instance', '1'), ('offset', cast(0 as varchar))]))
			        , ('src_token'          , map_from_entries(array[('source', 'trace-input'   ), ('start', cast(4 + 5*32 + 12 as varchar)), ('flag', '0x'), ('instance', '1'), ('offset', cast(0 as varchar))]))
			        , ('dst_token'          , map_from_entries(array[('source', 'trace-input'   ), ('start', cast(4 + 6*32 + 12 as varchar)), ('flag', '0x'), ('instance', '1'), ('offset', cast(0 as varchar))]))
			        , ('src_amount'         , map_from_entries(array[('source', 'trace-input'   ), ('start', cast(4 + 1*32 as varchar)), ('flag', '0x'), ('instance', '1'), ('offset', cast(0 as varchar))]))
			        , ('dst_result_amount'  , map_from_entries(array[('source', 'trace-output'  ), ('start', cast(0*32 as varchar)), ('flag', '0x'), ('instance', '1'), ('offset', cast(0 as varchar))]))
			        , ('min_return_amount'  , map_from_entries(array[('source', 'trace-input'   ), ('start', cast(4 + 2*32 as varchar)), ('flag', '0x'), ('instance', '1'), ('offset', cast(0 as varchar))]))
			    ]), array[0x5eff])
		    
		    -- sellTokenForTokenToUniswapV3 // ZeroEx AR // ethereum example: 0xb5d186397b106775a084b7cf172c431c0c444fc8d5443df0eedb7ca5a19f9e86
            , (0x6af479b2, 'AR', 0x00, 'sellTokenForTokenToUniswapV3'
			    , map_from_entries(array[
			          ('wallet'             , map_from_entries(array[('source', 'tx-from'       ), ('start', cast(0    as varchar)), ('flag', '0x'), ('instance', '1'), ('offset', cast(0 as varchar))]))
			        , ('src_token'          , map_from_entries(array[('source', 'trace-input'   ), ('start', cast(4 + 5*32 as varchar)), ('flag', '0x'), ('instance', '1'), ('offset', cast(0 as varchar))]))
			        , ('dst_token'          , map_from_entries(array[('source', 'trace-input'   ), ('start', cast(4 + 5*32 + 23 as varchar)), ('flag', '0x'), ('instance', '1'), ('offset', cast(0 as varchar))]))
			        , ('src_amount'         , map_from_entries(array[('source', 'trace-input'   ), ('start', cast(4 + 1*32 as varchar)), ('flag', '0x'), ('instance', '1'), ('offset', cast(0 as varchar))]))
			        , ('dst_result_amount'  , map_from_entries(array[('source', 'trace-output'  ), ('start', cast(0*32 as varchar)), ('flag', '0x'), ('instance', '1'), ('offset', cast(0 as varchar))]))
			        , ('min_return_amount'  , map_from_entries(array[('source', 'trace-input'   ), ('start', cast(4 + 2*32 as varchar)), ('flag', '0x'), ('instance', '1'), ('offset', cast(0 as varchar))]))
			    ]), array[0x5eff])
		    
		    -- _sellTokenForTokenToUniswapV3 // ZeroEx AR // ethereum example: 0x1f7c258179eb4cb0ac8cfc5d88ba0428f28693a83598eabcb6458e9a2fea17a6, 0xfff18f2b60bfe1272bbe31a6e650b81d10cb59dfee756e5253e01ade730325de
            , (0x168a6432, 'AR', 0x00, '_sellTokenForTokenToUniswapV3'
			    , map_from_entries(array[
			          ('wallet'             , map_from_entries(array[('source', 'tx-from'       ), ('start', cast(0    as varchar)), ('flag', '0x'), ('instance', '1'), ('offset', cast(0 as varchar))]))
			        , ('src_token'          , map_from_entries(array[('source', 'trace-input'   ), ('start', cast(4 + 6*32 as varchar)), ('flag', '0x'), ('instance', '1'), ('offset', cast(0 as varchar))]))
			        , ('dst_token'          , map_from_entries(array[('source', 'trace-input'   ), ('start', cast(4 + 6*32 + 23 as varchar)), ('flag', '0x'), ('instance', '1'), ('offset', cast(0 as varchar))]))
			        , ('src_amount'         , map_from_entries(array[('source', 'trace-input'   ), ('start', cast(4 + 1*32 as varchar)), ('flag', '0x'), ('instance', '1'), ('offset', cast(0 as varchar))]))
			        , ('dst_result_amount'  , map_from_entries(array[('source', 'trace-output'  ), ('start', cast(0*32 as varchar)), ('flag', '0x'), ('instance', '1'), ('offset', cast(0 as varchar))]))
			        , ('min_return_amount'  , map_from_entries(array[('source', 'trace-input'   ), ('start', cast(4 + 2*32 as varchar)), ('flag', '0x'), ('instance', '1'), ('offset', cast(0 as varchar))]))
			    ]), array[0x5eff])
		    
		    -- multiHopFill // ZeroEx AR // ethereum example: 0xd3fc02bb2098d7f5439c5811459aa9ff338180b8af6cfbbda84ab77feaf2298d, 0xff57774366bc247dcad1fa959c3a83b66a482cab45bc8c03f197f102896e3d0e
            , (0x21c184b6, 'AR', 0x00, 'multiHopFill'
			    , map_from_entries(array[
			          ('wallet'             , map_from_entries(array[('source', 'tx-from'       ), ('start', cast(0    as varchar)), ('flag', '0x'), ('instance', '1'), ('offset', cast(0 as varchar))]))
			        , ('src_token'          , map_from_entries(array[('source', 'trace-input'   ), ('start', cast(4 + 6*32 + 12 as varchar)), ('flag', '0x'), ('instance', '1'), ('offset', cast(0 as varchar))]))
			        , ('dst_token'          , map_from_entries(array[('source', 'trace-input'   ), ('start', cast(4 + 5*32 as varchar)), ('flag', '0x'), ('instance', '1'), ('offset', cast(5*32 as varchar))]))
			        , ('src_amount'         , map_from_entries(array[('source', 'trace-input'   ), ('start', cast(4 + 3*32 as varchar)), ('flag', '0x'), ('instance', '1'), ('offset', cast(0 as varchar))]))
			        , ('dst_result_amount'  , map_from_entries(array[('source', 'trace-output'  ), ('start', cast(0*32 as varchar)), ('flag', '0x'), ('instance', '1'), ('offset', cast(0 as varchar))]))
			        , ('min_return_amount'  , map_from_entries(array[('source', 'trace-input'   ), ('start', cast(4 + 1*32 as varchar)), ('flag', '0x'), ('instance', '1'), ('offset', cast(0 as varchar))]))
			    ]), array[0x5eff])
		    
		    -- executeMetaTransaction // ZeroEx AR // ethereum example: 0xc128e0c099c9419e5775dec2c0a41292859cdf8ca880d80142338f3ce4d151ab
		    , (0x3d61ed3e, 'AR', 0x00, 'executeMetaTransaction'
			    , map_from_entries(array[
			          ('wallet'             , map_from_entries(array[('source', 'trace-input'   ), ('start', cast(4 + 5*32 + 12 as varchar)), ('flag', '0x'), ('instance', '1'), ('offset', cast(0 as varchar))]))
			        , ('src_token'          , map_from_entries(array[('source', 'trace-input'   ), ('start', cast(4 + 16*32 + 12 as varchar)), ('flag', '0x'), ('instance', '1'), ('offset', cast(0 as varchar))]))
			        , ('dst_token'          , map_from_entries(array[('source', 'not'           ), ('start', cast(0 as varchar)), ('flag', '0x'), ('instance', '1'), ('offset', cast(0 as varchar))]))
			        , ('src_amount'         , map_from_entries(array[('source', 'not'           ), ('start', cast(0 as varchar)), ('flag', '0x'), ('instance', '1'), ('offset', cast(0 as varchar))]))
			        , ('dst_result_amount'  , map_from_entries(array[('source', 'trace-output'  ), ('start', cast(2*32  as varchar)), ('flag', '0x'), ('instance', '1'), ('offset', cast(0 as varchar))]))
			        , ('min_return_amount'  , map_from_entries(array[('source', 'not'           ), ('start', cast(0 as varchar)), ('flag', '0x'), ('instance', '1'), ('offset', cast(0 as varchar))]))
			    ]), array[0x5eff])
		    
		    -- executeMetaTransactionV2 // ZeroEx AR // ethereum example: 0xb7842a58b6574b9befdf3e54c6ee5dd756b8f7a34b519900fb409cb84613f6ce
		    , (0x3d8d4082, 'AR', 0x00, 'executeMetaTransactionV2'
			    , map_from_entries(array[
			          ('wallet'             , map_from_entries(array[('source', 'trace-input'   ), ('start', cast(4 + 5*32 + 12 as varchar)), ('flag', '0x'), ('instance', '1'), ('offset', cast(0 as varchar))]))
			        , ('src_token'          , map_from_entries(array[('source', 'trace-input'   ), ('start', cast(4 + 10*32 + 12 as varchar)), ('flag', '0x'), ('instance', '1'), ('offset', cast(0 as varchar))]))
			        , ('dst_token'          , map_from_entries(array[('source', 'not'           ), ('start', cast(0 as varchar)), ('flag', '0x'), ('instance', '1'), ('offset', cast(0 as varchar))]))
			        , ('src_amount'         , map_from_entries(array[('source', 'not'           ), ('start', cast(0 as varchar)), ('flag', '0x'), ('instance', '1'), ('offset', cast(0 as varchar))]))
			        , ('dst_result_amount'  , map_from_entries(array[('source', 'trace-output'  ), ('start', cast(2*32  as varchar)), ('flag', '0x'), ('instance', '1'), ('offset', cast(0 as varchar))]))
			        , ('min_return_amount'  , map_from_entries(array[('source', 'not'           ), ('start', cast(0 as varchar)), ('flag', '0x'), ('instance', '1'), ('offset', cast(0 as varchar))]))
			    ]), array[0x5eff])
		    
		    -- _multiplexBatchSell // ZeroEx AR // ethereum example: 0xb7842a58b6574b9befdf3e54c6ee5dd756b8f7a34b519900fb409cb84613f6ce
		    , (0x43475db9, 'AR', 0x00, '_multiplexBatchSell'
			    , map_from_entries(array[
			          ('wallet'             , map_from_entries(array[('source', 'trace-input'   ), ('start', cast(4 + 8*32 + 12 as varchar)), ('flag', '0x'), ('instance', '1'), ('offset', cast(0 as varchar))]))
			        , ('src_token'          , map_from_entries(array[('source', 'trace-input'   ), ('start', cast(4 + 2*32 + 12 as varchar)), ('flag', '0x'), ('instance', '1'), ('offset', cast(0 as varchar))]))
			        , ('dst_token'          , map_from_entries(array[('source', 'trace-input'   ), ('start', cast(4 + 3*32 + 12 as varchar)), ('flag', '0x'), ('instance', '1'), ('offset', cast(0 as varchar))]))
			        , ('src_amount'         , map_from_entries(array[('source', 'trace-value'   ), ('start', cast(4*32  as varchar)), ('flag', '0x'), ('instance', '1'), ('offset', cast(0 as varchar))]))
			        , ('dst_result_amount'  , map_from_entries(array[('source', 'trace-output'  ), ('start', cast(0*32  as varchar)), ('flag', '0x'), ('instance', '1'), ('offset', cast(0 as varchar))]))
			        , ('min_return_amount'  , map_from_entries(array[('source', 'trace-input'   ), ('start', cast(4 + 1*32  as varchar)), ('flag', '0x'), ('instance', '1'), ('offset', cast(0 as varchar))]))
			    ]), array[0x5eff])
		    
		    -- _multiplexMultiHopSell // ZeroEx AR // ethereum example: 0x1d392111af4c11830a3b1930b81fd2daab66f3fe488f69f6cf4b6b6f3ef77848
            , (0x59517361, 'AR', 0x00, '_multiplexMultiHopSell'
			    , map_from_entries(array[
			          ('wallet'             , map_from_entries(array[('source', 'trace-input'   ), ('start', cast(4 + 7*32 + 12 as varchar)), ('flag', '0x'), ('instance', '1'), ('offset', cast(0 as varchar))]))
			        , ('src_token'          , map_from_entries(array[('source', 'trace-input'   ), ('start', cast(4 + 9*32 + 12 as varchar)), ('flag', '0x'), ('instance', '1'), ('offset', cast(0 as varchar))]))
			        , ('dst_token'          , map_from_entries(array[('source', 'trace-input'   ), ('start', cast(4 + 8*32  as varchar)), ('flag', '0x'), ('instance', '1'), ('offset', cast(8*32 as varchar))]))
			        , ('src_amount'         , map_from_entries(array[('source', 'trace-input'   ), ('start', cast(4 + 3*32  as varchar)), ('flag', '0x'), ('instance', '1'), ('offset', cast(0 as varchar))]))
			        , ('dst_result_amount'  , map_from_entries(array[('source', 'trace-output'  ), ('start', cast(0*32  as varchar)), ('flag', '0x'), ('instance', '1'), ('offset', cast(0 as varchar))]))
			        , ('min_return_amount'  , map_from_entries(array[('source', 'trace-input'   ), ('start', cast(4 + 1*32  as varchar)), ('flag', '0x'), ('instance', '1'), ('offset', cast(0 as varchar))]))
			    ]), array[0x5eff])
		    
		    -- multiplexMultiHopSellEthForToken // ZeroEx AR // ethereum example: 0x1040ff5da994b2067b0b12f58077d0f298ae43c1a0bff09b1124a97f2a276563
            , (0x5161b966, 'AR', 0x00, 'multiplexMultiHopSellEthForToken'
			    , map_from_entries(array[
			          ('wallet'             , map_from_entries(array[('source', 'tx-from'       ), ('start', cast(0     as varchar)), ('flag', '0x'), ('instance', '1'), ('offset', cast(0 as varchar))]))
			        , ('src_token'          , map_from_entries(array[('source', 'trace-value'   ), ('start', cast(0     as varchar)), ('flag', '0x'), ('instance', '1'), ('offset', cast(0 as varchar))]))
			        , ('dst_token'          , map_from_entries(array[('source', 'trace-input'   ), ('start', cast(4 + 3*32  as varchar)), ('flag', '0x'), ('instance', '1'), ('offset', cast(3*32 as varchar))]))
			        , ('src_amount'         , map_from_entries(array[('source', 'trace-value'   ), ('start', cast(0     as varchar)), ('flag', '0x'), ('instance', '1'), ('offset', cast(0 as varchar))]))
			        , ('dst_result_amount'  , map_from_entries(array[('source', 'trace-output'  ), ('start', cast(0*32  as varchar)), ('flag', '0x'), ('instance', '1'), ('offset', cast(0 as varchar))]))
			        , ('min_return_amount'  , map_from_entries(array[('source', 'trace-input'   ), ('start', cast(4 + 2*32  as varchar)), ('flag', '0x'), ('instance', '1'), ('offset', cast(0 as varchar))]))
			    ]), array[0x5eff])
		    
		    -- multiplexMultiHopSellTokenForEth // ZeroEx AR // ethereum example: 0x8e50c7fe71b3b57f17074d8100907f0977f23201f20811ca3e990fc9814259e1
            , (0x9a2967d2, 'AR', 0x00, 'multiplexMultiHopSellTokenForEth'
			    , map_from_entries(array[
			          ('wallet'             , map_from_entries(array[('source', 'tx-from'       ), ('start', cast(0         as varchar)), ('flag', '0x'), ('instance', '1'), ('offset', cast(0 as varchar))]))
			        , ('src_token'          , map_from_entries(array[('source', 'trace-input'   ), ('start', cast(4 + 5*32 + 12 as varchar)), ('flag', '0x'), ('instance', '1'), ('offset', cast(0 as varchar))]))
			        , ('dst_token'          , map_from_entries(array[('source', 'trace-value'   ), ('start', cast(0         as varchar)), ('flag', '0x'), ('instance', '1'), ('offset', cast(0 as varchar))]))
			        , ('src_amount'         , map_from_entries(array[('source', 'trace-input'   ), ('start', cast(4 + 2*32      as varchar)), ('flag', '0x'), ('instance', '1'), ('offset', cast(0 as varchar))]))
			        , ('dst_result_amount'  , map_from_entries(array[('source', 'trace-output'  ), ('start', cast(0*32      as varchar)), ('flag', '0x'), ('instance', '1'), ('offset', cast(0 as varchar))]))
			        , ('min_return_amount'  , map_from_entries(array[('source', 'trace-input'   ), ('start', cast(4 + 3*32      as varchar)), ('flag', '0x'), ('instance', '1'), ('offset', cast(0 as varchar))]))
			    ]), array[0x5eff])
		    
		    -- multiplexBatchSellEthForToken // ZeroEx AR // ethereum example: 0x9c8e98ec20952d91ea2c448a15ce99dabaef170be12b1013468cfd43ffb1ddb3
            , (0xf35b4733, 'AR', 0x00, 'multiplexBatchSellEthForToken'
			    , map_from_entries(array[
			          ('wallet'             , map_from_entries(array[('source', 'tx-from'       ), ('start', cast(0    as varchar)), ('flag', '0x'), ('instance', '1'), ('offset', cast(0 as varchar))]))
			        , ('src_token'          , map_from_entries(array[('source', 'trace-value'   ), ('start', cast(0 as varchar)), ('flag', '0x'), ('instance', '1'), ('offset', cast(0 as varchar))]))
			        , ('dst_token'          , map_from_entries(array[('source', 'trace-input'   ), ('start', cast(4 + 0*32 + 12 as varchar)), ('flag', '0x'), ('instance', '1'), ('offset', cast(0 as varchar))]))
			        , ('src_amount'         , map_from_entries(array[('source', 'trace-value'   ), ('start', cast(0 as varchar)), ('flag', '0x'), ('instance', '1'), ('offset', cast(0 as varchar))]))
			        , ('dst_result_amount'  , map_from_entries(array[('source', 'trace-output'  ), ('start', cast(0*32 as varchar)), ('flag', '0x'), ('instance', '1'), ('offset', cast(0 as varchar))]))
			        , ('min_return_amount'  , map_from_entries(array[('source', 'trace-input'   ), ('start', cast(4 + 2*32 as varchar)), ('flag', '0x'), ('instance', '1'), ('offset', cast(0 as varchar))]))
			    ]), array[0x5eff])
		    
		    -- multiplexBatchSellTokenForToken // ZeroEx AR // ethereum example: 0x093b76f7b1bf171a546a30d3abd87a71f309bf0980ae3313f3b7300edf222fb5
            , (0x7a1eb1b9, 'AR', 0x00, 'multiplexBatchSellTokenForToken'
			    , map_from_entries(array[
			          ('wallet'             , map_from_entries(array[('source', 'tx-from'       ), ('start', cast(0    as varchar)), ('flag', '0x'), ('instance', '1'), ('offset', cast(0 as varchar))]))
			        , ('src_token'          , map_from_entries(array[('source', 'trace-input'   ), ('start', cast(4 + 0*32 + 12 as varchar)), ('flag', '0x'), ('instance', '1'), ('offset', cast(0 as varchar))]))
			        , ('dst_token'          , map_from_entries(array[('source', 'trace-input'   ), ('start', cast(4 + 1*32 + 12 as varchar)), ('flag', '0x'), ('instance', '1'), ('offset', cast(0 as varchar))]))
			        , ('src_amount'         , map_from_entries(array[('source', 'trace-input'   ), ('start', cast(4 + 3*32 + 12 as varchar)), ('flag', '0x'), ('instance', '1'), ('offset', cast(0 as varchar))]))
			        , ('dst_result_amount'  , map_from_entries(array[('source', 'trace-output'  ), ('start', cast(0*32 as varchar)), ('flag', '0x'), ('instance', '1'), ('offset', cast(0 as varchar))]))
			        , ('min_return_amount'  , map_from_entries(array[('source', 'trace-input'   ), ('start', cast(4 + 4*32 as varchar)), ('flag', '0x'), ('instance', '1'), ('offset', cast(0 as varchar))]))
			    ]), array[0x5eff])
			
		    -- multiplexBatchSellTokenForEth // ZeroEx AR // ethereum example: 0x690f5deeab7b9283911bffa1aab823d77ffd65035f0ff6d9c406efc9e3380ecf
            , (0x77725df6, 'AR', 0x00, 'multiplexBatchSellTokenForEth'
			    , map_from_entries(array[
			          ('wallet'             , map_from_entries(array[('source', 'tx-from'       ), ('start', cast(0    as varchar)), ('flag', '0x'), ('instance', '1'), ('offset', cast(0 as varchar))]))
			        , ('src_token'          , map_from_entries(array[('source', 'trace-input'   ), ('start', cast(4 + 0*32 + 12 as varchar)), ('flag', '0x'), ('instance', '1'), ('offset', cast(0 as varchar))]))
			        , ('dst_token'          , map_from_entries(array[('source', 'trace-value'   ), ('start', cast(0 as varchar)), ('flag', '0x'), ('instance', '1'), ('offset', cast(0 as varchar))]))
			        , ('src_amount'         , map_from_entries(array[('source', 'trace-input'   ), ('start', cast(4 + 2*32 + 12 as varchar)), ('flag', '0x'), ('instance', '1'), ('offset', cast(0 as varchar))]))
			        , ('dst_result_amount'  , map_from_entries(array[('source', 'trace-output'  ), ('start', cast(0*32 as varchar)), ('flag', '0x'), ('instance', '1'), ('offset', cast(0 as varchar))]))
			        , ('min_return_amount'  , map_from_entries(array[('source', 'trace-input'   ), ('start', cast(4 + 3*32 as varchar)), ('flag', '0x'), ('instance', '1'), ('offset', cast(0 as varchar))]))
			    ]), array[0x5eff])
			
        ) as s(selector, protocol, topic0, method, params, contracts), unnest(contracts) as contracts(contract_id)
    )

select
    
    -- contract
      project
    , contract_address
    , contract_id
    , contract_name
    , blockchain
    , created
    , creator
    
    -- methods
    , protocol
    , selector
    , coalesce(split_part(cs.signature, '(', 1), method) as method
    , topic0
    , split_part(ls.signature, '(', 1) as event
    , params as method_params
    , cs.signature as call_signature
    , ls.signature as log_signature

from {{ ref('oneinch_exchange_contracts') }}
join methods using(contract_id)
left join {{ source('abi', 'signatures') }} as cs on cs.id = methods.selector
left join {{ source('abi', 'signatures') }} as ls on ls.id = methods.topic0
order by created, method