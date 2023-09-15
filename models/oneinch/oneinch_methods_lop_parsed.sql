{{
    config(
        schema = 'oneinch',
        alias = alias('methods_lop_parsed'),
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
                
            -- 0xb9ed0243fdf00f0545c63a0af8850c090d86bb46682baec4bf3c496814fe4f02 - log OrderFilled
            -- 0xc3b639f02b125bfa160e50739b8c44eb2d1b6908e2b6d5925c6d770f2ca78127 - log OrderFilledRFQ
            
            -- fillOrder // 1inch LOP V1 // ethereum example: 0xfc8044bca3935fb730c0eca17d7f5d628eb5ed81310c71a634a2e5a858522dec, 0x93f9b8be3fab69d070f97a0059df673fd9e8d50585b2fecf1b19e2e225fa64b4
			  (0xf3432b1a, 'LOP', 0xb9ed0243fdf00f0545c63a0af8850c090d86bb46682baec4bf3c496814fe4f02, 'fillOrder'
			    , map_from_entries(array[
			          ('rfq'                , map_from_entries(array[('source', 'not'           ), ('start', null                                 ), ('flag', null), ('instance', '1')]))
			        , ('maker'              , map_from_entries(array[('source', 'trace-input'   ), ('start', cast(4 + 0*32 + 12        as varchar)), ('flag', '0x23b872dd'), ('instance', '1')]))
			        , ('src_token'          , map_from_entries(array[('source', 'trace-input'   ), ('start', cast(4 + 6*32 + 12        as varchar)), ('flag', '0x'), ('instance', '1')]))
			        , ('dst_token'          , map_from_entries(array[('source', 'trace-input'   ), ('start', cast(4 + 7*32 + 12        as varchar)), ('flag', '0x'), ('instance', '1')]))
			        , ('src_max_amount'     , map_from_entries(array[('source', 'trace-input'   ), ('start', cast(4 + 2*32             as varchar)), ('flag', '0x23b872dd'), ('instance', '1')]))
			        , ('dst_max_amount'     , map_from_entries(array[('source', 'trace-input'   ), ('start', cast(4 + 2*32             as varchar)), ('flag', '0x23b872dd'), ('instance', '2')]))
			        , ('src_fill_amount'    , map_from_entries(array[('source', 'trace-output'  ), ('start', cast(0*32                 as varchar)), ('flag', '0x'), ('instance', '1')]))
			        , ('dst_fill_amount'    , map_from_entries(array[('source', 'trace-output'  ), ('start', cast(1*32                 as varchar)), ('flag', '0x'), ('instance', '1')]))
			        , ('order_hash'         , map_from_entries(array[('source', 'log-data'      ), ('start', cast(0*32                 as varchar)), ('flag', '0x'), ('instance', '1')]))
			    ]), array[0xe782, 0x84eb, 0x6857, 0x8afe])
			
			-- fillOrder // 1inch LOP V2 // ethereum example: 0xf24cce21d0da165e7688201d1c322038204fd01559e1b6fa419c9a59c54a054e, 0x27c321f810c0faf4fbe10dff957a0f6d0367b6ffdc1e854d91d87369a7bc577f
			, (0x655d13cd, 'LOP', 0xb9ed0243fdf00f0545c63a0af8850c090d86bb46682baec4bf3c496814fe4f02, 'fillOrder'
			    , map_from_entries(array[
			          ('rfq'                , map_from_entries(array[('source', 'not'           ), ('start', null                                 ), ('flag', null), ('instance', '1')]))
			        , ('maker'              , map_from_entries(array[('source', 'trace-input'   ), ('start', cast(4 + 8*32 + 12        as varchar)), ('flag', '0x'), ('instance', '1')]))
			        , ('src_token'          , map_from_entries(array[('source', 'trace-input'   ), ('start', cast(4 + 6*32 + 12        as varchar)), ('flag', '0x'), ('instance', '1')]))
			        , ('dst_token'          , map_from_entries(array[('source', 'trace-input'   ), ('start', cast(4 + 7*32 + 12        as varchar)), ('flag', '0x'), ('instance', '1')]))
			        , ('src_max_amount'     , map_from_entries(array[('source', 'trace-input'   ), ('start', cast(4 + 11*32            as varchar)), ('flag', '0x'), ('instance', '1')])) -- 4 + 4*32
			        , ('dst_max_amount'     , map_from_entries(array[('source', 'trace-input'   ), ('start', cast(4 + 12*32            as varchar)), ('flag', '0x'), ('instance', '1')])) -- 4 + 3*32
			        , ('src_fill_amount'    , map_from_entries(array[('source', 'trace-output'  ), ('start', cast(0*32                 as varchar)), ('flag', '0x'), ('instance', '1')]))
			        , ('dst_fill_amount'    , map_from_entries(array[('source', 'trace-output'  ), ('start', cast(1*32                 as varchar)), ('flag', '0x'), ('instance', '1')]))
			        , ('order_hash'         , map_from_entries(array[('source', 'log-data'      ), ('start', cast(0*32                 as varchar)), ('flag', '0x'), ('instance', '1')]))
			    ]), array[0x2828, 0x6362, 0x734f, 0x690f, 0xf2b9, 0xb594, 0x7187])
			
			-- fillOrder // 1inch LOP V3 // ethereum example: 0xd6f31e37e9a2bdf79f9e08a3a1aff7aa27b6f33fad8e34506930058f944944eb, 0x0eb55a98c981973619e86beac7786ab8323eb5c5cf93e8fe03af02b3f51c67f9
			, (0x62e238bb, 'LOP', 0xb9ed0243fdf00f0545c63a0af8850c090d86bb46682baec4bf3c496814fe4f02, 'fillOrder'
			    , map_from_entries(array[
			          ('rfq'                , map_from_entries(array[('source', 'not'           ), ('start', null                                 ), ('flag', null), ('instance', '1')]))
			        , ('maker'              , map_from_entries(array[('source', 'trace-input'   ), ('start', cast(4 + 9*32 + 12        as varchar)), ('flag', '0x'), ('instance', '1')]))
			        , ('src_token'          , map_from_entries(array[('source', 'trace-input'   ), ('start', cast(4 + 7*32 + 12        as varchar)), ('flag', '0x'), ('instance', '1')]))
			        , ('dst_token'          , map_from_entries(array[('source', 'trace-input'   ), ('start', cast(4 + 8*32 + 12        as varchar)), ('flag', '0x'), ('instance', '1')]))
			        , ('src_max_amount'     , map_from_entries(array[('source', 'trace-input'   ), ('start', cast(4 + 12*32            as varchar)), ('flag', '0x'), ('instance', '1')]))
			        , ('dst_max_amount'     , map_from_entries(array[('source', 'trace-input'   ), ('start', cast(4 + 13*32            as varchar)), ('flag', '0x'), ('instance', '1')]))
			        , ('src_fill_amount'    , map_from_entries(array[('source', 'trace-output'  ), ('start', cast(0*32                 as varchar)), ('flag', '0x'), ('instance', '1')]))
			        , ('dst_fill_amount'    , map_from_entries(array[('source', 'trace-output'  ), ('start', cast(1*32                 as varchar)), ('flag', '0x'), ('instance', '1')]))
			        , ('order_hash'         , map_from_entries(array[('source', 'trace-output'  ), ('start', cast(2*32                 as varchar)), ('flag', '0x'), ('instance', '1')]))
			    ]), array[0x0582])
			
			--  fillOrderTo // 1inch LOP V2 // ethereum example: 0x72e2bd374bad0f3ba7844edb0ddabcd3bf2b0b04c082e9eafc2e1e705d6d111a, 0x15ff50eff62f5eb8431d56180ffdedc4dfa99a4b6f2466171b8219cfce3031ec
			, (0xb2610fe3, 'LOP', 0xb9ed0243fdf00f0545c63a0af8850c090d86bb46682baec4bf3c496814fe4f02, 'fillOrderTo'
			    , map_from_entries(array[
			          ('rfq'                , map_from_entries(array[('source', 'not'           ), ('start', null                                 ), ('flag', null), ('instance', '1')]))
			        , ('maker'              , map_from_entries(array[('source', 'trace-input'   ), ('start', cast(4 + 9*32 + 12        as varchar)), ('flag', '0x'), ('instance', '1')]))
			        , ('src_token'          , map_from_entries(array[('source', 'trace-input'   ), ('start', cast(4 + 7*32 + 12        as varchar)), ('flag', '0x'), ('instance', '1')]))
			        , ('dst_token'          , map_from_entries(array[('source', 'trace-input'   ), ('start', cast(4 + 8*32 + 12        as varchar)), ('flag', '0x'), ('instance', '1')]))
			        , ('src_max_amount'     , map_from_entries(array[('source', 'trace-input'   ), ('start', cast(4 + 12*32            as varchar)), ('flag', '0x'), ('instance', '1')]))
			        , ('dst_max_amount'     , map_from_entries(array[('source', 'trace-input'   ), ('start', cast(4 + 13*32            as varchar)), ('flag', '0x'), ('instance', '1')]))
			        , ('src_fill_amount'    , map_from_entries(array[('source', 'trace-output'  ), ('start', cast(0*32                 as varchar)), ('flag', '0x'), ('instance', '1')]))
			        , ('dst_fill_amount'    , map_from_entries(array[('source', 'trace-output'  ), ('start', cast(1*32                 as varchar)), ('flag', '0x'), ('instance', '1')]))
			        , ('order_hash'         , map_from_entries(array[('source', 'log-data'      ), ('start', cast(0*32                 as varchar)), ('flag', '0x'), ('instance', '1')]))
			    ]), array[0x2828, 0x6362, 0x734f, 0x690f, 0xf2b9, 0xb594, 0x7187])
			
			-- fillOrderTo // 1inch LOP V3 // ethereum example: 0x648fb51370939e256884417098eab8ed255ef56349110b7a4c9c3766d72cc871, 0x786f5be9094f92732f2f8b9554d6619d3242c39377b66975880a540a0ab51eee
			, (0xe5d7bde6, 'LOP', 0xb9ed0243fdf00f0545c63a0af8850c090d86bb46682baec4bf3c496814fe4f02, 'fillOrderTo'
			    , map_from_entries(array[
			          ('rfq'                , map_from_entries(array[('source', 'not'           ), ('start', null                                 ), ('flag', null), ('instance', '1')]))
			        , ('maker'              , map_from_entries(array[('source', 'trace-input'   ), ('start', cast(4 + 10*32 + 12       as varchar)), ('flag', '0x'), ('instance', '1')]))
			        , ('src_token'          , map_from_entries(array[('source', 'trace-input'   ), ('start', cast(4 + 8*32 + 12        as varchar)), ('flag', '0x'), ('instance', '1')]))
			        , ('dst_token'          , map_from_entries(array[('source', 'trace-input'   ), ('start', cast(4 + 9*32 + 12        as varchar)), ('flag', '0x'), ('instance', '1')]))
			        , ('src_max_amount'     , map_from_entries(array[('source', 'trace-input'   ), ('start', cast(4 + 13*32            as varchar)), ('flag', '0x'), ('instance', '1')]))
			        , ('dst_max_amount'     , map_from_entries(array[('source', 'trace-input'   ), ('start', cast(4 + 14*32            as varchar)), ('flag', '0x'), ('instance', '1')]))
			        , ('src_fill_amount'    , map_from_entries(array[('source', 'trace-output'  ), ('start', cast(0*32                 as varchar)), ('flag', '0x'), ('instance', '1')]))
			        , ('dst_fill_amount'    , map_from_entries(array[('source', 'trace-output'  ), ('start', cast(1*32                 as varchar)), ('flag', '0x'), ('instance', '1')]))
			        , ('order_hash'         , map_from_entries(array[('source', 'trace-output'  ), ('start', cast(2*32                 as varchar)), ('flag', '0x'), ('instance', '1')]))
			    ]), array[0x0582])
            
            -- fillOrderToWithPermit // 1inch LOP V2 // ethereum example: 0xfab8f2246070d10a8bc7155f286477e15ba576c16f212dcf6e616b3bd97c58dc, 0x3948591a792275c6edee5e809195e59fa0be395a5c9fbef70f5129fcef079c86
            , (0x6073cc20, 'LOP', 0xb9ed0243fdf00f0545c63a0af8850c090d86bb46682baec4bf3c496814fe4f02, 'fillOrderToWithPermit'
                , map_from_entries(array[
                      ('rfq'                , map_from_entries(array[('source', 'not'           ), ('start', null                                 ), ('flag', null), ('instance', '1')]))
                    , ('maker'              , map_from_entries(array[('source', 'trace-input'   ), ('start', cast(4 + 10*32 + 12       as varchar)), ('flag', '0x'), ('instance', '1')]))
                    , ('src_token'          , map_from_entries(array[('source', 'trace-input'   ), ('start', cast(4 + 8*32 + 12        as varchar)), ('flag', '0x'), ('instance', '1')]))
                    , ('dst_token'          , map_from_entries(array[('source', 'trace-input'   ), ('start', cast(4 + 9*32 + 12        as varchar)), ('flag', '0x'), ('instance', '1')]))
                    , ('src_max_amount'     , map_from_entries(array[('source', 'trace-input'   ), ('start', cast(4 + 13*32            as varchar)), ('flag', '0x'), ('instance', '1')])) -- 4 + 4*32
                    , ('dst_max_amount'     , map_from_entries(array[('source', 'trace-input'   ), ('start', cast(4 + 14*32            as varchar)), ('flag', '0x'), ('instance', '1')])) -- 4 + 3*32
                    , ('src_fill_amount'    , map_from_entries(array[('source', 'trace-output'  ), ('start', cast(0*32                 as varchar)), ('flag', '0x'), ('instance', '1')]))
                    , ('dst_fill_amount'    , map_from_entries(array[('source', 'trace-output'  ), ('start', cast(1*32                 as varchar)), ('flag', '0x'), ('instance', '1')]))
                    , ('order_hash'         , map_from_entries(array[('source', 'log-data'      ), ('start', cast(0*32                 as varchar)), ('flag', '0x'), ('instance', '1')]))
                ]), array[0x2828, 0x6362, 0x734f, 0x690f, 0xf2b9, 0xb594, 0x7187])
            
            -- fillOrderToWithPermit // 1inch LOP V3 // ethereum example: 0xee7bc9c7006c6ff4f047c5f08faa2ffcd8ce05f8960953187568827763a4e7da
            , (0xd365c695, 'LOP', 0xb9ed0243fdf00f0545c63a0af8850c090d86bb46682baec4bf3c496814fe4f02, 'fillOrderToWithPermit'
                , map_from_entries(array[
                      ('rfq'                , map_from_entries(array[('source', 'not'           ), ('start', null                                 ), ('flag', null), ('instance', '1')]))
                    , ('maker'              , map_from_entries(array[('source', 'trace-input'   ), ('start', cast(4 + 11*32 + 12       as varchar)), ('flag', '0x'), ('instance', '1')]))
                    , ('src_token'          , map_from_entries(array[('source', 'trace-input'   ), ('start', cast(4 + 9*32 + 12        as varchar)), ('flag', '0x'), ('instance', '1')]))
                    , ('dst_token'          , map_from_entries(array[('source', 'trace-input'   ), ('start', cast(4 + 10*32 + 12       as varchar)), ('flag', '0x'), ('instance', '1')]))
                    , ('src_max_amount'     , map_from_entries(array[('source', 'trace-input'   ), ('start', cast(4 + 14*32            as varchar)), ('flag', '0x'), ('instance', '1')]))
                    , ('dst_max_amount'     , map_from_entries(array[('source', 'trace-input'   ), ('start', cast(4 + 15*32            as varchar)), ('flag', '0x'), ('instance', '1')]))
                    , ('src_fill_amount'    , map_from_entries(array[('source', 'trace-output'  ), ('start', cast(0*32                 as varchar)), ('flag', '0x'), ('instance', '1')]))
                    , ('dst_fill_amount'    , map_from_entries(array[('source', 'trace-output'  ), ('start', cast(1*32                 as varchar)), ('flag', '0x'), ('instance', '1')]))
                    , ('order_hash'         , map_from_entries(array[('source', 'trace-output'  ), ('start', cast(2*32                 as varchar)), ('flag', '0x'), ('instance', '1')]))
                ]), array[0x0582])
			
			-- fillOrderRFQ // 1inch LOP V1 // ethereum example: 0x17eedf52cd16e9a2a6cf7dcd0ea60f07b25d9b972564542b702cd86301270bc4, 0x2daa9ebe548c6db946529981a9fcb7364589588648a425c2cdc20750d1c5706d
			, (0x74785238, 'LOP', 0xc3b639f02b125bfa160e50739b8c44eb2d1b6908e2b6d5925c6d770f2ca78127, 'fillOrderRFQ'
			    , map_from_entries(array[
			          ('rfq'                , map_from_entries(array[('source', 'yes'           ), ('start', null                                 ), ('flag', null), ('instance', '1')]))
			        , ('maker'              , map_from_entries(array[('source', 'trace-input'   ), ('start', cast(4 + 12               as varchar)), ('flag', '0x23b872dd'), ('instance', '1')]))
			        , ('src_token'          , map_from_entries(array[('source', 'trace-input'   ), ('start', cast(4 + 5*32 + 12        as varchar)), ('flag', '0x'), ('instance', '1')]))
			        , ('dst_token'          , map_from_entries(array[('source', 'trace-input'   ), ('start', cast(4 + 6*32 + 12        as varchar)), ('flag', '0x'), ('instance', '1')]))
			        , ('src_max_amount'     , map_from_entries(array[('source', 'trace-input'   ), ('start', cast(4 + 2*32             as varchar)), ('flag', '0x23b872dd'), ('instance', '1')]))
			        , ('dst_max_amount'     , map_from_entries(array[('source', 'trace-input'   ), ('start', cast(4 + 2*32             as varchar)), ('flag', '0x23b872dd'), ('instance', '2')]))
			        , ('src_fill_amount'    , map_from_entries(array[('source', 'log-data'      ), ('start', cast(1*32                 as varchar)), ('flag', '0x'), ('instance', '1')]))
			        , ('dst_fill_amount'    , map_from_entries(array[('source', 'trace-input'   ), ('start', cast(4 + 3*32             as varchar)), ('flag', '0x'), ('instance', '1')]))
			        , ('order_hash'         , map_from_entries(array[('source', 'log-data'      ), ('start', cast(0*32                 as varchar)), ('flag', '0x'), ('instance', '1')]))
			    ]), array[0xe782, 0x84eb, 0x6857, 0x8afe])
			
			-- fillOrderRFQ // 1inch LOP V2 // ethereum example: 0x164ffbffad7b41dd7e2bf2224c26d9302c387ce9f9670f9c399df6ef1e41228b,  0x180ec82f635e77723d2129e4cbe6b3b8da299b60fd2fbebf8b5c13062bffb20f    
			, (0xd0a3b665, 'LOP', 0xc3b639f02b125bfa160e50739b8c44eb2d1b6908e2b6d5925c6d770f2ca78127, 'fillOrderRFQ'
				, map_from_entries(array[
					    ('rfq'                , map_from_entries(array[('source', 'yes'           ), ('start', null                           ), ('flag', null), ('instance', '1')]))
					  , ('maker'              , map_from_entries(array[('source', 'trace-input'   ), ('start', cast(4 + 3*32 + 12  as varchar)), ('flag', '0x'), ('instance', '1')]))
					  , ('src_token'          , map_from_entries(array[('source', 'trace-input'   ), ('start', cast(4 + 1*32 + 12  as varchar)), ('flag', '0x'), ('instance', '1')]))
					  , ('dst_token'          , map_from_entries(array[('source', 'trace-input'   ), ('start', cast(4 + 2*32 + 12  as varchar)), ('flag', '0x'), ('instance', '1')]))
					  , ('src_max_amount'     , map_from_entries(array[('source', 'trace-input'   ), ('start', cast(4 + 5*32       as varchar)), ('flag', '0x'), ('instance', '1')]))
					  , ('dst_max_amount'     , map_from_entries(array[('source', 'trace-input'   ), ('start', cast(4 + 6*32       as varchar)), ('flag', '0x'), ('instance', '1')]))
					  , ('src_fill_amount'    , map_from_entries(array[('source', 'trace-input'   ), ('start', cast(4 + 8*32       as varchar)), ('flag', '0x'), ('instance', '1')]))
					  , ('dst_fill_amount'    , map_from_entries(array[('source', 'trace-input'   ), ('start', cast(4 + 9*32       as varchar)), ('flag', '0x'), ('instance', '1')]))
					  , ('order_hash'         , map_from_entries(array[('source', 'log-data'      ), ('start', cast(0*32           as varchar)), ('flag', '0x'), ('instance', '1')]))
				]), array[0x2828, 0x6362, 0x734f, 0x690f, 0xf2b9, 0xb594, 0x7187, 0x097d, 0xd199])
			
			-- fillOrderRFQToWithPermit // 1inch LOP V2 // ethereum example: 0x3053e54c2707cecad9e90f7df41351ed4a7dae430eed10696f32a473cd83bc44, 0x685a6e2ce2c551cf7ce7598f2b132ff813ee901a23a3901c6c60a66a3c975acb    
			, (0x4cc4a27b, 'LOP', 0xc3b639f02b125bfa160e50739b8c44eb2d1b6908e2b6d5925c6d770f2ca78127, 'fillOrderRFQToWithPermit'
				, map_from_entries(array[
					    ('rfq'                , map_from_entries(array[('source', 'yes'           ), ('start', null                           ), ('flag', null), ('instance', '1')]))
					  , ('maker'              , map_from_entries(array[('source', 'trace-input'   ), ('start', cast(4 + 3*32 + 12  as varchar)), ('flag', '0x'), ('instance', '1')]))
					  , ('src_token'          , map_from_entries(array[('source', 'trace-input'   ), ('start', cast(4 + 1*32 + 12  as varchar)), ('flag', '0x'), ('instance', '1')]))
					  , ('dst_token'          , map_from_entries(array[('source', 'trace-input'   ), ('start', cast(4 + 2*32 + 12  as varchar)), ('flag', '0x'), ('instance', '1')]))
					  , ('src_max_amount'     , map_from_entries(array[('source', 'trace-input'   ), ('start', cast(4 + 5*32       as varchar)), ('flag', '0x'), ('instance', '1')]))
					  , ('dst_max_amount'     , map_from_entries(array[('source', 'trace-input'   ), ('start', cast(4 + 6*32       as varchar)), ('flag', '0x'), ('instance', '1')]))
					  , ('src_fill_amount'    , map_from_entries(array[('source', 'trace-input'   ), ('start', cast(4 + 8*32       as varchar)), ('flag', '0x'), ('instance', '1')]))
					  , ('dst_fill_amount'    , map_from_entries(array[('source', 'trace-input'   ), ('start', cast(4 + 9*32       as varchar)), ('flag', '0x'), ('instance', '1')]))
					  , ('order_hash'         , map_from_entries(array[('source', 'log-data'      ), ('start', cast(0*32           as varchar)), ('flag', '0x'), ('instance', '1')]))
				]), array[0x2828, 0x6362, 0x734f, 0x690f, 0xf2b9, 0xb594, 0x7187, 0x097d, 0xd199])
			
			-- fillOrderRFQTo // 1inch LOP V2 // ethereum example: 0xe47b175adc0c662f09998f8f5987044144552d13310168e9844bf596bbd346c0, 0xceeaa8f5a17bf1be3b6d120b8b5f9c77626bfcb1eb7f495af89659c557be8f31
			, (0xbaba5855, 'LOP', 0xc3b639f02b125bfa160e50739b8c44eb2d1b6908e2b6d5925c6d770f2ca78127, 'fillOrderRFQTo'
				, map_from_entries(array[
					    ('rfq'                , map_from_entries(array[('source', 'yes'           ), ('start', null                           ), ('flag', null), ('instance', '1')]))
					  , ('maker'              , map_from_entries(array[('source', 'trace-input'   ), ('start', cast(4 + 3*32 + 12  as varchar)), ('flag', '0x'), ('instance', '1')]))
					  , ('src_token'          , map_from_entries(array[('source', 'trace-input'   ), ('start', cast(4 + 1*32 + 12  as varchar)), ('flag', '0x'), ('instance', '1')]))
					  , ('dst_token'          , map_from_entries(array[('source', 'trace-input'   ), ('start', cast(4 + 2*32 + 12  as varchar)), ('flag', '0x'), ('instance', '1')]))
					  , ('src_max_amount'     , map_from_entries(array[('source', 'trace-input'   ), ('start', cast(4 + 5*32       as varchar)), ('flag', '0x'), ('instance', '1')]))
					  , ('dst_max_amount'     , map_from_entries(array[('source', 'trace-input'   ), ('start', cast(4 + 6*32       as varchar)), ('flag', '0x'), ('instance', '1')]))
					  , ('src_fill_amount'    , map_from_entries(array[('source', 'trace-input'   ), ('start', cast(4 + 8*32       as varchar)), ('flag', '0x'), ('instance', '1')]))
					  , ('dst_fill_amount'    , map_from_entries(array[('source', 'trace-input'   ), ('start', cast(4 + 9*32       as varchar)), ('flag', '0x'), ('instance', '1')]))
					  , ('order_hash'         , map_from_entries(array[('source', 'log-data'      ), ('start', cast(0*32           as varchar)), ('flag', '0x'), ('instance', '1')]))
				]), array[0x2828, 0x6362, 0x734f, 0x690f, 0xf2b9, 0xb594, 0x7187, 0x097d, 0xd199])
			
			-- fillOrderRFQ // 1inch LOP V3 // ethereum example: 0x0f646e54860a48c0fd840d5f1387e4f6398e6035713868b48175f840de8bb072, 0x93cf31dfdf8bdc38fc27cb8523e3d45a3631899d3ecc369259b557c01365c505    
			, (0x3eca9c0a, 'LOP', 0xc3b639f02b125bfa160e50739b8c44eb2d1b6908e2b6d5925c6d770f2ca78127, 'fillOrderRFQ'
				, map_from_entries(array[
					    ('rfq'                , map_from_entries(array[('source', 'yes'           ), ('start', null                           ), ('flag', null), ('instance', '1')]))
					  , ('maker'              , map_from_entries(array[('source', 'trace-input'   ), ('start', cast(4 + 3*32 + 12  as varchar)), ('flag', '0x'), ('instance', '1')]))
					  , ('src_token'          , map_from_entries(array[('source', 'trace-input'   ), ('start', cast(4 + 1*32 + 12  as varchar)), ('flag', '0x'), ('instance', '1')]))
					  , ('dst_token'          , map_from_entries(array[('source', 'trace-input'   ), ('start', cast(4 + 2*32 + 12  as varchar)), ('flag', '0x'), ('instance', '1')]))
					  , ('src_max_amount'     , map_from_entries(array[('source', 'trace-input'   ), ('start', cast(4 + 5*32       as varchar)), ('flag', '0x'), ('instance', '1')]))
					  , ('dst_max_amount'     , map_from_entries(array[('source', 'trace-input'   ), ('start', cast(4 + 6*32       as varchar)), ('flag', '0x'), ('instance', '1')]))
					  , ('src_fill_amount'    , map_from_entries(array[('source', 'trace-output'  ), ('start', cast(0*32           as varchar)), ('flag', '0x'), ('instance', '1')]))
					  , ('dst_fill_amount'    , map_from_entries(array[('source', 'trace-output'  ), ('start', cast(1*32           as varchar)), ('flag', '0x'), ('instance', '1')]))
					  , ('order_hash'         , map_from_entries(array[('source', 'log-data'      ), ('start', cast(0*32           as varchar)), ('flag', '0x'), ('instance', '1')]))
				]), array[0x0582])
			
			-- fillOrderRFQCompact // 1inch LOP V3 // ethereum example: 0xbfc1cfa7d263e82001b77b6d5bc80561d69d5cd182516c17586cb8e7253ae3e0, 0x6dda1898d0a9027b6e30c064e3a14014bb6002809ee32ab8fb48610a3789a4ba    
			, (0x9570eeee, 'LOP', 0xc3b639f02b125bfa160e50739b8c44eb2d1b6908e2b6d5925c6d770f2ca78127, 'fillOrderRFQCompact'
				, map_from_entries(array[
					    ('rfq'                , map_from_entries(array[('source', 'yes'           ), ('start', null                           ), ('flag', null), ('instance', '1')]))
					  , ('maker'              , map_from_entries(array[('source', 'trace-input'   ), ('start', cast(4 + 3*32 + 12  as varchar)), ('flag', '0x'), ('instance', '1')]))
					  , ('src_token'          , map_from_entries(array[('source', 'trace-input'   ), ('start', cast(4 + 1*32 + 12  as varchar)), ('flag', '0x'), ('instance', '1')]))
					  , ('dst_token'          , map_from_entries(array[('source', 'trace-input'   ), ('start', cast(4 + 2*32 + 12  as varchar)), ('flag', '0x'), ('instance', '1')]))
					  , ('src_max_amount'     , map_from_entries(array[('source', 'trace-input'   ), ('start', cast(4 + 5*32       as varchar)), ('flag', '0x'), ('instance', '1')]))
					  , ('dst_max_amount'     , map_from_entries(array[('source', 'trace-input'   ), ('start', cast(4 + 6*32       as varchar)), ('flag', '0x'), ('instance', '1')]))
					  , ('src_fill_amount'    , map_from_entries(array[('source', 'trace-output'  ), ('start', cast(0*32           as varchar)), ('flag', '0x'), ('instance', '1')]))
					  , ('dst_fill_amount'    , map_from_entries(array[('source', 'trace-output'  ), ('start', cast(1*32           as varchar)), ('flag', '0x'), ('instance', '1')]))
					  , ('order_hash'         , map_from_entries(array[('source', 'log-data'      ), ('start', cast(0*32           as varchar)), ('flag', '0x'), ('instance', '1')]))
				]), array[0x0582])
			
			-- fillOrderRFQTo // 1inch LOP V3 // ethereum example: 0x30a429d1fcddd8cc9df8b304122d3aa84a718e10acb0d708f50a40e16c766971, 0xeb87c2a8df7a9daa879f5fb32270f8e63b29d8e5fbc543ad90f8c65837164c58    
			, (0x5a099843, 'LOP', 0xc3b639f02b125bfa160e50739b8c44eb2d1b6908e2b6d5925c6d770f2ca78127, 'fillOrderRFQTo'
				, map_from_entries(array[
					    ('rfq'                , map_from_entries(array[('source', 'yes'           ), ('start', null                           ), ('flag', null), ('instance', '1')]))
					  , ('maker'              , map_from_entries(array[('source', 'trace-input'   ), ('start', cast(4 + 3*32 + 12  as varchar)), ('flag', '0x'), ('instance', '1')]))
					  , ('src_token'          , map_from_entries(array[('source', 'trace-input'   ), ('start', cast(4 + 1*32 + 12  as varchar)), ('flag', '0x'), ('instance', '1')]))
					  , ('dst_token'          , map_from_entries(array[('source', 'trace-input'   ), ('start', cast(4 + 2*32 + 12  as varchar)), ('flag', '0x'), ('instance', '1')]))
					  , ('src_max_amount'     , map_from_entries(array[('source', 'trace-input'   ), ('start', cast(4 + 5*32       as varchar)), ('flag', '0x'), ('instance', '1')]))
					  , ('dst_max_amount'     , map_from_entries(array[('source', 'trace-input'   ), ('start', cast(4 + 6*32       as varchar)), ('flag', '0x'), ('instance', '1')]))
					  , ('src_fill_amount'    , map_from_entries(array[('source', 'trace-output'  ), ('start', cast(0*32           as varchar)), ('flag', '0x'), ('instance', '1')]))
					  , ('dst_fill_amount'    , map_from_entries(array[('source', 'trace-output'  ), ('start', cast(1*32           as varchar)), ('flag', '0x'), ('instance', '1')]))
					  , ('order_hash'         , map_from_entries(array[('source', 'log-data'      ), ('start', cast(0*32           as varchar)), ('flag', '0x'), ('instance', '1')]))
				]), array[0x0582])
			
			-- fillOrderRFQToWithPermit // 1inch LOP V3 // ethereum example: 0x072c23f6375281e8bd6fd1b51505825e3dac4df2b47e6e283febbc0b33db3cbc, 0xfe8b01230053d139316cc61bae94cda8c7615cc3e40ce135f483a285517d75f0    
			, (0x70ccbd31, 'LOP', 0xc3b639f02b125bfa160e50739b8c44eb2d1b6908e2b6d5925c6d770f2ca78127, 'fillOrderRFQToWithPermit'
				, map_from_entries(array[
					    ('rfq'                , map_from_entries(array[('source', 'yes'           ), ('start', null                           ), ('flag', null), ('instance', '1')]))
					  , ('maker'              , map_from_entries(array[('source', 'trace-input'   ), ('start', cast(4 + 3*32 + 12  as varchar)), ('flag', '0x'), ('instance', '1')]))
					  , ('src_token'          , map_from_entries(array[('source', 'trace-input'   ), ('start', cast(4 + 1*32 + 12  as varchar)), ('flag', '0x'), ('instance', '1')]))
					  , ('dst_token'          , map_from_entries(array[('source', 'trace-input'   ), ('start', cast(4 + 2*32 + 12  as varchar)), ('flag', '0x'), ('instance', '1')]))
					  , ('src_max_amount'     , map_from_entries(array[('source', 'trace-input'   ), ('start', cast(4 + 5*32       as varchar)), ('flag', '0x'), ('instance', '1')]))
					  , ('dst_max_amount'     , map_from_entries(array[('source', 'trace-input'   ), ('start', cast(4 + 6*32       as varchar)), ('flag', '0x'), ('instance', '1')]))
					  , ('src_fill_amount'    , map_from_entries(array[('source', 'trace-output'  ), ('start', cast(0*32           as varchar)), ('flag', '0x'), ('instance', '1')]))
					  , ('dst_fill_amount'    , map_from_entries(array[('source', 'trace-output'  ), ('start', cast(1*32           as varchar)), ('flag', '0x'), ('instance', '1')]))
					  , ('order_hash'         , map_from_entries(array[('source', 'log-data'      ), ('start', cast(0*32           as varchar)), ('flag', '0x'), ('instance', '1')]))
				]), array[0x0582])
			
			-- clipperSwap // 1inch AR V4 // ethereum example: 0xa76d6d8bd1f8db49f5348e92f067c9c68ab9adea1f9b1b18feac9dc37333bca9
            , (0xb0431182, 'AR', 0x4be05c8d54f5e056ab2cfa033e9f582057001268c3e28561bb999d35d2c8f2c8, 'clipperSwap'
                , map_from_entries(array[
                      ('rfq'                , map_from_entries(array[('source', 'yes'           ), ('start', null                           ), ('flag', null), ('instance', '1')]))
                    , ('maker'              , map_from_entries(array[('source', null            ), ('start', null                           ), ('flag', null), ('instance', '1')]))
                    , ('taker'              , map_from_entries(array[('source', 'log-data'      ), ('start', cast(2*32 + 12      as varchar)), ('flag', '0x'), ('instance', '1')]))
                    , ('src_token'          , map_from_entries(array[('source', 'log-data'      ), ('start', cast(0*32 + 12      as varchar)), ('flag', '0x'), ('instance', '1')]))
                    , ('dst_token'          , map_from_entries(array[('source', 'log-data'      ), ('start', cast(1*32 + 12      as varchar)), ('flag', '0x'), ('instance', '1')]))
                    , ('src_max_amount'     , map_from_entries(array[('source', 'trace-input'   ), ('start', cast(4 + 2*32       as varchar)), ('flag', '0x'), ('instance', '1')]))
                    , ('dst_max_amount'     , map_from_entries(array[('source', 'trace-input'   ), ('start', cast(4 + 3*32       as varchar)), ('flag', '0x'), ('instance', '1')]))
                    , ('src_fill_amount'    , map_from_entries(array[('source', 'log-data'      ), ('start', cast(3*32           as varchar)), ('flag', '0x'), ('instance', '1')]))
                    , ('dst_fill_amount'    , map_from_entries(array[('source', 'log-data'      ), ('start', cast(4*32           as varchar)), ('flag', '0x'), ('instance', '1')]))
                    , ('order_hash'         , map_from_entries(array[('source', 'log-data'      ), ('start', cast(5*32           as varchar)), ('flag', '0x'), ('instance', '1')]))
			    ]), array[0x097d, 0xd199])
            
            -- clipperSwapToWithPermit // 1inch AR V4 // ethereum example: 0x50c8c51010059fb99fa9b478635069318b249873af30b320181bf377e448c781
            , (0xd6a92a5d, 'AR', 0x4be05c8d54f5e056ab2cfa033e9f582057001268c3e28561bb999d35d2c8f2c8, 'clipperSwapToWithPermit'
                , map_from_entries(array[
                      ('rfq'                , map_from_entries(array[('source', 'yes'           ), ('start', null                           ), ('flag', null), ('instance', '1')]))
                    , ('maker'              , map_from_entries(array[('source', null            ), ('start', null                           ), ('flag', null), ('instance', '1')]))
                    , ('taker'              , map_from_entries(array[('source', 'log-data'      ), ('start', cast(2*32 + 12      as varchar)), ('flag', '0x'), ('instance', '1')]))
                    , ('src_token'          , map_from_entries(array[('source', 'log-data'      ), ('start', cast(0*32 + 12      as varchar)), ('flag', '0x'), ('instance', '1')]))
                    , ('dst_token'          , map_from_entries(array[('source', 'log-data'      ), ('start', cast(1*32 + 12      as varchar)), ('flag', '0x'), ('instance', '1')]))
                    , ('src_max_amount'     , map_from_entries(array[('source', 'trace-input'   ), ('start', cast(4 + 3*32       as varchar)), ('flag', '0x'), ('instance', '1')]))
                    , ('dst_max_amount'     , map_from_entries(array[('source', 'trace-input'   ), ('start', cast(4 + 4*32       as varchar)), ('flag', '0x'), ('instance', '1')]))
                    , ('src_fill_amount'    , map_from_entries(array[('source', 'log-data'      ), ('start', cast(3*32           as varchar)), ('flag', '0x'), ('instance', '1')]))
                    , ('dst_fill_amount'    , map_from_entries(array[('source', 'log-data'      ), ('start', cast(4*32           as varchar)), ('flag', '0x'), ('instance', '1')]))
                    , ('order_hash'         , map_from_entries(array[('source', 'log-data'      ), ('start', cast(5*32           as varchar)), ('flag', '0x'), ('instance', '1')]))
			    ]), array[0x097d, 0xd199])
			
			-- clipperSwap // 1inch AR V5 // ethereum example: 0x0b2050e36c02800cd0c3b838c57a385b7cf1ad03f78893d73f4263f9e41e50c1
            , (0x84bd6d29, 'AR', 0x4be05c8d54f5e056ab2cfa033e9f582057001268c3e28561bb999d35d2c8f2c8, 'clipperSwap'
                , map_from_entries(array[
                      ('rfq'                , map_from_entries(array[('source', 'yes'           ), ('start', null                           ), ('flag', null), ('instance', '1')]))
                    , ('maker'              , map_from_entries(array[('source', 'trace-input'   ), ('start', cast(4 + 0*32 + 12  as varchar)), ('flag', '0x'), ('instance', '1')]))
                    , ('taker'              , map_from_entries(array[('source', 'log-topic3'    ), ('start', cast(12             as varchar)), ('flag', '0x'), ('instance', '1')]))
                    , ('src_token'          , map_from_entries(array[('source', 'trace-input'   ), ('start', cast(4 + 1*32 + 12  as varchar)), ('flag', '0x'), ('instance', '1')]))
                    , ('dst_token'          , map_from_entries(array[('source', 'trace-input'   ), ('start', cast(4 + 2*32 + 12  as varchar)), ('flag', '0x'), ('instance', '1')]))
                    , ('src_max_amount'     , map_from_entries(array[('source', 'trace-input'   ), ('start', cast(4 + 3*32       as varchar)), ('flag', '0x'), ('instance', '1')]))
                    , ('dst_max_amount'     , map_from_entries(array[('source', 'trace-input'   ), ('start', cast(4 + 4*32       as varchar)), ('flag', '0x'), ('instance', '1')]))
                    , ('src_fill_amount'    , map_from_entries(array[('source', 'log-data'      ), ('start', cast(0*32           as varchar)), ('flag', '0x'), ('instance', '1')]))
                    , ('dst_fill_amount'    , map_from_entries(array[('source', 'log-data'      ), ('start', cast(1*32           as varchar)), ('flag', '0x'), ('instance', '1')]))
                    , ('order_hash'         , map_from_entries(array[('source', 'log-data'      ), ('start', cast(2*32           as varchar)), ('flag', '0x'), ('instance', '1')]))
			    ]), array[0x0582])
            
            -- clipperSwapTo // 1inch AR V5 // ethereum example: 0xfd6576e317c2dd6e94c3e982504913df65df3ebde8a7dbf5e4094e45dcdf9d53
            , (0x093d4fa5, 'AR', 0x4be05c8d54f5e056ab2cfa033e9f582057001268c3e28561bb999d35d2c8f2c8, 'clipperSwapTo'
                , map_from_entries(array[
			          ('rfq'                , map_from_entries(array[('source', 'yes'           ), ('start', null                           ), ('flag', null), ('instance', '1')]))
                    , ('maker'              , map_from_entries(array[('source', 'trace-input'   ), ('start', cast(4 + 0*32 + 12  as varchar)), ('flag', '0x'), ('instance', '1')]))
                    , ('taker'              , map_from_entries(array[('source', 'log-topic3'    ), ('start', cast(12             as varchar)), ('flag', '0x'), ('instance', '1')]))
                    , ('src_token'          , map_from_entries(array[('source', 'trace-input'   ), ('start', cast(4 + 2*32 + 12  as varchar)), ('flag', '0x'), ('instance', '1')]))
                    , ('dst_token'          , map_from_entries(array[('source', 'trace-input'   ), ('start', cast(4 + 3*32 + 12  as varchar)), ('flag', '0x'), ('instance', '1')]))
                    , ('src_max_amount'     , map_from_entries(array[('source', 'trace-input'   ), ('start', cast(4 + 4*32       as varchar)), ('flag', '0x'), ('instance', '1')]))
                    , ('dst_max_amount'     , map_from_entries(array[('source', 'trace-input'   ), ('start', cast(4 + 5*32       as varchar)), ('flag', '0x'), ('instance', '1')]))
                    , ('src_fill_amount'    , map_from_entries(array[('source', 'log-data'      ), ('start', cast(0*32           as varchar)), ('flag', '0x'), ('instance', '1')]))
                    , ('dst_fill_amount'    , map_from_entries(array[('source', 'log-data'      ), ('start', cast(1*32           as varchar)), ('flag', '0x'), ('instance', '1')]))
                    , ('order_hash'         , map_from_entries(array[('source', 'log-data'      ), ('start', cast(2*32           as varchar)), ('flag', '0x'), ('instance', '1')]))
			    ]), array[0x0582])
            
            -- clipperSwapToWithPermit // 1inch AR V5 // ethereum example: 0xc47778fe71d64950087a3b3c9df309aa85b0e360afd4fd3b72317baf8f3c0b1d
            , (0xc805a666, 'AR', 0x4be05c8d54f5e056ab2cfa033e9f582057001268c3e28561bb999d35d2c8f2c8, 'clipperSwapToWithPermit'
                , map_from_entries(array[
                      ('rfq'                , map_from_entries(array[('source', 'yes'           ), ('start', null                           ), ('flag', null), ('instance', '1')]))
                    , ('maker'              , map_from_entries(array[('source', 'trace-input'   ), ('start', cast(4 + 0*32 + 12  as varchar)), ('flag', '0x'), ('instance', '1')]))
                    , ('taker'              , map_from_entries(array[('source', 'log-topic3'    ), ('start', cast(12             as varchar)), ('flag', '0x'), ('instance', '1')]))
                    , ('src_token'          , map_from_entries(array[('source', 'trace-input'   ), ('start', cast(4 + 2*32 + 12  as varchar)), ('flag', '0x'), ('instance', '1')]))
                    , ('dst_token'          , map_from_entries(array[('source', 'trace-input'   ), ('start', cast(4 + 3*32 + 12  as varchar)), ('flag', '0x'), ('instance', '1')]))
                    , ('src_max_amount'     , map_from_entries(array[('source', 'trace-input'   ), ('start', cast(4 + 4*32       as varchar)), ('flag', '0x'), ('instance', '1')]))
                    , ('dst_max_amount'     , map_from_entries(array[('source', 'trace-input'   ), ('start', cast(4 + 5*32       as varchar)), ('flag', '0x'), ('instance', '1')]))
                    , ('src_fill_amount'    , map_from_entries(array[('source', 'log-data'      ), ('start', cast(0*32           as varchar)), ('flag', '0x'), ('instance', '1')]))
                    , ('dst_fill_amount'    , map_from_entries(array[('source', 'log-data'      ), ('start', cast(1*32           as varchar)), ('flag', '0x'), ('instance', '1')]))
                    , ('order_hash'         , map_from_entries(array[('source', 'log-data'      ), ('start', cast(2*32           as varchar)), ('flag', '0x'), ('instance', '1')]))
			    ]), array[0x0582])
    	    
			-- ######### ######### ######### ######### ######### ######### ######### ######### --
            -- ######### ######### ######### ######### ######### ######### ######### ######### --
            -- ######### ######### ######### ######### ######### ######### ######### ######### --
            -- ######### ######### ######### ######### ######### ######### ######### ######### --
            -- ######### ######### ######### ######### ######### ######### ######### ######### --
            -- ######### ######### ######### ######### ######### ######### ######### ######### --
            -- ######### ######### ######### ######### ######### ######### ######### ######### --
            -- ######### ######### ######### ######### ######### ######### ######### ######### --
			
            -- 0x0bcc4c97732e47d9946f229edb95f5b6323f601300e4690de719993f3c371129 - log Fill
            -- 0xab614d2b738543c0ea21f56347cf696a3a0c42a7cbec3212a5ca22a4dcff2124 - log LimitOrderFilled
            -- 0xac75f773e3a92f1a02b12134d65e1f47f8a14eabe4eaf1e24624918e6a8b269f - log OtcOrderFilled
            -- 0x829fa99d94dc4636925b38632e625736a614c154d55006b7ab6bea979c210c32 - log RfqOrderFilled
            -- 0xb709ddcc6550418e9b89df1f4938071eeaa3f6376309904c77e15d46b16066f5 - log UNSPECIFIED
            -- 0x8cf3dec1929508e5677d7db003124e74802bfba7250a572205a9986d86ca9f1e - log UNSPECIFIED
            -- 0x6621486d9c28838df4a87d2cca5007bc2aaf6a5b5de083b1db8faf709302c473 - log OrderFilled

			-- fillOrder // ZeroEx LOP // ethereum example: 0x31160ef8a47c9dc48f56faa4cdb856a1646ae906df97115f5967b64c9db914a3
			, (0xb4be83d5, 'LOP', 0x0bcc4c97732e47d9946f229edb95f5b6323f601300e4690de719993f3c371129, 'fillOrder'
				, map_from_entries(array[
					    ('rfq'                , map_from_entries(array[('source', 'not'             ), ('start', cast(null             as varchar)), ('flag', null), ('instance', '1')]))
					  , ('maker'              , map_from_entries(array[('source', 'log-topic1'      ), ('start', cast(12               as varchar)), ('flag', '0x'), ('instance', '1')]))
					  , ('taker'              , map_from_entries(array[('source', 'log-data'        ), ('start', cast(0*32 + 12        as varchar)), ('flag', '0x'), ('instance', '1')]))
					  , ('src_token'          , map_from_entries(array[('source', 'log-data'        ), ('start', cast(4 + 0*32 + 12    as varchar)), ('flag', '0xf47261b0'), ('instance', '1')]))
					  , ('dst_token'          , map_from_entries(array[('source', 'log-data'        ), ('start', cast(4 + 0*32 + 12    as varchar)), ('flag', '0xf47261b0'), ('instance', '2')]))
					  , ('src_max_amount'     , map_from_entries(array[('source', 'not'             ), ('start', cast(0                as varchar)), ('flag', '0x'), ('instance', '1')]))
					  , ('dst_max_amount'     , map_from_entries(array[('source', 'trace-input'     ), ('start', cast(4 + 1*32         as varchar)), ('flag', '0x'), ('instance', '1')]))
					  , ('src_fill_amount'    , map_from_entries(array[('source', 'trace-output'    ), ('start', cast(0*32             as varchar)), ('flag', '0x'), ('instance', '1')]))
					  , ('dst_fill_amount'    , map_from_entries(array[('source', 'trace-output'    ), ('start', cast(1*32             as varchar)), ('flag', '0x'), ('instance', '1')]))
					  , ('order_hash'         , map_from_entries(array[('source', 'log-topic3'      ), ('start', cast(12               as varchar)), ('flag', '0x'), ('instance', '1')])) 
				]), array[0x7712, 0x203a])
			
			-- fillLimitOrder // ZeroEx LOP // ethereum example: 0x01ac704e45ecea3d12dbb5d8b0de0559690c9cdcb2447db04647181fd8f2cf10, 0x0f7ecbbaee9d3d70a7909d6f45468beda8231ee5ef4f089932f47c07d324480f
			, (0xf6274f66, 'LOP', 0xab614d2b738543c0ea21f56347cf696a3a0c42a7cbec3212a5ca22a4dcff2124, 'fillLimitOrder'
				, map_from_entries(array[
					    ('rfq'                , map_from_entries(array[('source', 'not'             ), ('start', cast(null             as varchar)), ('flag', null), ('instance', '1')]))
					  , ('maker'              , map_from_entries(array[('source', 'log-data'        ), ('start', cast(1*32 + 12        as varchar)), ('flag', '0x'), ('instance', '1')]))
					  , ('taker'              , map_from_entries(array[('source', 'log-data'        ), ('start', cast(2*32 + 12        as varchar)), ('flag', '0x'), ('instance', '1')]))
					  , ('src_token'          , map_from_entries(array[('source', 'trace-input'     ), ('start', cast(4 + 0*32 + 12    as varchar)), ('flag', '0x'), ('instance', '1')]))
					  , ('dst_token'          , map_from_entries(array[('source', 'trace-input'     ), ('start', cast(4 + 1*32 + 12    as varchar)), ('flag', '0x'), ('instance', '1')]))
					  , ('src_max_amount'     , map_from_entries(array[('source', 'trace-input'     ), ('start', cast(4 + 2*32         as varchar)), ('flag', '0x'), ('instance', '1')]))
					  , ('dst_max_amount'     , map_from_entries(array[('source', 'trace-input'     ), ('start', cast(4 + 3*32         as varchar)), ('flag', '0x'), ('instance', '1')]))
					  , ('src_fill_amount'    , map_from_entries(array[('source', 'trace-output'    ), ('start', cast(0*32             as varchar)), ('flag', '0x'), ('instance', '1')]))
					  , ('dst_fill_amount'    , map_from_entries(array[('source', 'trace-output'    ), ('start', cast(1*32             as varchar)), ('flag', '0x'), ('instance', '1')]))
					  , ('order_hash'         , map_from_entries(array[('source', 'log-data'        ), ('start', cast(0*32             as varchar)), ('flag', '0x'), ('instance', '1')])) 
				]), array[0x5eff])
			
			-- _fillLimitOrder // ZeroEx LOP // ethereum example: 0xffffbfd4808bb470bd95daa9abc8ab23396ae0cb8901101aa4bd6f80425a878f, 0x9782447d2d13a61b3871db322feecaf1e7932e01609d341228e4593eb42508ad
			, (0x414e4ccf, 'LOP', 0xab614d2b738543c0ea21f56347cf696a3a0c42a7cbec3212a5ca22a4dcff2124, '_fillLimitOrder'
				, map_from_entries(array[
					    ('rfq'                , map_from_entries(array[('source', 'not'             ), ('start', cast(null              as varchar)), ('flag', null), ('instance', '1')]))
					  , ('maker'              , map_from_entries(array[('source', 'log-data'        ), ('start', cast(1*32 + 12         as varchar)), ('flag', '0x'), ('instance', '1')]))
					  , ('taker'              , map_from_entries(array[('source', 'log-data'        ), ('start', cast(2*32 + 12         as varchar)), ('flag', '0x'), ('instance', '1')]))
					  , ('src_token'          , map_from_entries(array[('source', 'trace-input'     ), ('start', cast(4 + 4*32 + 12     as varchar)), ('flag', '0x'), ('instance', '1')]))
					  , ('dst_token'          , map_from_entries(array[('source', 'trace-input'     ), ('start', cast(4 + 5*32 + 12     as varchar)), ('flag', '0x'), ('instance', '1')]))
					  , ('src_max_amount'     , map_from_entries(array[('source', 'trace-input'     ), ('start', cast(4 + 2*32          as varchar)), ('flag', '0x'), ('instance', '1')]))
					  , ('dst_max_amount'     , map_from_entries(array[('source', 'trace-input'     ), ('start', cast(4 + 3*32          as varchar)), ('flag', '0x'), ('instance', '1')]))
					  , ('src_fill_amount'    , map_from_entries(array[('source', 'trace-output'    ), ('start', cast(1*32              as varchar)), ('flag', '0x'), ('instance', '1')]))
					  , ('dst_fill_amount'    , map_from_entries(array[('source', 'trace-output'    ), ('start', cast(0*32              as varchar)), ('flag', '0x'), ('instance', '1')]))
					  , ('order_hash'         , map_from_entries(array[('source', 'log-data'        ), ('start', cast(0*32              as varchar)), ('flag', '0x'), ('instance', '1')])) 
				]), array[0x5eff])
			
			-- fillOrKillLimitOrder // ZeroEx LOP // ethereum example: 0x6ae5c34556b79b8bc30d0d99cf05dc9c817118dd2a34aefd818ba305146bca64, 0xf4f8c6d3e26414a5d769c34ff4cdab090a8b3c4682893d78a212dfee329665d7
			, (0x9240529c, 'LOP', 0xab614d2b738543c0ea21f56347cf696a3a0c42a7cbec3212a5ca22a4dcff2124, 'fillOrKillLimitOrder'
				, map_from_entries(array[
					    ('rfq'                , map_from_entries(array[('source', 'not'             ), ('start', null                           ), ('flag', null), ('instance', '1')]))
					  , ('maker'              , map_from_entries(array[('source', 'trace-input'     ), ('start', cast(4 + 5*32 + 12  as varchar)), ('flag', '0x'), ('instance', '1')]))
					  , ('src_token'          , map_from_entries(array[('source', 'trace-input'     ), ('start', cast(4 + 0*32 + 12  as varchar)), ('flag', '0x'), ('instance', '1')]))
					  , ('dst_token'          , map_from_entries(array[('source', 'trace-input'     ), ('start', cast(4 + 1*32 + 12  as varchar)), ('flag', '0x'), ('instance', '1')]))
					  , ('src_max_amount'     , map_from_entries(array[('source', 'trace-input'     ), ('start', cast(4 + 2*32       as varchar)), ('flag', '0x'), ('instance', '1')]))
					  , ('dst_max_amount'     , map_from_entries(array[('source', 'trace-input'     ), ('start', cast(4 + 3*32       as varchar)), ('flag', '0x'), ('instance', '1')]))
					  , ('src_fill_amount'    , map_from_entries(array[('source', 'trace-output'    ), ('start', cast(0*32           as varchar)), ('flag', '0x'), ('instance', '1')]))
					  , ('dst_fill_amount'    , map_from_entries(array[('source', 'log-data'        ), ('start', cast(6*32           as varchar)), ('flag', '0x'), ('instance', '1')]))
					  , ('order_hash'         , map_from_entries(array[('source', 'log-data'        ), ('start', cast(0*32           as varchar)), ('flag', '0x'), ('instance', '1')])) 
				]), array[0x5eff])
			
			-- fillOtcOrder // ZeroEx LOP // ethereum example: 0xc2374ffc960a19d8287c39a5ad3e01f0d681beab027e89161e582ad9bc7a9541
			, (0xdac748d4, 'LOP', 0xac75f773e3a92f1a02b12134d65e1f47f8a14eabe4eaf1e24624918e6a8b269f, 'fillOtcOrder'
				, map_from_entries(array[
					    ('rfq'                , map_from_entries(array[('source', 'not'             ), ('start', cast(null             as varchar)), ('flag', null), ('instance', '1')]))
					  , ('maker'              , map_from_entries(array[('source', 'log-data'        ), ('start', cast(1*32 + 12        as varchar)), ('flag', '0x'), ('instance', '1')]))
					  , ('taker'              , map_from_entries(array[('source', 'log-data'        ), ('start', cast(2*32 + 12        as varchar)), ('flag', '0x'), ('instance', '1')]))
					  , ('src_token'          , map_from_entries(array[('source', 'log-data'        ), ('start', cast(3*32 + 12        as varchar)), ('flag', '0x'), ('instance', '1')]))
					  , ('dst_token'          , map_from_entries(array[('source', 'log-data'        ), ('start', cast(4*32 + 12        as varchar)), ('flag', '0x'), ('instance', '1')]))
					  , ('src_max_amount'     , map_from_entries(array[('source', 'trace-input'     ), ('start', cast(4 + 2*32         as varchar)), ('flag', '0x'), ('instance', '1')]))
					  , ('dst_max_amount'     , map_from_entries(array[('source', 'trace-input'     ), ('start', cast(4 + 3*32         as varchar)), ('flag', '0x'), ('instance', '1')]))
					  , ('src_fill_amount'    , map_from_entries(array[('source', 'log-data'        ), ('start', cast(5*32             as varchar)), ('flag', '0x'), ('instance', '1')]))
					  , ('dst_fill_amount'    , map_from_entries(array[('source', 'log-data'        ), ('start', cast(6*32             as varchar)), ('flag', '0x'), ('instance', '1')]))
					  , ('order_hash'         , map_from_entries(array[('source', 'log-data'        ), ('start', cast(0*32             as varchar)), ('flag', '0x'), ('instance', '1')])) 
				]), array[0x5eff])
			
			-- _fillOtcOrder // ZeroEx LOP // ethereum example: 0xe987a4d420dbc993d249f480ec2a4c9097eafedc083f05aefa206063a130cda5
			, (0xe4ba8439, 'LOP', 0xac75f773e3a92f1a02b12134d65e1f47f8a14eabe4eaf1e24624918e6a8b269f, '_fillOtcOrder'
				, map_from_entries(array[
					    ('rfq'                , map_from_entries(array[('source', 'not'             ), ('start', cast(null             as varchar)), ('flag', null), ('instance', '1')]))
					  , ('maker'              , map_from_entries(array[('source', 'log-data'        ), ('start', cast(1*32 + 12        as varchar)), ('flag', '0x'), ('instance', '1')]))
					  , ('taker'              , map_from_entries(array[('source', 'log-data'        ), ('start', cast(2*32 + 12        as varchar)), ('flag', '0x'), ('instance', '1')]))
					  , ('src_token'          , map_from_entries(array[('source', 'log-data'        ), ('start', cast(3*32 + 12        as varchar)), ('flag', '0x'), ('instance', '1')]))
					  , ('dst_token'          , map_from_entries(array[('source', 'log-data'        ), ('start', cast(4*32 + 12        as varchar)), ('flag', '0x'), ('instance', '1')]))
					  , ('src_max_amount'     , map_from_entries(array[('source', 'trace-input'     ), ('start', cast(4 + 2*32         as varchar)), ('flag', '0x'), ('instance', '1')]))
					  , ('dst_max_amount'     , map_from_entries(array[('source', 'trace-input'     ), ('start', cast(4 + 3*32         as varchar)), ('flag', '0x'), ('instance', '1')]))
					  , ('src_fill_amount'    , map_from_entries(array[('source', 'log-data'        ), ('start', cast(5*32             as varchar)), ('flag', '0x'), ('instance', '1')]))
					  , ('dst_fill_amount'    , map_from_entries(array[('source', 'log-data'        ), ('start', cast(6*32             as varchar)), ('flag', '0x'), ('instance', '1')]))
					  , ('order_hash'         , map_from_entries(array[('source', 'log-data'        ), ('start', cast(0*32             as varchar)), ('flag', '0x'), ('instance', '1')])) 
				]), array[0x5eff])
			
			-- fillOtcOrderForEth // ZeroEx LOP // ethereum example: 0xb0e57d0096eb001c7b2b07f138c15dae5f7d31a9404b911f94cdc15a726dd369
			, (0xa578efaf, 'LOP', 0xac75f773e3a92f1a02b12134d65e1f47f8a14eabe4eaf1e24624918e6a8b269f, 'fillOtcOrderForEth'
				, map_from_entries(array[
					    ('rfq'                , map_from_entries(array[('source', 'not'             ), ('start', cast(null             as varchar)), ('flag', null), ('instance', '1')]))
					  , ('maker'              , map_from_entries(array[('source', 'log-data'        ), ('start', cast(1*32 + 12        as varchar)), ('flag', '0x'), ('instance', '1')]))
					  , ('taker'              , map_from_entries(array[('source', 'log-data'        ), ('start', cast(2*32 + 12        as varchar)), ('flag', '0x'), ('instance', '1')]))
					  , ('src_token'          , map_from_entries(array[('source', 'log-data'        ), ('start', cast(3*32 + 12        as varchar)), ('flag', '0x'), ('instance', '1')]))
					  , ('dst_token'          , map_from_entries(array[('source', 'log-data'        ), ('start', cast(4*32 + 12        as varchar)), ('flag', '0x'), ('instance', '1')]))
					  , ('src_max_amount'     , map_from_entries(array[('source', 'trace-input'     ), ('start', cast(4 + 2*32         as varchar)), ('flag', '0x'), ('instance', '1')]))
					  , ('dst_max_amount'     , map_from_entries(array[('source', 'trace-input'     ), ('start', cast(4 + 3*32         as varchar)), ('flag', '0x'), ('instance', '1')]))
					  , ('src_fill_amount'    , map_from_entries(array[('source', 'log-data'        ), ('start', cast(5*32             as varchar)), ('flag', '0x'), ('instance', '1')]))
					  , ('dst_fill_amount'    , map_from_entries(array[('source', 'log-data'        ), ('start', cast(6*32             as varchar)), ('flag', '0x'), ('instance', '1')]))
					  , ('order_hash'         , map_from_entries(array[('source', 'log-data'        ), ('start', cast(0*32             as varchar)), ('flag', '0x'), ('instance', '1')])) 
				]), array[0x5eff])
			
			-- fillOtcOrderWithEth // ZeroEx LOP // ethereum example: 0x68e807fde086d8b22583f387ab47d46c26067f198f7d04b6c8ddead2a5ae5d03
			, (0x706394d5, 'LOP', 0xac75f773e3a92f1a02b12134d65e1f47f8a14eabe4eaf1e24624918e6a8b269f, 'fillOtcOrderWithEth'
				, map_from_entries(array[
					    ('rfq'                , map_from_entries(array[('source', 'not'             ), ('start', cast(null             as varchar)), ('flag', null), ('instance', '1')]))
					  , ('maker'              , map_from_entries(array[('source', 'log-data'        ), ('start', cast(1*32 + 12        as varchar)), ('flag', '0x'), ('instance', '1')]))
					  , ('taker'              , map_from_entries(array[('source', 'log-data'        ), ('start', cast(2*32 + 12        as varchar)), ('flag', '0x'), ('instance', '1')]))
					  , ('src_token'          , map_from_entries(array[('source', 'log-data'        ), ('start', cast(3*32 + 12        as varchar)), ('flag', '0x'), ('instance', '1')]))
					  , ('dst_token'          , map_from_entries(array[('source', 'log-data'        ), ('start', cast(4*32 + 12        as varchar)), ('flag', '0x'), ('instance', '1')]))
					  , ('src_max_amount'     , map_from_entries(array[('source', 'trace-input'     ), ('start', cast(4 + 2*32         as varchar)), ('flag', '0x'), ('instance', '1')]))
					  , ('dst_max_amount'     , map_from_entries(array[('source', 'trace-input'     ), ('start', cast(4 + 3*32         as varchar)), ('flag', '0x'), ('instance', '1')]))
					  , ('src_fill_amount'    , map_from_entries(array[('source', 'log-data'        ), ('start', cast(5*32             as varchar)), ('flag', '0x'), ('instance', '1')]))
					  , ('dst_fill_amount'    , map_from_entries(array[('source', 'log-data'        ), ('start', cast(6*32             as varchar)), ('flag', '0x'), ('instance', '1')]))
					  , ('order_hash'         , map_from_entries(array[('source', 'log-data'        ), ('start', cast(0*32             as varchar)), ('flag', '0x'), ('instance', '1')])) 
				]), array[0x5eff])
			
			-- fillTakerSignedOtcOrderForEth // ZeroEx LOP // ethereum example: 0xeba0c29546f46a7802aa55c9a708d4d48fa673e6b82c4fb731f6b996aae63743
			, (0x724d3953, 'LOP', 0xac75f773e3a92f1a02b12134d65e1f47f8a14eabe4eaf1e24624918e6a8b269f, 'fillTakerSignedOtcOrderForEth'
				, map_from_entries(array[
					    ('rfq'                , map_from_entries(array[('source', 'not'             ), ('start', cast(null             as varchar)), ('flag', null), ('instance', '1')]))
					  , ('maker'              , map_from_entries(array[('source', 'log-data'        ), ('start', cast(1*32 + 12        as varchar)), ('flag', '0x'), ('instance', '1')]))
					  , ('taker'              , map_from_entries(array[('source', 'log-data'        ), ('start', cast(2*32 + 12        as varchar)), ('flag', '0x'), ('instance', '1')]))
					  , ('src_token'          , map_from_entries(array[('source', 'log-data'        ), ('start', cast(3*32 + 12        as varchar)), ('flag', '0x'), ('instance', '1')]))
					  , ('dst_token'          , map_from_entries(array[('source', 'log-data'        ), ('start', cast(4*32 + 12        as varchar)), ('flag', '0x'), ('instance', '1')]))
					  , ('src_max_amount'     , map_from_entries(array[('source', 'trace-input'     ), ('start', cast(4 + 2*32         as varchar)), ('flag', '0x'), ('instance', '1')]))
					  , ('dst_max_amount'     , map_from_entries(array[('source', 'trace-input'     ), ('start', cast(4 + 3*32         as varchar)), ('flag', '0x'), ('instance', '1')]))
					  , ('src_fill_amount'    , map_from_entries(array[('source', 'log-data'        ), ('start', cast(5*32             as varchar)), ('flag', '0x'), ('instance', '1')]))
					  , ('dst_fill_amount'    , map_from_entries(array[('source', 'log-data'        ), ('start', cast(6*32             as varchar)), ('flag', '0x'), ('instance', '1')]))
					  , ('order_hash'         , map_from_entries(array[('source', 'log-data'        ), ('start', cast(0*32             as varchar)), ('flag', '0x'), ('instance', '1')])) 
				]), array[0x5eff])
			
			-- fillTakerSignedOtcOrder // ZeroEx LOP // ethereum example: 0x73b023e92a7735d7a7472da9b1ba83123af5b376a020f8bd68eb7ede48cc6bc0
			, (0x4f948110, 'LOP', 0xac75f773e3a92f1a02b12134d65e1f47f8a14eabe4eaf1e24624918e6a8b269f, 'fillTakerSignedOtcOrder'
				, map_from_entries(array[
					    ('rfq'                , map_from_entries(array[('source', 'not'             ), ('start', cast(null             as varchar)), ('flag', null), ('instance', '1')]))
					  , ('maker'              , map_from_entries(array[('source', 'trace-input'     ), ('start', cast(4 + 4*32 + 12    as varchar)), ('flag', '0x'), ('instance', '1')]))
					  , ('taker'              , map_from_entries(array[('source', 'log-data'        ), ('start', cast(2*32 + 12        as varchar)), ('flag', '0x'), ('instance', '1')]))
					  , ('src_token'          , map_from_entries(array[('source', 'trace-input'     ), ('start', cast(4 + 0*32 + 12    as varchar)), ('flag', '0x'), ('instance', '1')]))
					  , ('dst_token'          , map_from_entries(array[('source', 'trace-input'     ), ('start', cast(4 + 1*32 + 12    as varchar)), ('flag', '0x'), ('instance', '1')]))
					  , ('src_max_amount'     , map_from_entries(array[('source', 'trace-input'     ), ('start', cast(4 + 2*32         as varchar)), ('flag', '0x'), ('instance', '1')]))
					  , ('dst_max_amount'     , map_from_entries(array[('source', 'trace-input'     ), ('start', cast(4 + 3*32         as varchar)), ('flag', '0x'), ('instance', '1')]))
					  , ('src_fill_amount'    , map_from_entries(array[('source', 'log-data'        ), ('start', cast(5*32             as varchar)), ('flag', '0x'), ('instance', '1')]))
					  , ('dst_fill_amount'    , map_from_entries(array[('source', 'log-data'        ), ('start', cast(6*32             as varchar)), ('flag', '0x'), ('instance', '1')]))
					  , ('order_hash'         , map_from_entries(array[('source', 'log-data'        ), ('start', cast(0*32             as varchar)), ('flag', '0x'), ('instance', '1')])) 
				]), array[0x5eff])
			
			-- fillRfqOrder // ZeroEx LOP // ethereum example: 0x2ac0917e5383a044d10ba5d64efc6ed1c8f5dc5f5439a3419581f74b61f241dc, 0xfffefad7f60428c8ee1cb94a5179e810d532822db9df8385d2fb1b506d825679
			, (0xaa77476c, 'LOP', 0x829fa99d94dc4636925b38632e625736a614c154d55006b7ab6bea979c210c32, 'fillRfqOrder'
				, map_from_entries(array[
					    ('rfq'                , map_from_entries(array[('source', 'yes'             ), ('start', cast(null             as varchar)), ('flag', null), ('instance', '1')]))
					  , ('maker'              , map_from_entries(array[('source', 'trace-input'     ), ('start', cast(4 + 4*32 + 12    as varchar)), ('flag', '0x'), ('instance', '1')]))
					  , ('taker'              , map_from_entries(array[('source', 'log-data'        ), ('start', cast(2*32 + 12        as varchar)), ('flag', '0x'), ('instance', '1')]))
					  , ('src_token'          , map_from_entries(array[('source', 'trace-input'     ), ('start', cast(4 + 0*32 + 12    as varchar)), ('flag', '0x'), ('instance', '1')]))
					  , ('dst_token'          , map_from_entries(array[('source', 'trace-input'     ), ('start', cast(4 + 1*32 + 12    as varchar)), ('flag', '0x'), ('instance', '1')]))
					  , ('src_max_amount'     , map_from_entries(array[('source', 'trace-input'     ), ('start', cast(4 + 2*32         as varchar)), ('flag', '0x'), ('instance', '1')]))
					  , ('dst_max_amount'     , map_from_entries(array[('source', 'trace-input'     ), ('start', cast(4 + 3*32         as varchar)), ('flag', '0x'), ('instance', '1')]))
					  , ('src_fill_amount'    , map_from_entries(array[('source', 'trace-output'    ), ('start', cast(0*32             as varchar)), ('flag', '0x'), ('instance', '1')]))
					  , ('dst_fill_amount'    , map_from_entries(array[('source', 'trace-output'    ), ('start', cast(1*32             as varchar)), ('flag', '0x'), ('instance', '1')]))
					  , ('order_hash'         , map_from_entries(array[('source', 'log-data'        ), ('start', cast(0*32             as varchar)), ('flag', '0x'), ('instance', '1')])) 
				]), array[0x5eff])
			
			-- _fillRfqOrder // ZeroEx LOP // ethereum example: 0x2bae560122720b1a3cb4d92191c11469d89957cc589f1b02430655df46f7db8d
			, (0xaa6b21cd, 'LOP', 0x829fa99d94dc4636925b38632e625736a614c154d55006b7ab6bea979c210c32, '_fillRfqOrder'
				, map_from_entries(array[
					    ('rfq'                , map_from_entries(array[('source', 'yes'             ), ('start', cast(null             as varchar)), ('flag', null), ('instance', '1')]))
					  , ('maker'              , map_from_entries(array[('source', 'trace-input'     ), ('start', cast(4 + 4*32 + 12    as varchar)), ('flag', '0x'), ('instance', '1')]))
					  , ('taker'              , map_from_entries(array[('source', 'log-data'        ), ('start', cast(2*32 + 12        as varchar)), ('flag', '0x'), ('instance', '1')]))
					  , ('src_token'          , map_from_entries(array[('source', 'trace-input'     ), ('start', cast(4 + 0*32 + 12    as varchar)), ('flag', '0x'), ('instance', '1')]))
					  , ('dst_token'          , map_from_entries(array[('source', 'trace-input'     ), ('start', cast(4 + 1*32 + 12    as varchar)), ('flag', '0x'), ('instance', '1')]))
					  , ('src_max_amount'     , map_from_entries(array[('source', 'trace-input'     ), ('start', cast(4 + 2*32         as varchar)), ('flag', '0x'), ('instance', '1')]))
					  , ('dst_max_amount'     , map_from_entries(array[('source', 'trace-input'     ), ('start', cast(4 + 3*32         as varchar)), ('flag', '0x'), ('instance', '1')]))
					  , ('src_fill_amount'    , map_from_entries(array[('source', 'trace-output'    ), ('start', cast(0*32             as varchar)), ('flag', '0x'), ('instance', '1')]))
					  , ('dst_fill_amount'    , map_from_entries(array[('source', 'trace-output'    ), ('start', cast(1*32             as varchar)), ('flag', '0x'), ('instance', '1')]))
					  , ('order_hash'         , map_from_entries(array[('source', 'log-data'        ), ('start', cast(0*32             as varchar)), ('flag', '0x'), ('instance', '1')])) 
				]), array[0x5eff])
			
			-- _fillRfqOrder // ZeroEx LOP // ethereum example: 0xf62f5699bfeba1d4f5b39c9abfdf0b29e4e9dad4a2721fd686d8c5fba3460486
			, (0xa656186b, 'LOP', 0x829fa99d94dc4636925b38632e625736a614c154d55006b7ab6bea979c210c32, '_fillRfqOrder'
				, map_from_entries(array[
					    ('rfq'                , map_from_entries(array[('source', 'yes'             ), ('start', cast(null             as varchar)), ('flag', null), ('instance', '1')]))
					  , ('maker'              , map_from_entries(array[('source', 'trace-input'     ), ('start', cast(4 + 4*32 + 12    as varchar)), ('flag', '0x'), ('instance', '1')]))
					  , ('taker'              , map_from_entries(array[('source', 'log-data'        ), ('start', cast(2*32 + 12        as varchar)), ('flag', '0x'), ('instance', '1')]))
					  , ('src_token'          , map_from_entries(array[('source', 'trace-input'     ), ('start', cast(4 + 0*32 + 12    as varchar)), ('flag', '0x'), ('instance', '1')]))
					  , ('dst_token'          , map_from_entries(array[('source', 'trace-input'     ), ('start', cast(4 + 1*32 + 12    as varchar)), ('flag', '0x'), ('instance', '1')]))
					  , ('src_max_amount'     , map_from_entries(array[('source', 'trace-input'     ), ('start', cast(4 + 2*32         as varchar)), ('flag', '0x'), ('instance', '1')]))
					  , ('dst_max_amount'     , map_from_entries(array[('source', 'trace-input'     ), ('start', cast(4 + 3*32         as varchar)), ('flag', '0x'), ('instance', '1')]))
					  , ('src_fill_amount'    , map_from_entries(array[('source', 'trace-output'    ), ('start', cast(0*32             as varchar)), ('flag', '0x'), ('instance', '1')]))
					  , ('dst_fill_amount'    , map_from_entries(array[('source', 'trace-output'    ), ('start', cast(1*32             as varchar)), ('flag', '0x'), ('instance', '1')]))
					  , ('order_hash'         , map_from_entries(array[('source', 'log-data'        ), ('start', cast(0*32             as varchar)), ('flag', '0x'), ('instance', '1')])) 
				]), array[0x5eff])
			
			-- fillOrKillRfqOrder // ZeroEx LOP // ethereum example: 0x38e4db7f1cd4673e5615a9d6435314e71f149e25756f219920cd5e87a63ee876
			, (0x438cdfc5, 'LOP', 0x829fa99d94dc4636925b38632e625736a614c154d55006b7ab6bea979c210c32, 'fillOrKillRfqOrder'
				, map_from_entries(array[
					    ('rfq'                , map_from_entries(array[('source', 'yes'             ), ('start', cast(null              as varchar)), ('flag', null), ('instance', '1')]))
					  , ('maker'              , map_from_entries(array[('source', 'log-data'        ), ('start', cast(1*32 + 12         as varchar)), ('flag', '0x'), ('instance', '1')]))
					  , ('taker'              , map_from_entries(array[('source', 'log-data'        ), ('start', cast(2*32 + 12         as varchar)), ('flag', '0x'), ('instance', '1')]))
					  , ('src_token'          , map_from_entries(array[('source', 'trace-input'     ), ('start', cast(4 + 3*32 + 12     as varchar)), ('flag', '0x'), ('instance', '1')]))
					  , ('dst_token'          , map_from_entries(array[('source', 'trace-input'     ), ('start', cast(4 + 4*32 + 12     as varchar)), ('flag', '0x'), ('instance', '1')]))
					  , ('src_max_amount'     , map_from_entries(array[('source', 'trace-input'     ), ('start', cast(4 + 2*32          as varchar)), ('flag', '0x'), ('instance', '1')]))
					  , ('dst_max_amount'     , map_from_entries(array[('source', 'trace-input'     ), ('start', cast(4 + 3*32          as varchar)), ('flag', '0x'), ('instance', '1')]))
					  , ('src_fill_amount'    , map_from_entries(array[('source', 'trace-output'    ), ('start', cast(0*32              as varchar)), ('flag', '0x'), ('instance', '1')]))
					  , ('dst_fill_amount'    , map_from_entries(array[('source', 'log-data'        ), ('start', cast(5*32              as varchar)), ('flag', '0x'), ('instance', '1')]))
					  , ('order_hash'         , map_from_entries(array[('source', 'log-data'        ), ('start', cast(0*32              as varchar)), ('flag', '0x'), ('instance', '1')])) 
				]), array[0x5eff])
			
			-- UNKNOWN // Hashflow LOP // ethereum example: 0xba774f2bd851b29ba1bc473830c6d04cf3ebb28fa50d16dd24400f85663eac9d, 0x905a780b05fe0c60571bf4d998f8200a4efd7847deef7bfa3008bf429156df51
			, (0x1e9a2e92, 'LOP', 0xb709ddcc6550418e9b89df1f4938071eeaa3f6376309904c77e15d46b16066f5, 'UNKNOWN'
				, map_from_entries(array[
					    ('rfq'                , map_from_entries(array[('source', 'yes'             ), ('start', cast(null              as varchar)), ('flag', null), ('instance', '1')]))
					  , ('maker'              , map_from_entries(array[('source', 'trace-input'     ), ('start', cast(4 + 3*32 + 12     as varchar)), ('flag', '0x'), ('instance', '1')]))
					  , ('maker_ext'          , map_from_entries(array[('source', 'trace-input'     ), ('start', cast(4 + 2*32 + 12     as varchar)), ('flag', '0x'), ('instance', '1')]))
					  , ('taker'              , map_from_entries(array[('source', 'log-data'        ), ('start', cast(1*32 + 12         as varchar)), ('flag', '0x'), ('instance', '1')]))
					  , ('src_token'          , map_from_entries(array[('source', 'log-data'        ), ('start', cast(4*32 + 12         as varchar)), ('flag', '0x'), ('instance', '1')]))
					  , ('dst_token'          , map_from_entries(array[('source', 'log-data'        ), ('start', cast(3*32 + 12         as varchar)), ('flag', '0x'), ('instance', '1')]))
					  , ('src_max_amount'     , map_from_entries(array[('source', 'trace-input'     ), ('start', cast(4 + 10*32         as varchar)), ('flag', '0x'), ('instance', '1')]))
					  , ('dst_max_amount'     , map_from_entries(array[('source', 'trace-input'     ), ('start', cast(4 + 8*32          as varchar)), ('flag', '0x'), ('instance', '1')]))
					  , ('src_fill_amount'    , map_from_entries(array[('source', 'log-data'        ), ('start', cast(6*32              as varchar)), ('flag', '0x'), ('instance', '1')]))
					  , ('dst_fill_amount'    , map_from_entries(array[('source', 'log-data'        ), ('start', cast(5*32              as varchar)), ('flag', '0x'), ('instance', '1')]))
					  , ('order_hash'         , map_from_entries(array[('source', 'log-data'        ), ('start', cast(2*32              as varchar)), ('flag', '0x'), ('instance', '1')])) 
				]), array[0x6af9])
			
			-- tradeSingleHop // Hashflow LOP // ethereum example: 0x00087dd7426afed508300c62dab4c33ace617ef6f07332efb51da14125fc0d1a, 0xffffefd7417c4ec044cd807e2a12c3f0691490b0a82686308434c5b9a8e9e64e
			, (0xf0210929, 'LOP', 0x8cf3dec1929508e5677d7db003124e74802bfba7250a572205a9986d86ca9f1e, 'tradeSingleHop'
				, map_from_entries(array[
					    ('rfq'                , map_from_entries(array[('source', 'yes'             ), ('start', cast(null              as varchar)), ('flag', null), ('instance', '1')]))
					  , ('maker'              , map_from_entries(array[('source', 'trace-input'     ), ('start', cast(4 + 2*32 + 12     as varchar)), ('flag', '0x'), ('instance', '1')]))
					  , ('maker_ext'          , map_from_entries(array[('source', 'trace-input'     ), ('start', cast(4 + 1*32 + 12     as varchar)), ('flag', '0x'), ('instance', '1')]))
					  , ('taker'              , map_from_entries(array[('source', 'log-data'        ), ('start', cast(0*32 + 12         as varchar)), ('flag', '0x'), ('instance', '1')]))
					  , ('src_token'          , map_from_entries(array[('source', 'log-data'        ), ('start', cast(3*32 + 12         as varchar)), ('flag', '0x'), ('instance', '1')]))
					  , ('dst_token'          , map_from_entries(array[('source', 'log-data'        ), ('start', cast(2*32 + 12         as varchar)), ('flag', '0x'), ('instance', '1')]))
					  , ('src_max_amount'     , map_from_entries(array[('source', 'trace-input'     ), ('start', cast(4 + 9*32          as varchar)), ('flag', '0x'), ('instance', '1')]))
					  , ('dst_max_amount'     , map_from_entries(array[('source', 'trace-input'     ), ('start', cast(4 + 7*32          as varchar)), ('flag', '0x'), ('instance', '1')]))
					  , ('src_fill_amount'    , map_from_entries(array[('source', 'log-data'        ), ('start', cast(5*32              as varchar)), ('flag', '0x'), ('instance', '1')]))
					  , ('dst_fill_amount'    , map_from_entries(array[('source', 'log-data'        ), ('start', cast(4*32              as varchar)), ('flag', '0x'), ('instance', '1')]))
					  , ('order_hash'         , map_from_entries(array[('source', 'log-data'        ), ('start', cast(1*32              as varchar)), ('flag', '0x'), ('instance', '1')])) 
				]), array[0x6613])
			
			-- fillOrder // Paraswap LOP // ethereum example: 0x23fa6e580186201106f1e902e24e56f9f6dd62863293c3f1f99e64d686ebe833
			, (0x98f9b46b, 'LOP', 0x6621486d9c28838df4a87d2cca5007bc2aaf6a5b5de083b1db8faf709302c473, 'fillOrder'
				, map_from_entries(array[
					    ('rfq'                , map_from_entries(array[('source', 'not'             ), ('start', cast(null             as varchar)), ('flag', null), ('instance', '1')]))
					  , ('maker'              , map_from_entries(array[('source', 'log-topic2'      ), ('start', cast(12               as varchar)), ('flag', '0x'), ('instance', '1')]))
					  , ('taker'              , map_from_entries(array[('source', 'log-topic3'      ), ('start', cast(12               as varchar)), ('flag', '0x'), ('instance', '1')]))
					  , ('src_token'          , map_from_entries(array[('source', 'log-data'        ), ('start', cast(0*32 + 12        as varchar)), ('flag', '0xf47261b0'), ('instance', '1')]))
					  , ('dst_token'          , map_from_entries(array[('source', 'log-data'        ), ('start', cast(2*32 + 12        as varchar)), ('flag', '0xf47261b0'), ('instance', '2')]))
					  , ('src_max_amount'     , map_from_entries(array[('source', 'trace-input'     ), ('start', cast(4 + 6*32         as varchar)), ('flag', '0x'), ('instance', '1')]))
					  , ('dst_max_amount'     , map_from_entries(array[('source', 'trace-input'     ), ('start', cast(4 + 7*32         as varchar)), ('flag', '0x'), ('instance', '1')]))
					  , ('src_fill_amount'    , map_from_entries(array[('source', 'log-data'        ), ('start', cast(1*32             as varchar)), ('flag', '0x'), ('instance', '1')]))
					  , ('dst_fill_amount'    , map_from_entries(array[('source', 'log-data'        ), ('start', cast(3*32             as varchar)), ('flag', '0x'), ('instance', '1')]))
					  , ('order_hash'         , map_from_entries(array[('source', 'log-topic1'      ), ('start', cast(12               as varchar)), ('flag', '0x'), ('instance', '1')])) 
				]), array[0x5a06])
    			
        ) as s(selector, protocol, topic0, method, params, contracts), unnest(contracts) as contracts(contract_id)
        
    )

select

    -- contracts
      project
    , contract_address
    , contract_id
    , contract_name
    , blockchain
    , created
    
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