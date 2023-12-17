{{
  config(
    
    alias='ccip_network_meta',
    post_hook='{{ expose_spells(\'["arbitrum", "ethereum", "avalanche_c", "bnb", "base", "polygon", "optimism"]\',
                                "project",
                                "chainlink",
                                \'["linkpool_jon"]\') }}'
  )
}}


SELECT 
blockchain, 
router AS router, 
cast(chain_selector AS UINT256) AS chain_selector 
FROM (VALUES 
 ('ethereum', 0xe561d5e02207fb5eb32cca20a699e0d8919a1476, '5009297550715157269')
, ('optimism', 0x261c05167db67b2b619f9d312e0753f3721ad6e8, '3734403246176062136')
, ('avalanche_c', 0x27f39d0af3303703750d4001fcc1844c6491563c, '6433500567565415381')
, ('polygon', 0x3c3d92629a02a8d95d5cb9650fe49c3544f69b43, '4051577828743386545')
, ('arbitrum', 0xE92634289A1841A979C11C2f618B33D376e4Ba85, '4949039107694359620')
, ('base', 0x673aa85efd75080031d44fca061575d1da427a28, '15971525489660198786')
, ('bnb', 0x536d7e53d0adeb1f20e7c81fea45d02ec9dbd698, '11344663589394136015')
)
AS a (blockchain, router, chain_selector)
