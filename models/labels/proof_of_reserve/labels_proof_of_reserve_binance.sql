{{config(alias='proof_of_reserve_ethereum',
        post_hook='{{ expose_spells(\'["ethereum"]\',
                                    "sector",
                                    "labels",
                                    \'["soispoke"]\') }}')}}

SELECT blockchain, address, name, category, contributor, source, created_at, updated_at
FROM (VALUES




,('0x4976a4a02f38326660d17bf34b431dc6e2eb2327'),('0xa344c7aDA83113B3B56941F6e85bf2Eb425949f3')

('0x47ac0Fb4F2D84898e4D9E7b4DaB3C24507a6D503')
,('0xa344c7aDA83113B3B56941F6e85bf2Eb425949f3')

,('0x9696f59e4d72e237be84ffd425dcad154bf96976'),('0x4976a4a02f38326660d17bf34b431dc6e2eb2327')
,('0x47ac0Fb4F2D84898e4D9E7b4DaB3C24507a6D503')
,('0xbe0eb53f46cd790cd13851d5eff43d12404d33e8'),
,('0xa344c7aDA83113B3B56941F6e85bf2Eb425949f3'),
,('0xbe0eb53f46cd790cd13851d5eff43d12404d33e8'),)

    -- Binance Proof-of-Reserve on Ethereum, Source: https://www.binance.com/en/collateral-btokens
      -- BTC
      (array('ethereum'), '0xf977814e90da44bfa03b6295a0616a897441acec', 'Binance: Proof-of-Reserve', 'proof_of_reserve', 'soispoke', 'static', timestamp('2022-11-14'), now()),
      --USDT, BUSD
      (array('ethereum'), '0x47ac0Fb4F2D84898e4D9E7b4DaB3C24507a6D503', 'Binance: Proof-of-Reserve', 'proof_of_reserve', 'soispoke', 'static', timestamp('2022-11-14'), now()),
      -- ETH and ERC-20
      (array('ethereum'), '0x9be89d2a4cd102d8fecc6bf9da793be995c22541', 'Binance: Proof-of-Reserve', 'proof_of_reserve', 'soispoke', 'static', timestamp('2022-11-14'), now()),
      -- DOT
      (array('ethereum'), '0x7884f51dc1410387371ce61747cb6264e1daee0b', 'Binance: Proof-of-Reserve', 'proof_of_reserve', 'soispoke', 'static', timestamp('2022-11-14'), now()),
    
    
    -- Binance Proof-of-Reserve on BNB chain
      (array('bnb'), '0x0ac2d6f5f5afc669d3ca38f830dad2b4f238ad3f', 'Binance: Proof-of-Reserve', 'proof_of_reserve', 'soispoke', 'static', timestamp('2022-11-14'), now()),
      (array('bnb'), '0x0ac2d6f5f5afc669d3ca38f830dad2b4f238ad3f', 'Binance: Proof-of-Reserve', 'proof_of_reserve', 'soispoke', 'static', timestamp('2022-11-14'), now()),
      (array('bnb'), '0x0ac2d6f5f5afc669d3ca38f830dad2b4f238ad3f', 'Binance: Proof-of-Reserve', 'proof_of_reserve', 'soispoke', 'static', timestamp('2022-11-14'), now())
    ) AS x (blockchain, address, name, category, contributor, source, created_at, updated_at);