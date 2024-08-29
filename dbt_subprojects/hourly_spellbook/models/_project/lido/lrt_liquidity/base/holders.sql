{{ config(
        schema='lido_lrt_liquidity_base',
        alias = 'holders',
        materialized = 'table',
        file_format = 'delta',
        post_hook='{{ expose_spells(\'["base"]\',
                                "project",
                                "lido_lrt_liquidity",
                                \'["pipistrella"]\') }}'
        )
}}

with holders as (

 SELECT * 
     FROM (
            values 
            
               (0x46e6b214b524310239732d51387075e0e70970bf,  'Compound v3', 'lending', 'base', '')
             , (0x7C307e128efA31F540F2E2d976C995E0B65F51F6,  'Aave', 'lending', 'base', '')
             , (0xBBBBBbbBBb9cC5e90e3b3Af64bdAF62C37EEFFCb,  'MorphoBlue', 'lending', 'base', '')
             , (0x223A4066bd6A30477Ead12a7AF52125390C735dA,  'Radiant', 'lending', 'base', '')
             , (0x91F0f34916Ca4E2cCe120116774b0e4fA0cdcaA8,  'Aerodrome', 'liquidity_pool', 'base', 'WETH')
             , (0x497139e8435E01555AC1e3740fccab7AFf149e02,  'Aerodrome', 'liquidity_pool', 'base', 'WETH')
             , (0xDC7EAd706795eDa3FEDa08Ad519d9452BAdF2C0d,  'Aerodrome', 'liquidity_pool', 'base', 'WETH')
             , (0x0C8bF3cb3E1f951B284EF14aa95444be86a33E2f,  'Aerodrome', 'liquidity_pool', 'base', 'WETH')
             , (0x52Aa899454998Be5b000Ad077a46Bbe360F4e497,  'Fluid', 'yield', 'base', '' )
             , (0x9c5e676914d6d9707A587dbAF4fE524c678d4B51,  'Beefy', 'yield', 'base', '' )
             
             
  )x (address, namespace, category, blockchain, paired_token)      
  
  )
  
  
SELECT * FROM holders
  