 {{
  config(
        schema='gamma_optimism',
        alias='uniswap_pools',
        materialized = 'table',
        unique_key = ['contract_address', 'pool_contract']
  )
}}

{% set project_start_date = '2022-04-26' %}

-- Gamma pools don't come from a factory contract, so we manually map them
with manual_mapping as (
SELECT lp_name, LOWER(addr) AS contract_address
        FROM (values
                ('wsteth.eth.05.p','0xa8524abc230725da48e61b677504fb5e8a409e38')
                ,('usdc.dai.01.s','0xaED05fdd471a4EecEe48B34d38c59CC76681A6C8')
                ,('eth.dai.05.w','0xC79265161eE0766802c576Cc0154B03CF2Cb72FF')
                ,('eth.dai.05.n','0x6F670062bCa8C759A876841FcF1c4B0147879097')
                ,('op.usdc.3.w','0x4f2b80D797Bd1B93903Cc84e5992586142C3ECD1')
                ,('op.usdc.3.n','0x2102BeF0d9727EA50bA844e7658e38480961835c')
                ,('eth.usdc.05.n','0x431f6e577a431d9ee87a535fde2db830e352e33c')
                ,('eth.usdc.05.w','0xed17209ab7f9224e29cc9894fa14a011f37b6115')
                ,('wsteth.weth.01.p','0x2225d4124d5da09765b768be5334b93844dfd1a0')
                ,('wsteth.wbtc.05.n','0x51b10b4c8224144e31ab7b570ae51359c4fcc3a6')
                ,('wsteth.wbtc.05.w','0x23851bf7d838d50b1b5164b75c79bda3c1bb6fa3')
                ,('wsteth.usdt.05.n','0xee538e14ec8ff4985301190e96ffa26e828c929c')
                ,('wsteth.usdt.05.w','0x4d41f2feb1e0c20a5aeadafc358a49b771682b22')
                ,('wsteth.usdc.05.n','0xa330d33f1da50a678f33c853d19fae28aa1077e5')
                ,('wsteth.usdc.05.w','0xf3f81e3140e5fb73ffb900cf0d6461196b1ecbc3')
                ,('usdc.usdt.01.s','0xe567d2fbe6c52f22250963a43506c1f15c08307e')
                ,('weth.wbtc.05.n','0xc581e7c92aee3ae52f6bd78437b9430be9fb0c76')
                ,('weth.wbtc.05.w','0x66df9d4a1f065106bd1c489b592d50cfebf78160')
        ) a (lp_name, addr)
)


SELECT distinct
    'optimism' AS blockchain,
    lp_name, mm.contract_address, pool AS pool_contract, fee, token0, token1
    FROM manual_mapping mm
    INNER JOIN {{ source('optimism', 'creation_traces') }} ct 
        ON ct.address = mm.contract_address
        -- only pull new contract creations
        AND ct.block_time >= '{{project_start_date}}'
    INNER JOIN {{ source('optimism', 'transactions') }} t 
        ON t.to = mm.contract_address
        AND t.block_time >= '{{project_start_date}}'
        AND t.block_time >= ct.block_time
        AND t.block_time < ct.block_time + interval '1 month'
        AND substring(t.data,1,10) IN ('0x85919c5d','0xa8559872') --on rebalances & withdrawals, we can pull the uniswap pool
    INNER JOIN {{ source('optimism', 'logs') }} l 
        ON t.hash = l.tx_hash
        AND t.block_number = l.block_number
        AND t.block_time = l.block_time
        AND l.block_time >= ct.block_time
        AND l.block_time < ct.block_time + interval '1 month'
        AND l.block_time >= '{{project_start_date}}'
    INNER JOIN {{ ref('uniswap_optimism_pools') }} up
        ON up.pool = l.contract_address
