{{ config(
        schema='lido_accounting_ethereum',
        alias = 'liquidity_incentives',

        materialized = 'table',
        file_format = 'delta',
        post_hook='{{ expose_spells(\'["ethereum"]\',
                                "project",
                                "lido_accounting",
                                \'["pipistrella", "adcv", "zergil1397"]\') }}'
        )
}}

with tokens AS (
          select * from (values
          -- Ethereum tokens
          (0x5A98FcBEA516Cf06857215779Fd812CA3beF1B32, 'ethereum', 'LDO'),
          (0x6B175474E89094C44Da98b954EedeAC495271d0F, 'ethereum', 'DAI'),
          (0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48, 'ethereum', 'USDC'),
          (0xdAC17F958D2ee523a2206206994597C13D831ec7, 'ethereum', 'USDT'),
          (0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2, 'ethereum', 'WETH'),
          (0x7D1AfA7B718fb893dB30A3aBc0Cfc608AaCfeBB0, 'ethereum', 'MATIC'),
          (0xae7ab96520DE3A18E5e111B5EaAb095312D7fE84, 'ethereum', 'stETH'),
          (0x7f39C581F595B53c5cb19bD0b3f8dA6c935E2Ca0, 'ethereum', 'wstETH'),
          -- L2/Sidechain tokens
          (0xfdb794692724153d1488ccdbe0c56c252596735f, 'optimism', 'LDO'),
          (0x13ad51ed4f1b7e9dc168d8a00cb3f4ddd85efa60, 'arbitrum', 'LDO'),
          (0x1F32b1c2345538c0c6f582fCB022739c4A194Ebb, 'optimism', 'wstETH'),
          (0x5979D7b546E38E414F7E9822514be443A4800529, 'arbitrum', 'wstETH'),
          (0xc1CBa3fCea344f92D9239c08C0568f6F2F0ee452, 'base', 'wstETH'),
          (0x703b52F2b28fEbcB60E1372858AF5b18849FE867, 'zksync', 'wstETH'),
          (0x26c5e01524d2E6280A48F2c50fF6De7e52E9611C, 'bnb', 'wstETH'),
          (0xB5beDd42000b71FddE22D3eE8a79Bd49A568fC8F, 'linea', 'wstETH'),
          (0x458ed78EB972a369799fb278c0243b25e5242A83, 'mantle', 'wstETH'),
          (0xf610A9dfB7C89644979b4A0f27063E9e7d7Cda32, 'scroll', 'wstETH')
          ) as tokens(address, chain, symbol)
      ),

      filtered_msigs AS (
          select address, chain
          from multisigs_list
          where name in ('LiquidityRewardsMsig', 'LiquidityRewardMngr')
      ),

      filtered_excluded AS (
          select address from multisigs_list
          union all
          select address from intermediate_addresses
      ),

      filtered_transfers AS (
          {% for chain in ['ethereum', 'optimism', 'arbitrum', 'base', 'zksync', 'bnb', 'linea', 'mantle', 'scroll'] %}

          select
              evt_block_time as period,
              evt_tx_hash,
              '{{chain}}' as blockchain,
              cast(value as double) as amount_token,
              contract_address as raw_token
          from {{source('erc20_' ~ chain,'evt_Transfer')}}
          where "from" in (select address from filtered_msigs where chain = '{{chain}}')
          and to not in (select address from filtered_excluded)
          and to != 0x0000000000000000000000000000000000000000
          and contract_address in (select address from tokens where chain = '{{chain}}')

          union all

          select
              evt_block_time as period,
              evt_tx_hash,
              '{{chain}}' as blockchain,
              -cast(value as double) as amount_token,
              contract_address as raw_token
          from {{source('erc20_' ~ chain,'evt_Transfer')}}
          where to in (select address from filtered_msigs where chain = '{{chain}}')
          and "from" not in (select address from filtered_excluded)
          and "from" != 0x0000000000000000000000000000000000000000
          and contract_address in (select address from tokens where chain = '{{chain}}')

          {% if not loop.last %}
          union all
          {% endif %}

          {% endfor %}
      )

      select
          period,
          evt_tx_hash,
          blockchain,
          amount_token,
          case
              when t.symbol in ('LDO') then 0x5A98FcBEA516Cf06857215779Fd812CA3beF1B32
              when t.symbol in ('wstETH') then 0x7f39C581F595B53c5cb19bD0b3f8dA6c935E2Ca0
              else raw_token
          end as token
      from filtered_transfers f
      inner join tokens t on f.raw_token = t.address and f.blockchain = t.chain
      where amount_token != 0