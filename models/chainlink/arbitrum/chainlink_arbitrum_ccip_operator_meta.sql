{{
  config(
    alias='ccip_operator_meta',
	materialized = 'view'
  )
}}

SELECT
   'arbitrum' as blockchain,
   node_address,
   operator_name
FROM (values
  (0x1e181f7ac976208D448546C99ffDb69CDF7c513D, 'cryptomanufaktur'),
  (0x7bA784D7ea6369781552058ed9c81bb193E52a1b, 'dextrac'),
  (0x2A47f18ED6B4BBeB6106c96e42C912b401495e15, 'everstake'),
  (0x3Ac6ACCbC8FE6D087A038e1EE40369BA85c42BEC, 'fiews'),
  (0xcbed1c2B44a9C2F2F972295E2B803b94A8fF5595, 'kytzu'),
  (0xee38FB85C6de951F00595e466D606f3A6909F51f, 'linkforest'),
  (0x3605e81c7976Ec485C5f406C2e315AA2a7C7B72b, 'linkpool'),
  (0xb5FD1c35f08F798A173CCeBFC2dbBA24960F8634, 'linkriver'),
  (0xA12a88F1A0aC5b794BA1d012Fe37D69BB4A3b90b, 'newroad'),
  (0xCbde8d712893dE5A5f1C0F069164BE261b1f8E5e, 'northwestnodes'),
  (0x5c5586A7fCCcAfF6DddFe387b6DcBA84AA5fC019, 'omniscience'),
  (0x5fBBC8da114f426691eDf2835Dff527D5D626d0D, 'p2porg'),
  (0xA4c2Cf0286bC126F252cA549f0e48Defb74839ac, 'piertwo'),
  (0x186cF2112481163712404c8820C4B268951D5eeC, 'prophet'),
  (0xeE48705b37a6a9E6261646fFd54b3CDb95984833, 'syncnode'),
  (0xd499Ec90377a7F797392bD4499eB2464CCF8d793, 'validationcloud'),
  (0xC371Cf8701104B15C7ae4D69a4abb184016BDb6B, 'wetez')
) a (node_address, operator_name)
