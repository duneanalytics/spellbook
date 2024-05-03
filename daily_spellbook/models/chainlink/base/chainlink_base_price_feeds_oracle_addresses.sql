{{
  config(
    alias='price_feeds_oracle_addresses',
    post_hook='{{ expose_spells(\'["base"]\',
                                "project",
                                "chainlink",
                                \'["linkpool_jon"]\') }}'
  )
}}

{% set aero_usd = 'AERO / USD' %}
{% set apt_usd = 'APT / USD' %}
{% set avax_usd = 'AVAX / USD' %}
{% set axl_usd = 'AXL / USD' %}
{% set bnb_usd = 'BNB / USD' %}
{% set btc_usd = 'BTC / USD' %}
{% set cbeth_eth = 'CBETH / ETH' %}
{% set cbeth_usd = 'CBETH / USD' %}
{% set comp_usd = 'COMP / USD' %}
{% set dai_usd = 'DAI / USD' %}
{% set eth_usd = 'ETH / USD' %}
{% set l2_sequencer_uptime_status_feed = 'L2 Sequencer Uptime Status Feed' %}
{% set link_eth = 'LINK / ETH' %}
{% set link_usd = 'LINK / USD' %}
{% set matic_usd = 'MATIC / USD' %}
{% set op_usd = 'OP / USD' %}
{% set reth_eth = 'RETH / ETH' %}
{% set rsr_usd = 'RSR / USD' %}
{% set snx_usd = 'SNX / USD' %}
{% set sol_usd = 'SOL / USD' %}
{% set steth_eth = 'STETH / ETH' %}
{% set stg_usd = 'STG / USD' %}
{% set usdc_usd = 'USDC / USD' %}
{% set usdt_usd = 'USDT / USD' %}
{% set usde_usd = 'USDe / USD' %}
{% set wbtc_usd = 'WBTC / USD' %}
{% set yfi_usd = 'YFI / USD' %}
{% set cbeth_eth_exchange_rate = 'cbETH-ETH Exchange Rate' %}
{% set ezeth_eth = 'ezETH / ETH' %}
{% set susde_usde_exchange_rate = 'sUSDe / USDe Exchange Rate' %}
{% set sfrxeth_frxeth_exchange_rate = 'sfrxETH-frxETH Exchange Rate' %}
{% set woeth_oeth_exchange_rate = 'wOETH / OETH Exchange Rate' %}
{% set weeth_eeth_exchange_rate = 'weETH / eETH Exchange Rate' %}
{% set wsteth_eth_exchange_rate = 'wstETH-ETH Exchange Rate' %}
{% set wsteth_steth_exchange_rate = 'wstETH-stETH Exchange Rate' %}

SELECT
   'base' as blockchain,
   feed_name,
   CAST(decimals AS BIGINT) as decimals,
   proxy_address,
   aggregator_address
FROM (values
  ('{{aero_usd}}', 8, 0x4EC5970fC728C5f65ba413992CD5fF6FD70fcfF0, 0xC18cC9B106A50D945024F0a25EfF16B6dC56D4B9),
  ('{{apt_usd}}', 8, 0x88a98431C25329AA422B21D147c1518b34dD36F4, 0xAa1399A25AB0f9a5464f44963BA77626937D1523),
  ('{{avax_usd}}', 8, 0xE70f2D34Fd04046aaEC26a198A35dD8F2dF5cd92, 0x84efF9466d371ccAB94728e8bdFcd9Bc095D7Ca6),
  ('{{axl_usd}}', 8, 0x676C4C6C31D97A5581D3204C04A8125B350E2F9D, 0x038fa58bd4DA1c938D2783941e657164D497C4B6),
  ('{{bnb_usd}}', 8, 0x4b7836916781CAAfbb7Bd1E5FDd20ED544B453b1, 0xbF477e69a0adF91b6e3d6e70cb67E5D1A27e88e3),
  ('{{btc_usd}}', 8, 0x64c911996D3c6aC71f9b455B1E8E7266BcbD848F, 0x852aE0B1Af1aAeDB0fC4428B4B24420780976ca8),
  ('{{cbeth_eth}}', 18, 0x806b4Ac04501c29769051e42783cF04dCE41440b, 0x08F9654349B33B955133b28e35dbEcCe9950c219),
  ('{{cbeth_usd}}', 8, 0xd7818272B9e248357d13057AAb0B417aF31E817d, 0x330eC3210511cC8f5A87A737A08905092e033AF3),
  ('{{comp_usd}}', 8, 0x9DDa783DE64A9d1A60c49ca761EbE528C35BA428, 0x6228A44Cd0Ec29c3373C9742e4bBAF6f2E536B9A),
  ('{{dai_usd}}', 8, 0x591e79239a7d679378eC8c847e5038150364C78F, 0x21b1E4eA0E9AE2e79932662300eB12A0f90AbE59),
  ('{{eth_usd}}', 8, 0x71041dddad3595F9CEd3DcCFBe3D1F4b0a16Bb70, 0x57d2d46Fc7ff2A7142d479F2f59e1E3F95447077),
  ('{{l2_sequencer_uptime_status_feed}}', 0, 0xBCF85224fc0756B9Fa45aA7892530B47e10b6433, 0x606c6ecBD272E2174F6710b5974F23fE9899602e),
  ('{{link_eth}}', 18, 0xc5E65227fe3385B88468F9A01600017cDC9F3A12, 0x290B97eb30Af8Ed088659D6738e314069d71352b),
  ('{{link_usd}}', 8, 0x17CAb8FE31E32f08326e5E27412894e49B0f9D65, 0x72FC7950A832396720736e7e08D6F74C84C6909a),
  ('{{matic_usd}}', 8, 0x12129aAC52D6B0f0125677D4E1435633E61fD25f, 0x851a369f1c7e3F82a2AE8D75Ee94eaBfd9781805),
  ('{{op_usd}}', 8, 0x3E3A6bD129A63564FE7abde85FA67c3950569060, 0x23e47A253776F1Fce32e5F2D5D342ca5D6Edd226),
  ('{{reth_eth}}', 18, 0xf397bF97280B488cA19ee3093E81C0a77F02e9a5, 0x484Cc23Fee336291E3C8803cF27e16B9BEe68744),
  ('{{rsr_usd}}', 8, 0xAa98aE504658766Dfe11F31c5D95a0bdcABDe0b1, 0xf3764B1fc0Ab831f75D3edd7435ABFE4Af675c9A),
  ('{{snx_usd}}', 8, 0xe3971Ed6F1A5903321479Ef3148B5950c0612075, 0x735326Bcc0479e3F23eD65DC83310d63eBA6250D),
  ('{{sol_usd}}', 8, 0x975043adBb80fc32276CbF9Bbcfd4A601a12462D, 0xEA990BCCb5b4dA5023B6dc88480297405Fd222c3),
  ('{{steth_eth}}', 18, 0xf586d0728a47229e747d824a939000Cf21dEF5A0, 0x79b0e87fF1C40D27a0F941296D70a91cD1553482),
  ('{{stg_usd}}', 8, 0x63Af8341b62E683B87bB540896bF283D96B4D385, 0x6f22C6925b27bCf9713fAE2Ab6f4397549D684b8),
  ('{{usdc_usd}}', 8, 0x7e860098F58bBFC8648a4311b374B1D669a2bc6B, 0x0Ee7145e1370653533e2F2E824424bE2AA95A4Aa),
  ('{{usdt_usd}}', 8, 0xf19d560eB8d2ADf07BD6D13ed03e1D11215721F9, 0xDC2D2fA8E7b824A2c16128446E288280dcB12844),
  ('{{usde_usd}}', 8, 0x790181e93e9F4Eedb5b864860C12e4d2CffFe73B, 0x29a0BF5D5e677d38f7AbBd4d292895a3574796C0),
  ('{{wbtc_usd}}', 8, 0xCCADC697c55bbB68dc5bCdf8d3CBe83CdD4E071E, 0xE186722b9d5C063625C49a4BF6Bb3d669F66A8b5),
  ('{{yfi_usd}}', 8, 0xD40e758b5eC80820B68DFC302fc5Ce1239083548, 0xdB793acA8bE40a123c34300Bb21b02F21F8ef501),
  ('{{cbeth_eth_exchange_rate}}', 18, 0x868a501e68F3D1E89CfC0D22F6b22E8dabce5F04, 0x16f542BC40723DfE8976A334564eF0c3CfD602Fd),
  ('{{ezeth_eth}}', 18, 0x960BDD1dFD20d7c98fa482D793C3dedD73A113a3, 0x00be872906C07d6d7D0eC3968b99C4e3D6Bd552a),
  ('{{susde_usde_exchange_rate}}', 18, 0xdEd37FC1400B8022968441356f771639ad1B23aA, 0x801B6E7d186370EeE854F76481643c22c7d1da99),
  ('{{sfrxeth_frxeth_exchange_rate}}', 18, 0x1Eba1d6941088c8FCE2CbcaC80754C77871aD093, 0x5d427E797C665Ad7413a4e0fF4ceB3E31959C4C5),
  ('{{woeth_oeth_exchange_rate}}', 18, 0xe96EB1EDa83d18cbac224233319FA5071464e1b9, 0x05acfeE2c0b4efbBCe705932239A30613aCE42f2),
  ('{{weeth_eeth_exchange_rate}}', 18, 0x35e9D7001819Ea3B39Da906aE6b06A62cfe2c181, 0x19e6821Ee47a4c23E5971fEBeE29f78C2e514DC8),
  ('{{wsteth_eth_exchange_rate}}', 18, 0xa669E5272E60f78299F4824495cE01a3923f4380, 0x4C83489A62d52eE68a800Dd09410f790A14A5d95),
  ('{{wsteth_steth_exchange_rate}}', 18, 0xB88BAc61a4Ca37C43a3725912B1f472c9A5bc061, 0x04030d2F38Bc799aF9B0AaB5757ADC98000D7DeD)
) a (feed_name, decimals, proxy_address, aggregator_address)
