{{
  config(
    
    alias='price_feeds_oracle_addresses',
    post_hook='{{ expose_spells(\'["fantom"]\',
                                "project",
                                "chainlink",
                                \'["linkpool_ryan","linkpool_jon"]\') }}'
  )
}}

{% set alpaca_usd = 'ALPACA / USD' %}
{% set bifi_usd = 'BIFI / USD' %}
{% set boo_usd = 'BOO / USD' %}
{% set busd_usd = 'BUSD / USD' %}
{% set chf_usd = 'CHF / USD' %}
{% set cream_usd = 'CREAM / USD' %}
{% set crv_usd = 'CRV / USD' %}
{% set cvx_usd = 'CVX / USD' %}
{% set calculated_sftmx_usd = 'Calculated sFTMX / USD' %}
{% set eur_usd = 'EUR / USD' %}
{% set frax_usd = 'FRAX / USD' %}
{% set gmx_usd = 'GMX / USD' %}
{% set link_ftm = 'LINK / FTM' %}
{% set mim_usd = 'MIM / USD' %}
{% set mimatic_usd = 'MIMATIC / USD' %}
{% set ohm_index = 'OHM Index' %}
{% set spell_usd = 'SPELL / USD' %}
{% set wbtc_usd = 'WBTC / USD' %}
{% set yfi_usd = 'YFI / USD' %}

SELECT
   'fantom' as blockchain,
   feed_name,
   CAST(decimals AS BIGINT) as decimals,
   proxy_address,
   aggregator_address
FROM (values
  ('{{alpaca_usd}}', 8, 0x95d3FFf86A754AB81A7c59FcaB1468A2076f8C9b, 0xd867c068534Ad7d3BE0fE4f321AACddCe371DB1A),
  ('{{bifi_usd}}', 8, 0x4F5Cc6a2291c964dEc4C7d6a50c0D89492d4D91B, 0xc7439cd23025a798913a027Fb46bc347021483Db),
  ('{{boo_usd}}', 8, 0xc8C80c17f05930876Ba7c1DD50D9186213496376, 0x755Dc32541B82eecE3F8aad681575f01985062C2),
  ('{{boo_usd}}', 8, 0xc8C80c17f05930876Ba7c1DD50D9186213496376, 0x8173d07C6b085Ae79326Fd6Dd514ab5966c3248c),
  ('{{busd_usd}}', 8, 0xf8f57321c2e3E202394b0c0401FD6392C3e7f465, 0xFD94D015B1a293f027dB73060b8e0F7c9E84DB59),
  ('{{chf_usd}}', 8, 0x4be9c8fb4105380116c03fC2Eeb9eA1e1a109D95, 0xfDB46212df397E25D96F646f9a2647dAEC3E3cCA),
  ('{{cream_usd}}', 8, 0xD2fFcCfA0934caFdA647c5Ff8e7918A10103c01c, 0x2946220288DbBF77dF0030fCecc2a8348CbBE32C),
  ('{{crv_usd}}', 8, 0xa141D7E3B44594cc65142AE5F2C7844Abea66D2B, 0xbfc6236cE03765739Db1393421C0d7675eeD8D7D),
  ('{{cvx_usd}}', 8, 0x1A8d750240Cdf7b671805Eec761e622F13781cEb, 0xE3932dd2b44931C10a3254AdBa01e7E291780CcD),
  ('{{calculated_sftmx_usd}}', 8, 0xb94533460Db5A1d8baf56240896eBB3491E608f7, 0x48a4A030673B0F2Af94AB3b2F8d77abFd903303B),
  ('{{eur_usd}}', 8, 0x3E68e68ea2c3698400465e3104843597690ae0f7, 0x69aE9C103F8F39dF5D35Fc6BFCF346223A71BA48),
  ('{{frax_usd}}', 8, 0xBaC409D670d996Ef852056f6d45eCA41A8D57FbD, 0xc2bD6467d9567Cfaf2783d7DE5F337bf98Fe62C1),
  ('{{gmx_usd}}', 8, 0x8a84D922eF06c1f13a30ddD1304BEf556ffa7552, 0xa15CD3fF5EDF2AE2710C7bEfcF15EEEb53967BC1),
  ('{{link_ftm}}', 18, 0x3FFe75E8EDA86F48e454e6bfb5F74d95C20744f4, 0xB33835712E03ec36c6e4e6Da2207ed7111c2B59d),
  ('{{mim_usd}}', 8, 0x28de48D3291F31F839274B8d82691c77DF1c5ceD, 0x50a0a7C4066336203488c877958A8D7D3ab946FE),
  ('{{mimatic_usd}}', 8, 0x827863222c9C603960dE6FF2c0dD58D457Dcc363, 0x42A70DC2cfCa080Da3a2568a3EC3A51E6E76363F),
  ('{{ohm_index}}', 9, 0xCeC98f20cCb5c19BB42553D70eBC2515E3B33947, 0x0Aaf3EAcc3088691be6921fd33Bad8075590aE85),
  ('{{spell_usd}}', 8, 0x02E48946849e0BFDD7bEa5daa80AF77195C7E24c, 0x421CfF3FF719b5101f9c8Da487445C39838A566c),
  ('{{spell_usd}}', 8, 0x02E48946849e0BFDD7bEa5daa80AF77195C7E24c, 0xF458289502A7D4686f541110083Aa92bFaa86CDe),
  ('{{wbtc_usd}}', 8, 0x9Da678cE7f28aAeC8A578A1e414219049509a552, 0xe67D6eDDFF2d7A09c3070e5f564F35E559E32C17),
  ('{{yfi_usd}}', 8, 0x9B25eC3d6acfF665DfbbFD68B3C1D896E067F0ae, 0x0cEe0aee5C6C0d1f99829E9Debf6F3cE39266160)
) a (feed_name, decimals, proxy_address, aggregator_address)
