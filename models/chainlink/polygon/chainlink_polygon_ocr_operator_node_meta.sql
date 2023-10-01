{{
  config(
    tags=['dunesql'],
    alias=alias('ocr_operator_node_meta'),
    materialized = 'view',
    post_hook='{{ expose_spells(\'["polygon"]\',
                                "project",
                                "chainlink",
                                \'["linkpool_ryan", "linkpool_jon"]\') }}'
  )
}}

{% set a01node = '01Node' %}
{% set alphachain = 'Alphachain' %}
{% set bharvest = 'B Harvest' %}
{% set blocksizecapital = 'BlocksizeCapital' %}
{% set chainlayer = 'Chainlayer' %}
{% set cryptomanufaktur = 'CryptoManufaktur' %}
{% set dmakers = 'dMakers' %}
{% set dextrac = 'DexTrac' %}
{% set fiews = 'Fiews' %}
{% set inotel = 'Inotel' %}
{% set linkforest = 'LinkForest' %}
{% set linkpool = 'LinkPool' %}
{% set linkriver = 'LinkRiver' %}
{% set matrixedlink = 'Matrixed.Link' %}
{% set newroad = 'Newroad Network' %}
{% set piertwo = 'Pier Two' %}
{% set simplyvc = 'Simply VC' %}
{% set snzpool = 'SNZPool' %}
{% set stakingfacilities = 'Staking Facilities' %}
{% set validationcloud = 'Validation Cloud' %}
{% set ztake = 'Ztake.org' %}

SELECT node_address, operator_name FROM (VALUES
  (0xe0Ed2A6CAd84df5191Fe337e7Dc9685d03bA3eD0, '{{a01node}}'),
  (0x8867ca28d5dD0E3eD9bc86f889322395715b5A27, '{{a01node}}'),
  (0xC2a5c4518849E85C424993f16AbA347165b734B7, '{{a01node}}'),
  (0x777225197088C54997Ff8904eBF01382825def85, '{{a01node}}'),
  (0xffe2613a53c1222C295C6Df941afa3eC63311B0D, '{{a01node}}'),
  (0xED5cBf90D90eCcF2a846a1DA6D966A4B7E0A3269, '{{alphachain}}'),
  (0x51FAfb35f31C434066267fc86EA24D8424115d2a, '{{bharvest}}'),
  (0x50D6BDfc451314fB162D7D3322bFB4a005Cf192f, '{{bharvest}}'),
  (0x4Da725FE670aD0610e06378b9B91F4f0a2a74128, '{{blocksizecapital}}'),
  (0x7537cB7b7E8083ff8E68cb5c0cA18553Ab54946f, '{{chainlayer}}'),
  (0xb1A9Fe770D7bD542feD4Ef9b5eA7B936D7786D0E, '{{chainlayer}}'),
  (0x23FF32EE34c4b43daf478cF6205FF3d342b0719b, '{{chainlayer}}'),
  (0xE6c27255Fbb9d3a9718Fb5E2DC313Cd6EbA10b78, '{{cryptomanufaktur}}'),
  (0x51FD7E0b225095A8826686aBf6C45fB739d2Bb7E, '{{cryptomanufaktur}}'),
  (0x1516288E09975CC53c04505380dc81B13142C91d, '{{cryptomanufaktur}}'),
  (0x229306CB192f2cf1edC712eAA16006fBd5F9B008, '{{cryptomanufaktur}}'),
  (0x21148F81D302442c34D39cB65B82f5e7138F9bE6, '{{dmakers}}'),
  (0xd588b2470D0E78A170383148ae83327338e3c61A, '{{dmakers}}'),
  (0x7BF377f69Da0E46Da1502D5F2bcf9fB00c3B610b, '{{dextrac}}'),
  (0x5a0aAF39C78939eda540f8D50C2F5eF5A087E7AA, '{{dextrac}}'),
  (0xaEf8567821859bF21BaB20Ae1A1Afec8C89Ed2bb, '{{dextrac}}'),
  (0x550365027554bD20D750f9361e460C7428ffBd75, '{{fiews}}'),
  (0xFCE3E6b1739812cdDa335964f281E9A0472B6047, '{{fiews}}'),
  (0xC1DCB8AC71bd771D37dF807F34323A005A515326, '{{fiews}}'),
  (0x52Add4435c81a4e0fB2eC494966863e48BF9302E, '{{inotel}}'),
  (0x9DF75B14905FCe7bf38add3021aC6a48Ed8569C8, '{{inotel}}'),
  (0x8f3f684efA5cc45DFD739cd9efdcaC79d27fbe4b, '{{inotel}}'),
  (0x250ABd1D4EBC8e70a4981677D5525f827660bDE4, '{{linkforest}}'),
  (0xd9f89292a21941826b62460009d9c634c4fA0069, '{{linkforest}}'),
  (0x2D4799D475c9da5Da53013cC284F34D2424A8a28, '{{linkforest}}'),
  (0xa1ab1c841898Fe94900d00d9312ba954e4F81501, '{{linkpool}}'),
  (0x9F9922d4bBa463EfBBcF8563282723d98587f7eb, '{{linkpool}}'),
  (0xf03b7095B089A4e601fB13F2BF6af518eb199a0b, '{{linkpool}}'),
  (0x8eD47843e5030b6F06e6F204Fcf2725378BB837a, '{{linkriver}}'),
  (0xf5EEc17396c5e8A0047ee169d74D3c1066e6908B, '{{linkriver}}'),
  (0x0517395146AB8b43Fa3f8940A57d03177710E278, '{{linkriver}}'),
  (0xd0fF3C55A27c930069Cb4EFA32921B89792CA8CC, '{{linkriver}}'),
  (0x5543FF441d3B0fCce59Aa08eb52f15d27294AF21, '{{matrixedlink}}'),
  (0x983D0e1d23D3109D15e833fB800A51ba154DEfD3, '{{newroad}}'),
  (0x875fc6a5Ff9090435EE197717F7eeD5a05d747e5, '{{newroad}}'),
  (0xd0A8Cb58efcee1CAeE48F3c357074862cA8210dc, '{{piertwo}}'),
  (0x5c51C26bfE38a58c89c78142D20aA538e2D45DF5, '{{piertwo}}'),
  (0x205Ad86aeA1F7A3B035F9fcd38B359Ba40f3EBb3, '{{piertwo}}'),
  (0x777Ad55EFc465052d6A4AB7bc75B6A15175bB399, '{{simplyvc}}'),
  (0x7d2264a1203c625001fe56011240794228CdB346, '{{simplyvc}}'),
  (0x313b80977344FA0FBA3912B710072E3eDd9faA18, '{{simplyvc}}'),
  (0x84A611B71254F5fCCb1E5a619aD723CAD8a03638, '{{snzpool}}'),
  (0xf23971dE39a087cFf61EB54f77A1951983F90723, '{{snzpool}}'),
  (0x7ba865F70E32C9f46f67E33FE06139c8C31a2fAd, '{{stakingfacilities}}'),
  (0x4e791FEC7329738Fb7D3a8caEf80D1201Bb12C14, '{{stakingfacilities}}'),
  (0x3Dd12EB5aE0F1A106fB358C8B99830ab5690a7a2, '{{validationcloud}}'),
  (0xC1aaF3D6e0189C4f6D5CF35514328e6F747a2472, '{{validationcloud}}'),
  (0xcDee224d35860622A61F59D06daFe76d93f8db7c, '{{ztake}}'),
  (0x36C3891112B381538b2B7Cb5650da6C89bB48FFF, '{{ztake}}')
) AS tmp_node_meta(node_address, operator_name)
