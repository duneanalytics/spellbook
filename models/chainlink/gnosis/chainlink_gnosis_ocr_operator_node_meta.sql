{{
  config(
    tags=['dunesql'],
    alias=alias('ocr_operator_node_meta'),
    materialized = 'view',
    post_hook='{{ expose_spells(\'["gnosis"]\',
                                "project",
                                "chainlink",
                                \'["linkpool_ryan", "linkpool_jon"]\') }}'
  )
}}

{% set a01node = '01Node' %}
{% set blockdaemon = 'Blockdaemon' %}
{% set chainlayer = 'Chainlayer' %}
{% set dmakers = 'dMakers' %}
{% set dextrac = 'DexTrac' %}
{% set fiews = 'Fiews' %}
{% set inotel = 'Inotel' %}
{% set linkpool = 'LinkPool' %}
{% set piertwo = 'Pier Two' %}
{% set simplyvc = 'Simply VC' %}
{% set snzpool = 'SNZPool' %}
{% set ztake = 'Ztake.org' %}

SELECT node_address, operator_name FROM (VALUES
  (0x4FB3376d5bF6AD8947FCf737A2C7e793CC245fDa, '{{a01node}}'),
  (0xb5f0EfcB8a426ffAc5F92BbE6fCd6b4255b4E1aa, '{{a01node}}'),
  (0x274C2FB716155e2E2a537a08205D4938199Fb32e, '{{a01node}}'),
  (0x3036c926cCc3096beCF584E7523A1a57fdebba77, '{{blockdaemon}}'),
  (0x577b17c9A02B7A360e0cf945D623D6C1ace6074c, '{{blockdaemon}}'),
  (0xE769724C9295458b026875ff964aCCb964B13D50, '{{chainlayer}}'),
  (0x582067f4b986775EAF8949Cc6370B8b75B836572, '{{chainlayer}}'),
  (0x88f578278381B8eFC04558D0017D06E818170EBb, '{{dmakers}}'),
  (0xCd906f0467eFaB9f0A59DF479e873B4A5320D1Be, '{{dmakers}}'),
  (0x4407E5caC83a1397741fEB1ACcAF6C23968180f4, '{{dextrac}}'),
  (0x4562845F37813A201b9DDB52E57A902659b7AE6A, '{{dextrac}}'),
  (0x16F5c3Dc347A5814B81553c7725D4ed9214C8A3c, '{{fiews}}'),
  (0x0184Ee351E270fb0942C5Cb66f0Ff37bF4d37D3e, '{{fiews}}'),
  (0x45a3983bD8b0D7a77Bb00F1C86AD794515D96f34, '{{inotel}}'),
  (0x2607E6F021922A5483D64935F87e15EA797FE8d4, '{{inotel}}'),
  (0x11eB6a69A56DF3a89b99c4b1484691Af4AB0c508, '{{linkpool}}'),
  (0x6D04B8dB14B5a891f9aA1e32C093584815291551, '{{linkpool}}'),
  (0x82e2dE20848E58CDDfea53Ad56cAB0471AE8BDcF, '{{piertwo}}'),
  (0xFc7C442154C04C31203e2b94B96fd57C44ac003D, '{{piertwo}}'),
  (0xC8a9A5b3517071f582b50b18633E522F6F4f38f5, '{{simplyvc}}'),
  (0x12d61a95CF55e18D267C2F1AA67d8e42ae1368f8, '{{simplyvc}}'),
  (0x9d3A9b7cadc14dA9cB57E9e9E83eD13ea1D36d40, '{{snzpool}}'),
  (0x5C73E956A8B4cE22D898AB18eeA2C3921Edc5fED, '{{snzpool}}'),
  (0x32cbe102400A1B8a430232d7cCAD3E75AB73C1F2, '{{ztake}}')
) AS tmp_node_meta(node_address, operator_name)
