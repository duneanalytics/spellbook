{{
  config(
    tags=['dunesql'],
    alias=alias('ocr_operator_node_meta'),
    materialized = 'view',
    post_hook='{{ expose_spells(\'["bnb"]\',
                                "project",
                                "chainlink",
                                \'["linkpool_ryan", "linkpool_jon"]\') }}'
  )
}}

{% set a01node = '01Node' %}
{% set alphachain = 'Alpha Chain' %}
{% set blockdaemon = 'Blockdaemon' %}
{% set chainlayer = 'Chainlayer' %}
{% set cosmostation = 'Cosmostation' %}
{% set cryptomanufaktur = 'CryptoManufaktur' %}
{% set dmakers = 'dMakers' %}
{% set dextrac = 'DexTrac' %}
{% set dxfeed = 'dxFeed' %}
{% set easy2stake = 'Easy 2 stake' %}
{% set fiews = 'Fiews' %}
{% set frameworkventures = 'Framework Ventures' %}
{% set inotel = 'Inotel' %}
{% set linkforest = 'LinkForest' %}
{% set linkpool = 'LinkPool' %}
{% set linkriver = 'LinkRiver' %}
{% set onchaintech = 'On-chain Tech' %}
{% set piertwo = 'Pier Two' %}
{% set simplyvc = 'Simply VC' %}
{% set snzpool = 'SNZPool' %}
{% set syncnode = 'SyncNode' %}
{% set tiingo = 'Tiingo' %}
{% set validationcloud = 'Validation Cloud' %}
{% set xbto = 'XBTO' %}
{% set ztake = 'Ztake.org' %}

SELECT node_address, operator_name FROM (VALUES
  (0x39D36fD647a933Fe51670c1eB6b0e6b8b4a69f07, '{{a01node}}'),
  (0xF133B7B7EEb70B1156e7ca6A76C4bbbf1fEE85De, '{{a01node}}'),
  (0x54f177F25600144561610AE7214A6Bf9199217f7, '{{a01node}}'),
  (0x52bDfF084C524148cA4A44524EB88D2b995Ee7C5, '{{a01node}}'),
  (0x561AeB24d224c0ea153ACE03a011DafE703468E6, '{{a01node}}'),
  (0x17721D492b7EFdcf7365e541EdEFDDE552A1360F, '{{alphachain}}'),
  (0xC2dC5B1836F7ad85B62ff8e22F0a766d3E07DAE6, '{{alphachain}}'),
  (0xed3A0ac63d7e48399D05d9a25925e8FCb0Cd98D0, '{{blockdaemon}}'),
  (0x428986fbc3D27AC7D6b0cf4cfB133b3e0788B825, '{{blockdaemon}}'),
  (0xAd28531021eB793874E8Dfb84b9e7b862C9e0e53, '{{chainlayer}}'),
  (0x5AAbed10758A5B4b76Ba29F70ED77a967cc1eF2F, '{{chainlayer}}'),
  (0xd46f50ba5bD6e4a2AB3FAd8207FCd94c7ba0DEa7, '{{cosmostation}}'),
  (0xc3f5e64C83f0aE4e8dF53553f8C2DbAcBB89Ff3C, '{{cosmostation}}'),
  (0xbe863D7Fd1C2fD020047498fD4EFdd915fc39053, '{{cryptomanufaktur}}'),
  (0x19588eFd97DD4a5D412352047334Ed6Fdf1b8eb9, '{{dmakers}}'),
  (0x8b18a1A5BF514AA94298804fb4D839Bb51B85Da6, '{{dmakers}}'),
  (0xa53bdb1522a58dEe57B89e0579C13B15825B8D77, '{{dextrac}}'),
  (0x3c5a0cb6433Cc3B8c492961Dd4dEFb5a94465a67, '{{dextrac}}'),
  (0xfB6C19672B2C929333A5c2eC7B768c671FA5D12F, '{{dextrac}}'),
  (0x3446f7897f9f0B376a8E60984CC3dF97C3c9c292, '{{dxfeed}}'),
  (0x0693e34cf4f26076677B3Be609d5cc95955DF74d, '{{dxfeed}}'),
  (0xA1e96Ecaab9d97Bd17583966963C486cf856B639, '{{easy2stake}}'),
  (0x7a8cB388CEf668201aE8d213f87227007D39cC9C, '{{easy2stake}}'),
  (0xE6E6D6b5Fb5688e41aaB43686c87ea24Ef4b76A8, '{{fiews}}'),
  (0x981816992ca910B8D00d88db0217b07c199E995a, '{{fiews}}'),
  (0x6Ad15e7cC3456858544cBe885d20c54B44c881e8, '{{fiews}}'),
  (0xC5aaf1525Dd8E1A5a01273C3d59C47426F426756, '{{frameworkventures}}'),
  (0x74855Fa4418449D15380A3e289D417Ee3200550b, '{{frameworkventures}}'),
  (0x5901B2B3B48e5310605fc1bb4a51B1001680a05B, '{{inotel}}'),
  (0xf0E4ec934D377319C44a0A1417682508Fc52942F, '{{inotel}}'),
  (0x58C34A3a7E4b9C61e802deE3d31C834705c01978, '{{inotel}}'),
  (0xa6b40a7Bcc64E813C22561c921B81bc7A6b43eF9, '{{linkforest}}'),
  (0xDa1cC1c3Fb19E19eb055bd028EEa7d0EC6EE9AC9, '{{linkforest}}'),
  (0x42223207FA6ffA0795219EA658876E89E40cAd02, '{{linkforest}}'),
  (0x37Fc26312b831f7efb494cDB192c9aE91Fd27597, '{{linkpool}}'),
  (0xe4e2582d95cB7D8F5B3E746C44d26f8DC1265cAE, '{{linkpool}}'),
  (0x3B5398B508a26b43822456b0D3Ad78B649011dA6, '{{linkpool}}'),
  (0xEBbc95fB63ad3e0D66169297c029BCd8e86fdc71, '{{linkriver}}'),
  (0x05870Ef2194ea1dfa246Ff464AA53fDe8544a9A5, '{{linkriver}}'),
  (0xD46a5604CA9fEe830e291248701F616f53Bf174B, '{{onchaintech}}'),
  (0x3A86052b4926CB0aeCB5d155Ef2389243dB3c22c, '{{onchaintech}}'),
  (0xB35F4988074F14dC1F1d27062346D6123F46A41B, '{{piertwo}}'),
  (0xDC31322087d6B250C0264FEebF34F6F7BA6D5A21, '{{piertwo}}'),
  (0x6D9df15c54B4654De0263e82Fd92C577FeD47e64, '{{piertwo}}'),
  (0xBF44AcA665062a696b2d4c72663CFefD87769A10, '{{simplyvc}}'),
  (0xaDb83Abbf7A8987AfB76DB33Ed2855A07f5497C7, '{{simplyvc}}'),
  (0x2f53d676770Cf2Ab6A094109132b70f9ab282C12, '{{simplyvc}}'),
  (0x573352F3FD171FB103Fd9107D04D5025660f1B8a, '{{snzpool}}'),
  (0x7CDDCB63bA13edd23Ed948ea0D25Fa6ed0683945, '{{snzpool}}'),
  (0x5ce038429e4d6025F94Ae271759d63bcfdCCb6cC, '{{syncnode}}'),
  (0xaE7e8B829f1aE0c2EA04a68041B1eEBB4c527fdc, '{{syncnode}}'),
  (0xfD9fd7dcA91fB4C17f2c389C318911162cf623A6, '{{tiingo}}'),
  (0x3866D3B9a156F8ecdADF7Eb5b3f0362A13d1c51E, '{{tiingo}}'),
  (0x3032cE567c21329fC95B150022d50753ecCB4F1d, '{{validationcloud}}'),
  (0x50fEa67C2C3C53BDa421e6eA1Def30224559b293, '{{validationcloud}}'),
  (0x4D447f5479DF06Bf630bf836237352AfDB7680B0, '{{xbto}}'),
  (0x6b6b1b0F7Cf88Ba887B9a67E091Cc6C552e473b1, '{{xbto}}'),
  (0xC7B702ee7C5b63BD5fF534FaAA82320CaB2B2c0F, '{{ztake}}'),
  (0x4E21fc375a0567A3Ce4F76a05add6CDbd6C61014, '{{ztake}}')
) AS tmp_node_meta(node_address, operator_name)
