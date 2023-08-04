{{ config( alias = alias('rebase'), tags=['static'])}}

SELECT contract_address, symbol
  FROM (VALUES
          ('0x798d1be841a82a273720ce31c822c61a67a601c3'
        , '$DIGG')
        , ('0xd46ba6d942050d489dbd938a2c909a5d5039a161'
        , '$AMPL')
        , ('0x470ebf5f030ed85fc1ed4c2d36b9dd02e77cf1b7'
        , '$TEMPLE')
        , ('0xc0d4ceb216b3ba9c3701b291766fdcba977cec3a'
        , '$BTRFLY')
        , ('0x2f6081e3552b1c86ce4479b80062a1dda8ef23e3'
        , '$USDX')
        , ('0x07150e919b4de5fd6a63de1f9384828396f25fdc'
        , '$BASE')
        , ('0x8a14897ea5f668f36671678593fae44ae23b39fb'
        , '$3DOG')
        , ('0x67c597624b17b16fb77959217360b7cd18284253'
        , '$MARK')
        , ('0x21ad647b8f4fe333212e735bfc1f36b4941e6ad2'
        , '$SQUID')
        , ('0x39795344cbcc76cc3fb94b9d1b15c23c2070c66d'
        , '$SHARE')
        , ('0x1c7bbadc81e18f7177a95eb1593e5f5f35861b10'
        , '$AUSCM')
        , ('0x68a118ef45063051eac49c7e647ce5ace48a68a5'
        , '$BASED')
        , ('0xf911a7ec46a2c6fa49193212fe4a2a9b95851c27'
        , '$XAMP')
        -- Olympus v1 (OHM)
        , ('0x383518188c0c6d7730d91b2c03a03c837814a899'
        , '$OHM')
        -- Olympus (OHM)
        , ('0x64aa3364f17a4d01c6f1751fd97c2bd3d7e7f1d5'
        , '$OHM')) AS temp_table (contract_address, symbol)