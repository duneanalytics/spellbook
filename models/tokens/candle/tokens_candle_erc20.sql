{{ config( alias='erc20')}}

SELECT contract_address, symbol, decimals
FROM (VALUES
          ('0x85FA00f55492B0437b3925381fAaf0E024747627', 'WCNDL', 18)
          ,('0x5c17C48F127D6aE5794b2404F1F8A5CeED419eDf', 'ZED', 6)
          ,('0xa018034190943D6c8E10218d9F8E8Af491272411', 'SHIBA', 18)
          ,('0x95A0A7953F9292838C0614D690005D5c716E718E', 'USDC', 6)
          ,('0xad43669cbAC863e33449d423261E525de8da0Ff4', 'DAI', 18)
          ,('0xb750990F953B36F806d0327678eCFB4eEFd16979', 'WETH', 18)
          ,('0xB307B497aF3fDDF68c27ce0356876dC6b88602D7', 'UNI-V3', 18)
          ,('0x659E0345Ef83b2c25439B2aEAc71dfE6a4B71a27', 'ALICE', 18)
     ) AS temp_table (contract_address, symbol, decimals)
