{{ config(
      schema = 'tokens_ethereum'
      , alias = 'rwa'
      , tags=['static']
      , post_hook='{{ expose_spells(\'["ethereum"]\',
                                  "sector",
                                  "tokens_ethereum",
                                  \'["synthquest"]\') }}'
      , unique_key = ['contract_address']
  )
}}

SELECT blockchain, contract_address, backing, symbol, decimals, name
FROM (VALUES

    ('ethereum', 0x68749665FF8D2d112Fa859AA293F07A622782F38, 'Gold-backed', 'XAUT', 6, 'Tether'),
    ('ethereum', 0x45804880de22913dafe09f4980848ece6ecbaf78, 'Gold-backed', 'PAXG', 18, 'Paxos'),
    ('ethereum', 0x2103e845c5e135493bb6c2a4f0b8651956ea8682, 'Gold-backed', 'XAUM', 18, 'Matrixdock'),
    ('ethereum', 0x136471a34f6ef19fe571effc1ca711fdb8e49f2b, 'Treasury-backed', 'USYC', 6, 'Hashnote'),
    ('ethereum', 0x1b19c19393e2d034d8ff31ff34c81252fcbbee92, 'Treasury-backed', 'OUSG', 18, 'Ondo'),
    ('ethereum', 0x7712c34205737192402172409a8F7ccef8aA2AEc, 'Treasury-backed', 'BUIDL', 6, 'Blackrock'),
    ('ethereum', 0x96F6eF951840721AdBF46Ac996b59E0235CB985C, 'Treasury-backed', 'USDY', 18, 'Ondo'),
    ('ethereum', 0x43415eB6ff9DB7E26A15b704e7A3eDCe97d31C4e, 'Treasury-backed', 'USTB', 6, 'Superstate'),
    ('ethereum', 0xdd50C053C096CB04A3e3362E2b622529EC5f2e8a, 'Treasury-backed', 'TBILL', 6, 'Open Eden'),
    ('ethereum', 0xe4880249745eAc5F1eD9d8F7DF844792D560e750, 'Treasury-backed', 'USTBL', 5, 'Spiko'),
    ('ethereum', 0x8c213ee79581Ff4984583C6a801e5263418C4b86, 'Treasury-backed', 'LTF', 6, 'Centrifuge'),
    ('ethereum', 0xa0769f7A8fC65e47dE93797b4e21C073c117Fc80, 'Global-Bond-backed', 'EUTBL', 5, 'Spiko'),
    ('ethereum', 0x3f95AA88dDbB7D9D484aa3D482bf0a80009c52c9, 'Global-Bond-backed', 'bERNX', 18, 'Backed'),
    ('ethereum', 0x2F123cF3F37CE3328CC9B5b8415f9EC5109b45e7, 'Global-Bond-backed', 'bC3M', 18, 'Backed'),
    ('ethereum', 0x3Ee320c9F73a84D1717557af00695A34b26d1F1d, 'Global-Bond-backed', 'XEVT', 6, 'OpenTrade'),
    ('ethereum', 0xe4A6f23Fb9e00Fca037Aa0EA0a6954dE0a6C53bF, 'Gold-backed', 'TXAU', 18, 'Aurus'),
    ('ethereum', 0x34ABce75D2f8f33940c721dCA0f562617787bfF3, 'Silver-backed', 'TXAG', 18, 'Aurus'),
    ('ethereum', 0x6d57B2E05F26C26b549231c866bdd39779e4a488, 'Gold-backed', 'VNXAU', 18, 'VNX'),
    ('ethereum', 0x19b22DbADc298c359A1D1b59e35f352A2b40E33c, 'Platinum-backed', 'TXPT', 18, 'Aurus'),
    ('ethereum', 0xC139190F447e929f090Edeb554D95AbB8b18aC1C, 'Treasury-backed', 'USDtb', 18, 'Ethena'),
    ('ethereum', 0x35D8949372D46B7a3D5A56006AE77B215fc69bC0, 'Treasury-backed', 'USD0++', 18, 'Usual')
    


    ) AS temp_table (blockchain, contract_address, backing, symbol, decimals, name)
