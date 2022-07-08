 {{ config( alias='aggregators') }}

SELECT
  contract_address,
  name
FROM
  (
    VALUES
      (
        '0x0a267cf51ef038fc00e71801f5a524aec06e4f07',
        'GenieSwap'
      ),
      (
        '0x0000000031f7382a812c64b604da4Fc520afef4b',
        'Gem'
      ),
      (
        '0xf24629fbb477e10f2cf331c2b7452d8596b5c7a5',
        'Gem'
      ),
      (
        '0x83c8f28c26bf6aaca652df1dbbe0e1b56f8baba2',
        'Gem'
      )
  ) AS temp_table (contract_address, name)