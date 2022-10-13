{{ config( alias='test')}}

SELECT hex, hex2dec(hex)
  FROM (
    VALUES ('0x346f95b1dc5ffcf194bd33017b0e857e6d755d6c000000000000500000000019')
    )
  AS temp_table (hex)
