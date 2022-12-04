{{ config( alias='rebase', tags=['static'])}}

SELECT
  LOWER(contract_address),
  symbol
FROM (
  VALUES
    ('0xE5bA47fD94CB645ba4119222e34fB33F59C7CD90', 'SAFUU'),
    ('0x0DFCb45EAE071B3b846E220560Bbcdd958414d78', 'LIBERO'),
    ('0x4e3cABD3AD77420FF9031d19899594041C420aeE', 'TITANO'),
    ('0x1b239abe619e74232c827fbe5e49a4c072bd869d', 'GYRO'),
    ('0x19e6bfc1a6e4b042fb20531244d47e252445df01', 'TEM'),
    ('0x8ac9dc3358a2db19fdd57f433ff45d1fc357afb3', 'NMS'),
    ('0x63290fc683d11ea077aba09596ff7387d49df912', 'RAM'),
    ('0x8fba8c1f92210f24fb277b588541ac1952e1aac8', 'GRX'),
    ('0x4e141769366634d9c4e498257fa7ec204d22b634', 'XEUS'),
    ('0x9505dbd77dacd1f6c89f101b98522d4b871d88c5', 'LOVE')
) AS temp_table (contract_address, symbol)