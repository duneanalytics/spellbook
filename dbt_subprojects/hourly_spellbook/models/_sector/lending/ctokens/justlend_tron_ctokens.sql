{{
  config(
    schema = 'justlend_tron',
    alias = 'ctokens',
    post_hook = '{{ hide_spells() }}'
  )
}}

select
  asset_symbol,
  ctoken_address,
  asset_address,
  decimals_mantissa
from (
  values
    ('USD1', 0x0dd3f1b2e5781688d5cf8c350050c5c236535642, 0x91bed8e784249c91611e61c4585c40e21fd0ace2, 1e18),
    ('WSTUSDT', 0x22163f4926c1b7e1d22dbbc76fbef7f54d364d87, 0x4a7832a4c51dfbc423cf562cfcd534b88ffd4624, 1e18),
    ('WTRX', 0x2c7c9963111905d29eb8da37d28b0f53a7bb5c28, 0x891cdb91d149f23b1a45d9c5ca78a88d0cb44c18, 1e6),
    ('NFT', 0x40262ab2a177fb3fc6d2709a816db3b1a10bc78e, 0x3dfe637b2b9ae4190a458b5f3efc1969afe27819, 1e6),
    ('SUNOLD', 0x4434beca3ac7d96e2b4eef1974cf9bddcb7a328b, 0x6b5151320359ec18b08607c70a3b7439af626aa3, 1e18),
    ('STRX', 0x5c78c77bbad44c3ebd2088e6b7b5d5f01bb0a8f5, 0xc64e69acde1c7b16c2a3efcdbbdaa96c3644c2b3, 1e18),
    ('USDD', 0x65c9fede72ba73cd1b0dca2a974c070153dc6fcb, 0xe91a7411e56ce79e83570570f49b9fc35b7727c5, 1e18),
    ('USDJ', 0x6ef7c4870977c6a2543b0e8cf4f659af883c96dc, 0x834295921a488d9d42b4b3021ed1a3c39fb0f03e, 1e18),
    ('BUSDOLD', 0x71169cc742905196d4ae1b6330e5366b5459a3dc, 0x83c91bfde3e6d130e286a3722f171ae49fb25047, 1e18),
    ('BTCT', 0x7513102bc947f138b88f4bcc6acf73acb8d4d087, 0x84716914c0fdf7110a44030d04d0c4923504d9cc, 1e8),
    ('USDCOLD', 0x88bb336c70a33fe2506240a19826c2ad487ae6d8, 0x3487b63d30b5b2c87fb7ffa8bcfade38eaac1abe, 1e6),
    ('SUN', 0x94a7a1e585a77e2edfd834005be9f545fe1f3c97, 0xb4a428ab7092c2f1395f376ce297033b3bb446c1, 1e18),
    ('WETH', 0xa60befaf69b18090b762a83177f09831773967ea, 0x53908308f4aa220fb10d778b5d1b34489cd6edfc, 1e18),
    ('WIN', 0xac456571ac5a383b77c65d9fdcd66d8ac2ed62bb, 0x74472e7d35395a6b5add427eecb7f4b62ad2b071, 1e6),
    ('TUSD', 0xb5b1a24c3067f985ac2da2f6bce0fa685bf8ec06, 0xcebde71077b830b958c8da17bcddeeb85d0bcf25, 1e18),
    ('WBTT', 0xcba95c5726a36046503570496e2c5a457ed7c008, 0x6a6337ae47a09aea0bbd4faeb23ca94349c7b774, 1e6),
    ('BTT', 0xcc1d948f9397db4c047de179eb74ca013529022a, 0x032017411f4663b317fe77c257d28d5cd1b26e3d, 1e18),
    ('ETHB', 0xddcbbcb2f17db034fc970fbd87ffa7da51bebbfc, 0xa7a572f6d8b4ca291b9353cf26580abed74f3e31, 1e18),
    ('JST', 0xe03473f8720297d9bf887f2d7e4ec2efc70c3460, 0x18fd0626daf3af02389aef3ed87db9c33f638ffa, 1e18),
    ('USDT', 0xea09611b57e89d67fbb33a516eb90508ca95a3e5, 0xa614f803b6fd780986a42c78ec9c7f77e6ded13c, 1e6)
) as x (asset_symbol, ctoken_address, asset_address, decimals_mantissa)
