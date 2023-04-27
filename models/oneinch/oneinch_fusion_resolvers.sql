{{ config(materialized='view', alias='fusion_resolvers') }}

with resolvers as (
    select * 
    from (
        values
        ('1inch Labs', '0x55dcad916750c19c4ec69d65ff0317767b36ce90'),
        ('1inch Labs', '0x3169de0e661d684e0d235f19cf72327173e0be11'),
        ('1inch Labs', '0x8acdb3bcc5101b1ba8a5070f003a77a2da376fe8'),
        ('1inch Labs', '0x84d99aa569d93a9ca187d83734c8c4a519c4e9b1'),
        ('1inch Labs', '0xb33839e05ce9fc53236ae325324a27612f4d110d'),

        ('Laertes', '0x9108813f22637385228a1c621c1904bbbc50dc25'),

        ('Arctic Bastion', '0x2eb393fbac8aaa16047d4242033a25486e14f345'),
        ('Arctic Bastion', '0x7636a5bfd763cefec2da9858c459f2a9b0fe8a6c'),
        ('Arctic Bastion', '0xf1b2e1fef70e0383ef29618d02d0dd503ae239ae'),
        ('Arctic Bastion', '0x377a1286ff83df266ff11bede2ef600044f3626b'),
        ('Arctic Bastion', '0xe16e2f35da363a4bd330812e7cffb3f51a97c7d1'),

        ('The Open DAO resolver', '0xcfa62f77920d6383be12c91c71bd403599e1116f'),

        ('Seawise', '0xad7149152a65e6ec97add7b1b1f917dcafcf9b21'),
        ('Seawise', '0xd1742b3c4fbb096990c8950fa635aec75b30781a'),
        ('Seawise', '0xa9ff271ee217dc1c9ce3f7ebf0d6f096842cd82f'),

        ('The T Resolver', '0xc6c7565644ea1893ad29182f7b6961aab7edfed0'),

        ('Resolver 8', '0x69313aec23db7e4e8788b942850202bcb6038734'),

        ('Kinetex Labs Resolver', '0xee230dd7519bc5d0c9899e8704ffdc80560e8509')
    ) as t(
        resolver_name, resolver_address
    )
)

select 
    lower(resolver_address) as resolver_address
    , resolver_name
from resolvers