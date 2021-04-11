CREATE OR REPLACE VIEW index.view_indices (symbol, project, token_address, asset_pool_address, status) AS VALUES
('ypie'::text,  'pieDAO'    ::text, '\x17525e4f4af59fbc29551bc4ece6ab60ed49ce31'::bytea,    '\x17525e4f4af59fbc29551bc4ece6ab60ed49ce31'::bytea, 'wip'::text),
('BCP'::text,   'pieDAO'    ::text, '\xe4f726adc8e89c6a6017f01eada77865db22da14'::bytea,    '\x25af1F2c3772d6F19Aa6615571203757365D29C6'::bytea, 'wip'::text),
('DEFI++'::text,'pieDAO'    ::text, '\x8d1ce361eb68e9e05573443c407d4a3bed23b033'::bytea,    '\xd485e6a0389A42D75f4b00EcE91fc02340B73938'::bytea, 'wip'::text),
('DEFI+L'::text,'pieDAO'    ::text, '\x78F225869c08d478c34e5f645d07A87d3fe8eb78'::bytea,    '\x0e5C1813587088378787E7DD6C9cb4Cb01a0Ea18'::bytea, 'ready'::text),
('DEFI+S'::text,'pieDAO'    ::text, '\xad6a626ae2b43dcb1b39430ce496d2fa0365ba9c'::bytea,    '\x94743cfAa3FDC62e9693572314B5ee377EBa5d11'::bytea, 'ready'::text),
('BTC++'::text, 'pieDAO'    ::text, '\x0327112423f3a68efdf1fcf402f6c5cb9f7c33fd'::bytea,    '\x9891832633a83634765952b051bc7feF36714A46'::bytea, 'ready'::text),
('USD++'::text, 'pieDAO'    ::text, '\x9A48BD0EC040ea4f1D3147C025cd4076A2e71e3e'::bytea,    '\x9A48BD0EC040ea4f1D3147C025cd4076A2e71e3e'::bytea, 'ready'::text),
('ASSY'::text,  'PowerPool' ::text, '\xfa2562da1bba7b954f26c74725df51fb62646313'::bytea,    '\xfa2562da1bba7b954f26c74725df51fb62646313'::bytea, 'ready'::text),
('PIPT'::text,  'PowerPool' ::text, '\x26607aC599266b21d13c7aCF7942c7701a8b699c'::bytea,    '\x26607aC599266b21d13c7aCF7942c7701a8b699c'::bytea, 'ready'::text),
('YETI'::text,  'PowerPool' ::text, '\xb4bebD34f6DaaFd808f73De0d10235a92Fbb6c3D'::bytea,    '\xb4bebD34f6DaaFd808f73De0d10235a92Fbb6c3D'::bytea, 'ready'::text),
('DPI'::text,   'IndexCoop' ::text, '\x1494CA1F11D487c2bBe4543E90080AeBa4BA3C2b'::bytea,    '\x1494CA1F11D487c2bBe4543E90080AeBa4BA3C2b'::bytea, 'ready'::text),
('CGI'::text,   'IndexCoop' ::text, '\xada0a1202462085999652dc5310a7a9e2bf3ed42'::bytea,    '\xada0a1202462085999652dc5310a7a9e2bf3ed42'::bytea, 'ready'::text),
('MVI'::text,   'IndexCoop' ::text, '\x72e364f2abdc788b7e918bc238b21f109cd634d7'::bytea,    '\x72e364f2abdc788b7e918bc238b21f109cd634d7'::bytea, 'ready'::text),
('DEFI5'::text, 'Indexed'   ::text, '\xfa6de2697D59E88Ed7Fc4dFE5A33daC43565ea41'::bytea,    '\xfa6de2697D59E88Ed7Fc4dFE5A33daC43565ea41'::bytea, 'ready'::text),
('ORCL5'::text, 'Indexed'   ::text, '\xd6cb2adf47655b1babddc214d79257348cbc39a7'::bytea,    '\xd6cb2adf47655b1babddc214d79257348cbc39a7'::bytea, 'ready'::text),
('CC10'::text,  'Indexed'   ::text, '\x17ac188e09a7890a1844e5e65471fe8b0ccfadf3'::bytea,    '\x17ac188e09a7890a1844e5e65471fe8b0ccfadf3'::bytea, 'ready'::text),
('DEGEN'::text,  'Indexed'   ::text, '\x126c121f99e1E211dF2e5f8De2d96Fa36647c855'::bytea,    '\x126c121f99e1E211dF2e5f8De2d96Fa36647c855'::bytea, 'ready'::text)
;
