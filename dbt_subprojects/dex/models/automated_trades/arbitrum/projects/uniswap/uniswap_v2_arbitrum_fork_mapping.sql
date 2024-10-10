-- this should probably live somewhere else, just for testing purposes for now

{{ config(
    schema = 'uniswap_v2_arbitrum',
    alias = 'fork_mapping',
    tags = ['static'],
    unique_key = ['factory_address'])
}}

SELECT factory_address, project_name
FROM
(VALUES
    (0x5c69bee701ef814a2b6a3edd4b1652cb9cc5aa6f, 'uniswap_v2')
    , (0xc0aee478e3658e2610c5f7a4a2e1777ce9e4f2ac, 'sushi')
    , (0xb8900621b03892c2d030e05cb9e01f6474814f6a, 'sweepnflip')
    , (0x115934131916c8b277dd010ee02de363c09d037c, 'shibaswap')
    , (0x9deb29c9a4c7a88a3c0257393b7f3335338d9a9d, 'crodefi')
    , (0x1097053fd2ea711dad45caccc45eff7548fcb362, 'pancakeswap_v2')
    , (0x43ec799eadd63848443e2347c49f5f52e8fe0f6f, 'fraxswap')
    , (0xee3e9e46e34a27dc755a63e2849c9913ee1a06e2, 'verse_dex')
    , (0xc480b33ee5229de3fbdfad1d2dcd3f3bad0c56c6, 'integral_size')
    , (0xcbae5c3f8259181eb7e2309bc4c72fdf02dd56d8, '9inch')
    , (0x4eef5746ed22a2fd368629c1852365bf5dcb79f1, 'convergence')
    , (0x1e895bfe59e3a5103e8b7da3897d1f2391476f3c, 'DooarSwap')
    , (0x5fa0060fcfea35b31f7a5f6025f0ff399b98edf1, 'OrionProtocol')
    , (0x75e48c954594d64ef9613aeef97ad85370f13807, 'sakeswap')
    , (0x696708db871b77355d6c2be7290b27cf0bb9b24b, 'linkswap_v1')
    , (0x52fba58f936833f8b643e881ad308b2e37713a86, 'pepex')
    , (0x25393bb68c89a894b5e20fa3fc3b3b34f843c672, 'SaitaSwap')
    , (0x0388c1e0f210abae597b7de712b9510c6c36c857, 'luaswap')
    , (0xa40ec8a93293a3179d4b544239916c1b68cb47b6, 'SunflowerSwap')
    , (0x460b2005b3318982feada99f7ebf13e1d6f6effe, 'pepedex')
    , (0x1264f802364e0776b9a9e3d161b43c7333ac08b2, 'rhino_fi')
    , (0x7de800467afce442019884f51a4a1b9143a34fac, 'xchange')
    , (0xd87ad19db2c4ccbf897106de034d52e3dd90ea60, 'plasmaswap')
    , (0x46adc1c052fafd590f56c42e379d7d16622835a2, 'yape')
    , (0x6c565c5bbdc7f023cae8a2495105a531caac6e54, 'groveswap')
    , (0xd34971bab6e5e356fd250715f5de0492bb070452, 'swapr')
    , (0x35113a300ca0d7621374890abfeac30e88f214b1, 'SaitaSwap')
    , (0xb076b06f669e682609fb4a8c6646d2619717be4b, 'fraxswap')
    , (0xfb1eb9a45feb7269f3277233af513482bc04ea63, 'Swapos')
    , (0x5fbe219e88f6c6f214ce6f5b1fcaa0294f31ae1b, 'gammaswap')
    , (0xbae5dc9b19004883d0377419fef3c2c8832d7d7b, 'apeswap')
    , (0x1fed2e360a5afb2ac4b047102a7012a57f3c8cab, 'btswap')
    , (0x26f53fbadeeb777fb2a122dc703433d79241b64e, 'light_dao')
    , (0x08e7974cacf66c5a92a37c221a15d3c30c7d97e0, 'unifi')
    , (0xfae6a4370a3499f363461647fd54d110b3c8dc64, 'CelSwap')
    , (0xf028f723ed1d0fe01cc59973c49298aa95c57472, 'SashimiSwap')
    , (0x54f454d747e037da288db568d4121117eab34e79, 'bbb')
    , (0x2d723f60ad8da76286b2ac120498a5ea6babc792, 'neopin')
    , (0x91fAe1bc94A9793708fbc66aDcb59087C46dEe10, 'radioshack')
    , (0xe185e5335d68c2a18564b4b43bdf4ed86337ee70, 'quantoswap')
) AS t (factory_address, project_name)


   