-- this should probably live somewhere else, just for testing purposes for now

{{ config(
    schema = 'dex_mass_decoding_ethereum',
    alias = 'uniswap_v2_fork_mapping',
    tags = ['static'],
    unique_key = ['factory_address'])
}}

SELECT factory_address, project_name
FROM
(VALUES
      (0xfb1eb9a45feb7269f3277233af513482bc04ea63, 'Swapos')
    , (0xfae6a4370a3499f363461647fd54d110b3c8dc64, 'CelSwap')
    , (0xcA143Ce32Fe78f1f7019d7d551a6402fC5350c73, 'PancakeSwap')
    , (0x9Ad32bf5DaFe152Cbe027398219611DB4E8753B3, 'NomiSwap')
    , (0x98813bD470A3BA8Da3D16488c58374e8dBc2FF22, 'DigiSwap')
    , (0x5B9f88Ee10413e764BEFACa083fB290c4f25F720, 'Broccoli')
    , (0x858E3312ed3A876947EA49d572A7C42DE08af7EE, 'BiSwap')
    , (0x01bF7C66c6BD861915CdaaE475042d3c4BaE16A7, 'BakerySwap')
    , (0x0841BD0B734E4F5853f0dD8d7Ea041c241fb0Da6, 'ApeSwap')
    , (0x4d05D0045df5562D6D52937e93De6Ec1FECDAd21, 'SafeSwap')
    , (0xf1B735685416253A8F7c8a6686970cA2B0cceCce, 'CoSwap')
    , (0x8BA1a4C24DE655136DEd68410e222cCA80d43444, 'Sphynx')
    , (0x1e895bFe59E3A5103e8B7dA3897d1F2391476f3c, 'DooarSwap')
    , (0x31aFfd875e9f68cd6Cd12Cee8943566c9A4bBA13, 'Elk')
    , (0x1A04Afe9778f95829017741bF46C9524B91433fB, 'Orbital')
    , (0x9A272d734c5a0d7d84E0a892e891a553e8066dce, 'FstSwap')
    , (0xD04A80baeeF12fD7b1D1ee6b1f8ad354f81bc4d7, 'W3Swap')
    , (0xCe8fd65646F2a2a897755A1188C04aCe94D2B8D0, 'BSCswap')
    , (0x8b6Ca4B3E08c9f80209e66436187088C99C9C2AC, 'BSCswap')
    , (0xdd538E4Fd1b69B7863E1F741213276A6Cf1EfB3B, 'CheeseSwap')
    , (0x3CD1C46068dAEa5Ebb0d3f55F6915B10648062B8, 'Mdex')
    , (0x86407bEa2078ea5f5EB5A52B2caA963bC1F889Da, 'BabySwap')
    , (0xBCfCcbde45cE874adCB698cC183deBcF17952812, 'PancakeSwap')
    , (0x59DA12BDc470C8e85cA26661Ee3DCD9B85247C88, 'FastSwap')
    , (0x79C342FddBBF376cA6B4EFAc7aaA457D6063F8Cb, 'Winery')
    , (0xB42E3FE71b7E0673335b3331B3e1053BD9822570, 'WaultSwap')
    , (0x3e708FdbE3ADA63fc94F8F61811196f1302137AD, 'CafeSwap')
    , (0x4E66Fda7820c53C1a2F601F84918C375205Eac3E, 'Twindex')
    , (0xC7a506ab3ac668EAb6bF9eCf971433D6CFeF05D9, 'Alita')
    , (0x25393bb68c89a894b5e20fa3fc3b3b34f843c672, 'SaitaSwap')
    , (0x03407772f5ebfb9b10df007a2dd6fff4ede47b53 ,'capitaldex')
    , (0x6c565c5bbdc7f023cae8a2495105a531caac6e54, 'groveswap')
    , (0xF028F723ED1D0fE01cC59973C49298AA95c57472, 'SashimiSwap')
    , (0x1fed2e360a5afb2ac4b047102a7012a57f3c8cab, 'BTswap')
    , (0x35113a300ca0D7621374890ABFEAC30E88f214b1, 'SaitaSwap')
    , (0xA40ec8A93293A3179D4b544239916C1B68cB47B6, 'SunflowerSwap')
    , (0x5fa0060fcfea35b31f7a5f6025f0ff399b98edf1, 'OrionProtocol')

) AS t (factory_address, project)