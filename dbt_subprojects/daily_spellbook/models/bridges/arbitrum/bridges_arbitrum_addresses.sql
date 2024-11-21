{{ config(alias = 'addresses',
        schema = 'bridges_arbitrum',
        tags=['static']
          ) }}

SELECT blockchain, address, bridge_name, description
FROM (VALUES 
('arbitrum', 0xe35e9842fceaca96570b734083f4a58e8f7c5f2a, 'Across Protocol', 'Bridge'),
('arbitrum', 0xd8c6dd978a3768f7ddfe3a9aad2c3fd75fa9b6fd, 'Across Protocol', 'Deposit Box'),
('arbitrum', 0x096760f208390250649e3e8763348e783aef5562, 'Arbitrum One', 'L2 Custom Gateway'),
('arbitrum', 0x467194771dae2967aef3ecbedd3bf9a310c76c65, 'Arbitrum One', 'L2 DAI Gateway'),
('arbitrum', 0x09e9222e96e7b4ae2a407b98d48e330053351eee, 'Arbitrum One', 'L2 ERC20 Gateway'),
('arbitrum', 0x5288c571fd7ad117bea99bf60fe0846c4e84f933, 'Arbitrum One', 'L2 Gateway Router'),
('arbitrum', 0x6c411ad3e74de3e7bd422b94a27770f5b86c623b, 'Arbitrum One', 'L2 WETH Gateway'),
('arbitrum', 0x1619de6b6b20ed217a58d00f37b9d47c7663feca, 'Celer Network', 'cBridge'),
('arbitrum', 0xdd90e5e87a2081dcf0391920868ebc2ffb81a1af, 'Celer Network', 'cBridge 2.0'),
('arbitrum', 0x43de2d77bf8027e25dbd179b491e8d64f38398aa, 'deBridgeGate', ''),
('arbitrum', 0xe7351fd770a37282b91d153ee690b63579d6dd7f, 'Dln', 'Destination'),
('arbitrum', 0x3c2269811836af69497e5f486a85d7316753cf62, 'LayerZero', 'Arbitrum Endpoint'),
('arbitrum', 0x82e0b8cdd80af5930c4452c684e71c861148ec8a, 'Metamask', 'Bridge 1'),
('arbitrum', 0xc10ef9f491c9b59f936957026020c321651ac078, 'Multichain', 'anyCall V6'),
('arbitrum', 0xc931f61b1534eb21d8c11b24f3f5ab2471d4ab50, 'Multichain', 'Router V4'),
('arbitrum', 0x650af55d5877f289837c30b94af91538a7504b76, 'Multichain', 'Router V6'),
('arbitrum', 0x80c67432656d59144ceff962e8faf8926599bcf8, 'Orbiter Finance', 'Bridge'),
('arbitrum', 0x3a23f943181408eac424116af7b7790c94cb97a5, 'Socket', 'Gateway'),
('arbitrum', 0x6f4e8eba4d337f874ab57478acc2cb5bacdc19c9, 'Synapse', 'Bridge'),
('arbitrum', 0x375e9252625bdb10b457909157548e1d047089f9, 'Synapse', 'Bridge Zap'),
('arbitrum', 0xcf4d2994088a8cde52fb584fe29608b63ec063b2, 'xPollinate', 'Transaction Manager'),
('arbitrum', 0xEE9deC2712cCE65174B561151701Bf54b99C24C8, 'connext', ''),
('arbitrum', 0x3749c4f034022c39ecaffaba182555d4508caccc, 'Hop', ''),
('arbitrum', 0x25fb92e505f752f730cad0bd4fa17ece4a384266, 'Hop', ''),
('arbitrum', 0x0e0e3d2c5c292161999474247956ef542cabf8dd, 'Hop', ''),
('arbitrum', 0x72209fe68386b37a40d6bca04f78356fd342491f, 'Hop', ''),
('arbitrum', 0x53bf833a5d6c4dda888f69c22c88c9f356a41614, 'Stargate', ''),
('arbitrum', 0xaf8ae6955d07776ab690e565ba6fbc79b8de3a5d, 'Rhino.fi', ''),
('arbitrum', 0xce16f69375520ab01377ce7b88f5ba8c48f8d666, 'Squid', ''),
('arbitrum', 0xca506793A420E901BbCa8066be5661E3C52c84c2, 'Symbiosis', ''),
('arbitrum', 0x000039ddcf1f63cf3555e62a8d32a11bd1e7e1e1, 'Meson', '')



      ) AS x (blockchain, address, bridge_name, description)