{{ config(alias = 'l2_batch_submitters',
        tags=['static'],
        post_hook='{{ expose_spells(\'["ethereum"]\',
                                    "sector",
                                    "addresses",
                                    \'["msilb7"]\') }}') }}

SELECT address AS address, protocol_name, submitter_type, role_type, version, description
FROM (VALUES
         (0x5e4e65926ba27467555eb562121fac00d24e9dd2, 'OP Mainnet', 'Canonical Transaction Chain','to_address','2','OP Mainnet: Canonical Transaction Chain')
        ,(0xbe5dab4a2e9cd0f27300db4ab94bee3a233aeb19, 'OP Mainnet', 'State Commitment Chain','to_address','2','OP Mainnet: State Commitment Chain')
        ,(0x4bf681894abec828b212c906082b444ceb2f6cf6, 'OP Mainnet', 'Canonical Transaction Chain','to_address','1','OP Mainnet: OVM Canonical Transaction Chain')
        ,(0x473300df21d047806a082244b417f96b32f13a33, 'OP Mainnet', 'State Commitment Chain','from_address','1','OP Mainnet: OVM State Commitment Chain')

        ,(0x6887246668a3b87f54deb3b94ba47a6f63f32985, 'OP Mainnet', 'Canonical Transaction Chain','from_address','','OP Mainnet: Sequencer')
        ,(0xe969c2724d2448f1d1a6189d3e2aa1f37d5998c1, 'OP Mainnet', 'State Commitment Chain','from_address','','OP Mainnet: State Root Proposer')

        ,(0xff00000000000000000000000000000000000010, 'OP Mainnet', 'L1BatchInbox','to_address','Bedrock','OP Mainnet: L1BatchInbox')
        ,(0x6887246668a3b87f54deb3b94ba47a6f63f32985, 'OP Mainnet', 'L1BatchInbox','from_address','Bedrock','OP Mainnet: L1BatchInbox')

        ,(0xd2e67b6a032f0a9b1f569e63ad6c38f7342c2e00, 'OP Mainnet', 'L2OutputOracle','to_address','Bedrock','OP Mainnet: L2OutputOracle')
        ,(0xdfe97868233d1aa22e815a266982f2cf17685a27, 'OP Mainnet', 'L2OutputOracleProxy','to_address','Bedrock','OP Mainnet: L2OutputOracleProxy')
        ,(0x473300df21d047806a082244b417f96b32f13a33, 'OP Mainnet', 'L2OutputOracle','from_address','Bedrock','OP Mainnet: L2OutputOracle')

        ---
        ,(0xFf00000000000000000000000000000000008453, 'Base', 'L1BatchInbox','to_address','Bedrock','Base: L1BatchInbox')
        ,(0x5050F69a9786F081509234F1a7F4684b5E5b76C9, 'Base', 'L1BatchInbox','from_address','Bedrock','Base: L1BatchInbox')

        ,(0xf2460d3433475c8008ceffe8283f07eb1447e39a, 'Base', 'L2OutputOracle','to_address','Bedrock','Base: L2OutputOracle')
        ,(0x56315b90c40730925ec5485cf004d835058518A0, 'Base', 'L2OutputOracleProxy','to_address','Bedrock','Base: L2OutputOracleProxy')
        ,(0x642229f238fb9dE03374Be34B0eD8D9De80752c5, 'Base', 'L2OutputOracle','from_address','Bedrock','Base: L2OutputOracle')
        
        ---
        ,(0xC1B90E1e459aBBDcEc4DCF90dA45ba077d83BFc5, 'Public Goods Network', 'L1BatchInbox','to_address','Bedrock','Public Goods Network: L1BatchInbox')
        ,(0x99526b0e49A95833E734EB556A6aBaFFAb0Ee167, 'Public Goods Network', 'L1BatchInbox','from_address','Bedrock','Public Goods Network: L1BatchInbox')

        ,(0x76983dfed43c7ae7ebb592a92be2be972cae4348, 'Public Goods Network', 'L2OutputOracle','to_address','Bedrock','Public Goods Network: L2OutputOracle')
        ,(0xA38d0c4E6319F9045F20318BA5f04CDe94208608, 'Public Goods Network', 'L2OutputOracleProxy','to_address','Bedrock','Public Goods Network: L2OutputOracleProxy')
        ,(0x69968Ce0E92d9c101BAd81de55EFbcb69603cFe3, 'Public Goods Network', 'L2OutputOracle','from_address','Bedrock','Public Goods Network: L2OutputOracle')

        ---
        ,(0x6F54Ca6F6EdE96662024Ffd61BFd18f3f4e34DFf, 'Zora Network', 'L1BatchInbox','to_address','Bedrock','Zora Network: L1BatchInbox')
        ,(0x48247032092e7b0ecf5dEF611ad89eaf3fC888Dd, 'Zora Network', 'L1BatchInbox','from_address','Bedrock','Zora Network: L1BatchInbox')

        ,(0x9eedde6b4d3263b97209ba860edf3fc6a8fb6a44, 'Zora Network', 'L2OutputOracle','to_address','Bedrock','Zora Network: L2OutputOracle')
        ,(0x9E6204F750cD866b299594e2aC9eA824E2e5f95c, 'Zora Network', 'L2OutputOracleProxy','to_address','Bedrock','Zora Network: L2OutputOracleProxy')
        ,(0x48247032092e7b0ecf5dEF611ad89eaf3fC888Dd, 'Zora Network', 'L2OutputOracle','from_address','Bedrock','Zora Network: L2OutputOracle')
        
        ---

        ,(0xa4b10ac61e79ea1e150df70b8dda53391928fd14, 'Arbitrum','SequencerInbox','from_address','','Arbitrum: Sequencer')
        ,(0xcce5c6cff61c49b4d53dd6024f8295f3c5230513, 'Arbitrum','SequencerInbox','from_address','','Arbitrum: Sequencer 2')
        ,(0x4c6f947ae67f572afa4ae0730947de7c874f95ef, 'Arbitrum','SequencerInbox','to_address','1','Arbitrum: Old Sequencer Inbox')
        ,(0x1c479675ad559dc151f6ec7ed3fbf8cee79582b6, 'Arbitrum','SequencerInbox','to_address','2','Arbitrum: Sequencer Inbox')

        ---

        ,(0xfa46908b587f9102e81ce6c43b7b41b52881c57f, 'Boba', 'Canonical Transaction Chain','from_address','2','')
        ,(0x702ad5c5fb87aace54978143a707d565853d6fd5, 'Boba', 'Canonical Transaction Chain','from_address','1','')
        ,(0xfbd2541e316948b259264c02f370ed088e04c3db, 'Boba', 'Canonical Transaction Chain','to_address','2','')
        ,(0x4b5d9e5a6b1a514eba15a2f949531dccd7c272f2, 'Boba', 'Canonical Transaction Chain','to_address','1','')
        ,(0x5558c63d5bf229450995adc160c023c9f4d4be80, 'Boba', 'State Commitment Chain','from_address','','')
        ,(0xde7355c971a5b733fe2133753abd7e5441d441ec, 'Boba', 'State Commitment Chain','to_address','','')

        ---

        ,(0xcdf02971871b7736874e20b8487c019d28090019, 'Metis', 'Canonical Transaction Chain','from_address','','')
        ,(0x56a76bcc92361f6df8d75476fed8843edc70e1c9, 'Metis', 'Canonical Transaction Chain','to_address','1','')
        ,(0x6a1db7d799fba381f2a518ca859ed30cb8e1d41a, 'Metis', 'Canonical Transaction Chain','to_address','2','')
        ,(0x9cb01d516d930ef49591a05b09e0d33e6286689d, 'Metis', 'State Commitment Chain','from_address','','')
        ,(0xf209815e595cdf3ed0aaf9665b1772e608ab9380, 'Metis', 'State Commitment Chain','to_address','','')

        ---

        ,(0x253887577420cb7e7418cd4d50147743c8041b28, 'Aevo', 'L1BatchInbox','to_address','OP Stack','Aevo (Ribbon Finance): L1BatchInbox')
        ,(0x889e21d7ba3d6dd62e75d4980a4ad1349c61599d, 'Aevo', 'L1BatchInbox','from_address','OP Stack','Aevo (Ribbon Finance): L1BatchInbox')

        ,(0x75acb7ae6c76b3f5ca049431fe2c0797dd002b90, 'Aevo', 'L2OutputOracle','to_address','OP Stack','Aevo (Ribbon Finance): L1OutputOracle')
        ,(0x909e51211e959339efb14b36f5a50955a8ae3770, 'Aevo', 'L2OutputOracle','from_address','OP Stack','Aevo (Ribbon Finance): L1OutputOracle')



        ) AS x (address, protocol_name, submitter_type, role_type, version, description)