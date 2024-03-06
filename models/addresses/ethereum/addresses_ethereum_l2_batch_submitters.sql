{{ config(
        schema = 'addresses_ethereum',
        alias = 'l2_batch_submitters',
        tags=['static'],
        post_hook='{{ expose_spells(\'["ethereum"]\',
                                    "sector",
                                    "addresses",
                                    \'["msilb7"]\') }}') }}

SELECT address AS address, protocol_name, codebase, submitter_type, role_type, version, description
FROM (VALUES
         (0x5e4e65926ba27467555eb562121fac00d24e9dd2, 'OP Mainnet', 'Optimism', 'Canonical Transaction Chain','to_address','2','OP Mainnet: Canonical Transaction Chain')
        ,(0xbe5dab4a2e9cd0f27300db4ab94bee3a233aeb19, 'OP Mainnet', 'Optimism', 'State Commitment Chain','to_address','2','OP Mainnet: State Commitment Chain')
        ,(0x4bf681894abec828b212c906082b444ceb2f6cf6, 'OP Mainnet', 'Optimism', 'Canonical Transaction Chain','to_address','1','OP Mainnet: OVM Canonical Transaction Chain')
        ,(0x473300df21d047806a082244b417f96b32f13a33, 'OP Mainnet', 'Optimism', 'State Commitment Chain','from_address','1','OP Mainnet: OVM State Commitment Chain')

        ,(0x6887246668a3b87f54deb3b94ba47a6f63f32985, 'OP Mainnet', 'Optimism', 'Canonical Transaction Chain','from_address','','OP Mainnet: Sequencer')
        ,(0xe969c2724d2448f1d1a6189d3e2aa1f37d5998c1, 'OP Mainnet', 'Optimism', 'State Commitment Chain','from_address','','OP Mainnet: State Root Proposer')

        ,(0xff00000000000000000000000000000000000010, 'OP Mainnet', 'Optimism', 'L1BatchInbox','to_address','Modern','OP Mainnet: L1BatchInbox')
        ,(0x6887246668a3b87f54deb3b94ba47a6f63f32985, 'OP Mainnet', 'Optimism', 'L1BatchInbox','from_address','Modern','OP Mainnet: L1BatchInbox')

        ,(0xd2e67b6a032f0a9b1f569e63ad6c38f7342c2e00, 'OP Mainnet', 'Optimism', 'L2OutputOracle','to_address','Modern','OP Mainnet: L2OutputOracle')
        ,(0xdfe97868233d1aa22e815a266982f2cf17685a27, 'OP Mainnet', 'Optimism', 'L2OutputOracleProxy','to_address','Modern','OP Mainnet: L2OutputOracleProxy')
        ,(0x473300df21d047806a082244b417f96b32f13a33, 'OP Mainnet', 'Optimism', 'L2OutputOracle','from_address','Modern','OP Mainnet: L2OutputOracle')

        ---
        ,(0xFf00000000000000000000000000000000008453, 'Base', 'Optimism', 'L1BatchInbox','to_address','Modern','Base: L1BatchInbox')
        ,(0x5050F69a9786F081509234F1a7F4684b5E5b76C9, 'Base', 'Optimism', 'L1BatchInbox','from_address','Modern','Base: L1BatchInbox')

        ,(0xf2460d3433475c8008ceffe8283f07eb1447e39a, 'Base', 'Optimism', 'L2OutputOracle','to_address','Modern','Base: L2OutputOracle')
        ,(0x56315b90c40730925ec5485cf004d835058518A0, 'Base', 'Optimism', 'L2OutputOracleProxy','to_address','Modern','Base: L2OutputOracleProxy')
        ,(0x642229f238fb9dE03374Be34B0eD8D9De80752c5, 'Base', 'Optimism', 'L2OutputOracle','from_address','Modern','Base: L2OutputOracle')
        
        ---
        ,(0xC1B90E1e459aBBDcEc4DCF90dA45ba077d83BFc5, 'Public Goods Network', 'Optimism', 'L1BatchInbox','to_address','Modern','Public Goods Network: L1BatchInbox')
        ,(0x99526b0e49A95833E734EB556A6aBaFFAb0Ee167, 'Public Goods Network', 'Optimism', 'L1BatchInbox','from_address','Modern','Public Goods Network: L1BatchInbox')

        ,(0x76983dfed43c7ae7ebb592a92be2be972cae4348, 'Public Goods Network', 'Optimism', 'L2OutputOracle','to_address','Modern','Public Goods Network: L2OutputOracle')
        ,(0xA38d0c4E6319F9045F20318BA5f04CDe94208608, 'Public Goods Network', 'Optimism', 'L2OutputOracleProxy','to_address','Modern','Public Goods Network: L2OutputOracleProxy')
        ,(0x69968Ce0E92d9c101BAd81de55EFbcb69603cFe3, 'Public Goods Network', 'Optimism', 'L2OutputOracle','from_address','Modern','Public Goods Network: L2OutputOracle')

        ---
        ,(0x6F54Ca6F6EdE96662024Ffd61BFd18f3f4e34DFf, 'Zora', 'Optimism', 'L1BatchInbox','to_address','Modern','Zora: L1BatchInbox')
        ,(0x625726c858dbf78c0125436c943bf4b4be9d9033, 'Zora', 'Optimism', 'L1BatchInbox','from_address','Modern','Zora: L1BatchInbox')

        ,(0x9eedde6b4d3263b97209ba860edf3fc6a8fb6a44, 'Zora', 'Optimism', 'L2OutputOracle','to_address','Modern','Zora: L2OutputOracle')
        ,(0x9E6204F750cD866b299594e2aC9eA824E2e5f95c, 'Zora', 'Optimism', 'L2OutputOracleProxy','to_address','Modern','Zora: L2OutputOracleProxy')
        ,(0x48247032092e7b0ecf5dEF611ad89eaf3fC888Dd, 'Zora', 'Optimism', 'L2OutputOracle','from_address','Modern','Zora: L2OutputOracle')
        ---
        ,(0x24E59d9d3Bd73ccC28Dc54062AF7EF7bFF58Bd67, 'Mode', 'Optimism', 'L1BatchInbox','to_address','Modern','Mode: L1BatchInbox')
        ,(0x99199a22125034c808ff20f377d91187E8050F2E, 'Mode', 'Optimism', 'L1BatchInbox','from_address','Modern','Mode: L1BatchInbox')

        ,(0x6093023a4A7E6873EDFb02B4bCE48c53FD310EEc, 'Mode', 'Optimism', 'L2OutputOracle','to_address','Modern','Mode: L2OutputOracle')
        ,(0x4317ba146D4933D889518a3e5E11Fe7a53199b04, 'Mode', 'Optimism', 'L2OutputOracleProxy','to_address','Modern','Mode: L2OutputOracleProxy')
        ,(0x674F64D64Ddc198db83cd9047dF54BF89cCD0ddB, 'Mode', 'Optimism', 'L2OutputOracle','from_address','Modern','Mode: L2OutputOracle')
        ---
        ,(0x08aA34cC843CeEBcC88A627F18430294aA9780be, 'Orderly Network', 'Optimism', 'L1BatchInbox','to_address','Modern','Orderly Network: L1BatchInbox')
        ,(0xf8dB8Aba597fF36cCD16fECfbb1B816B3236E9b8, 'Orderly Network', 'Optimism', 'L1BatchInbox','from_address','Modern','Orderly Network: L1BatchInbox')

        ,(0x334251f91a3795c043663172CB59a963a9029aed, 'Orderly Network', 'Optimism', 'L2OutputOracle','to_address','Modern','Orderly Network: L2OutputOracle')
        ,(0x5e76821C3c1AbB9fD6E310224804556C61D860e0, 'Orderly Network', 'Optimism', 'L2OutputOracleProxy','to_address','Modern','Orderly Network: L2OutputOracleProxy')
        ,(0x74BaD482a7f73C8286F50D8Aa03e53b7d24A5f3B, 'Orderly Network', 'Optimism', 'L2OutputOracle','from_address','Modern','Orderly Network: L2OutputOracle')
        ---
        ,(0x5f7f7f6DB967F0ef10BdA0678964DBA185d16c50, 'Lyra', 'Optimism', 'L1BatchInbox','to_address','Modern','Lyra: L1BatchInbox')
        ,(0x14e4E97bDc195d399Ad8E7FC14451C279FE04c8e, 'Lyra', 'Optimism', 'L1BatchInbox','from_address','Modern','Lyra: L1BatchInbox')

        ,(0xad206309916Fe08A27221133dde05a8F30f75e29, 'Lyra', 'Optimism', 'L2OutputOracle','to_address','Modern','Lyra: L2OutputOracle')
        ,(0x1145E7848c8B64c6cab86Fd6D378733385c5C3Ba, 'Lyra', 'Optimism', 'L2OutputOracleProxy','to_address','Modern','Lyra: L2OutputOracleProxy')
        ,(0x03e820562ffd2e0390787caD706EaF1FF98C2608, 'Lyra', 'Optimism', 'L2OutputOracle','from_address','Modern','Lyra: L2OutputOracle')
        ---
        ,(0xFf00000000000000000000000000000000081457, 'Blast', 'Optimism', 'L1BatchInbox','to_address','Modern','Blast: L1BatchInbox')
        ,(0x415c8893D514F9BC5211d36eEDA4183226b84AA7, 'Blast', 'Optimism', 'L1BatchInbox','from_address','Modern','Blast: L1BatchInbox')

        ,(0xaEbA6c3042B463DfAA6A2DFA96486D5A92186cFF, 'Blast', 'Optimism', 'L2OutputOracle','to_address','Modern','Blast: L2OutputOracle')
        ,(0x826D1B0D4111Ad9146Eb8941D7Ca2B6a44215c76, 'Blast', 'Optimism', 'L2OutputOracleProxy','to_address','Modern','Blast: L2OutputOracleProxy')
        ,(0x082b616Ec99167B2FEdee053F07db6795D4dA821, 'Blast', 'Optimism', 'L2OutputOracle','from_address','Modern','Blast: L2OutputOracle')
        ---
        ,(0xAEbA8e2307A22B6824a9a7a39f8b016C357Cd1Fe, 'Manta Pacific', 'Optimism', 'L1BatchInbox','to_address','Modern','Manta Pacific: L1BatchInbox')
        ,(0xA76E31D8471D569EfDd3D95d1b11Ce6710f4533F, 'Manta Pacific', 'Optimism', 'L1BatchInbox','from_address','Modern','Manta Pacific: L1BatchInbox')

        ,(0x1E5e634981564fc645dcbC6546aE618d7870B30a, 'Manta Pacific', 'Optimism', 'L2OutputOracle','to_address','Modern','Manta Pacific: L2OutputOracle')
        ,(0x30c789674ad3B458886BBC9abf42EEe19EA05C1D, 'Manta Pacific', 'Optimism', 'L2OutputOracleProxy','to_address','Modern','Manta Pacific: L2OutputOracleProxy')
        ,(0x853bDfF715eFeDc280C16241dD30A6d651f90aCa, 'Manta Pacific', 'Optimism', 'L2OutputOracle','from_address','Modern','Manta Pacific: L2OutputOracle')
        ---
        ,(0x0C57B7f3bAc278bE091431B52470fBAdBc4240E6, 'Hypr Network', 'Optimism', 'L1BatchInbox','to_address','Modern','Hypr Network: L1BatchInbox')
        ,(0x994c288de8418c8D3c5a4D21A69f35bF9641781C, 'Hypr Network', 'Optimism', 'L1BatchInbox','from_address','Modern','Hypr Network: L1BatchInbox')

        ,(0x2246d85AC397d289d49a92C804201738C4Bd2d73, 'Hypr Network', 'Optimism', 'L2OutputOracle','to_address','Modern','Hypr Network: L2OutputOracle')
        ,(0x3E4F4Eb77a9c1f88c0e1F5aDCc9d3521Ce157FdD, 'Hypr Network', 'Optimism', 'L2OutputOracleProxy','to_address','Modern','Hypr Network: L2OutputOracleProxy')
        ,(0xeF2169180a2eda91B695eA5e5C4f4547C013840c, 'Hypr Network', 'Optimism', 'L2OutputOracle','from_address','Modern','Hypr Network: L2OutputOracle')
        ---
        ,(0xfF000000000000000000000000000000000420fC, 'Fraxtal', 'Optimism', 'L1BatchInbox','to_address','Modern','Fraxtal: L1BatchInbox')
        ,(0x6017f75108f251a488B045A7ce2a7C15b179d1f2, 'Fraxtal', 'Optimism', 'L1BatchInbox','from_address','Modern','Fraxtal: L1BatchInbox')

        ,(0x84c2f365733c6560bd49D8768d9c23B07A9DD630, 'Fraxtal', 'Optimism', 'L2OutputOracle','to_address','Modern','Fraxtal: L2OutputOracle')
        ,(0x66CC916Ed5C6C2FA97014f7D1cD141528Ae171e4, 'Fraxtal', 'Optimism', 'L2OutputOracleProxy','to_address','Modern','Fraxtal: L2OutputOracleProxy')
        ,(0xFb90465f3064fF63FC460F01A6307eC73d64bc50, 'Fraxtal', 'Optimism', 'L2OutputOracle','from_address','Modern','Fraxtal: L2OutputOracle')

        ---

        ,(0xa4b10ac61e79ea1e150df70b8dda53391928fd14, 'Arbitrum', 'Arbitrum','SequencerInbox','from_address','','Arbitrum: Sequencer')
        ,(0xcce5c6cff61c49b4d53dd6024f8295f3c5230513, 'Arbitrum','Arbitrum','SequencerInbox','from_address','','Arbitrum: Sequencer 2')
        ,(0x4c6f947ae67f572afa4ae0730947de7c874f95ef, 'Arbitrum','Arbitrum','SequencerInbox','to_address','1','Arbitrum: Old Sequencer Inbox')
        ,(0x1c479675ad559dc151f6ec7ed3fbf8cee79582b6, 'Arbitrum','Arbitrum','SequencerInbox','to_address','2','Arbitrum: Sequencer Inbox')

        ---

        ,(0xfa46908b587f9102e81ce6c43b7b41b52881c57f, 'Boba','Optimism', 'Canonical Transaction Chain','from_address','2','')
        ,(0x702ad5c5fb87aace54978143a707d565853d6fd5, 'Boba','Optimism', 'Canonical Transaction Chain','from_address','1','')
        ,(0xfbd2541e316948b259264c02f370ed088e04c3db, 'Boba','Optimism', 'Canonical Transaction Chain','to_address','2','')
        ,(0x4b5d9e5a6b1a514eba15a2f949531dccd7c272f2, 'Boba','Optimism', 'Canonical Transaction Chain','to_address','1','')
        ,(0x5558c63d5bf229450995adc160c023c9f4d4be80, 'Boba','Optimism', 'State Commitment Chain','from_address','','')
        ,(0xde7355c971a5b733fe2133753abd7e5441d441ec, 'Boba','Optimism', 'State Commitment Chain','to_address','','')

        ---

        ,(0xcdf02971871b7736874e20b8487c019d28090019, 'Metis','Optimism', 'Canonical Transaction Chain','from_address','','')
        ,(0x56a76bcc92361f6df8d75476fed8843edc70e1c9, 'Metis','Optimism', 'Canonical Transaction Chain','to_address','1','')
        ,(0x6a1db7d799fba381f2a518ca859ed30cb8e1d41a, 'Metis','Optimism', 'Canonical Transaction Chain','to_address','2','')
        ,(0x9cb01d516d930ef49591a05b09e0d33e6286689d, 'Metis','Optimism', 'State Commitment Chain','from_address','','')
        ,(0xf209815e595cdf3ed0aaf9665b1772e608ab9380, 'Metis','Optimism', 'State Commitment Chain','to_address','','')

        ---

        ,(0x253887577420cb7e7418cd4d50147743c8041b28, 'Aevo','Optimism', 'L1BatchInbox','to_address','OP Stack','Aevo (Ribbon Finance): L1BatchInbox')
        ,(0x889e21d7ba3d6dd62e75d4980a4ad1349c61599d, 'Aevo','Optimism', 'L1BatchInbox','from_address','OP Stack','Aevo (Ribbon Finance): L1BatchInbox')

        ,(0x909e51211e959339efb14b36f5a50955a8ae3770, 'Aevo','Optimism', 'L2OutputOracle','to_address','OP Stack','Aevo (Ribbon Finance): L1OutputOracle')
        ,(0x75acb7ae6c76b3f5ca049431fe2c0797dd002b90, 'Aevo','Optimism', 'L2OutputOracle','from_address','OP Stack','Aevo (Ribbon Finance): L1OutputOracle')



        ) AS x (address, protocol_name, codebase, submitter_type, role_type, version, description)
