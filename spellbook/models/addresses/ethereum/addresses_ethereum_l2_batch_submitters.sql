{{ config(alias='l2_batch_submitters',
        post_hook='{{ expose_spells(\'["ethereum"]\',
                                    "sector",
                                    "addresses",
                                    \'["msilb7"]\') }}') }}

SELECT lower(address) as address, protocol_name, submitter_type, role_type, version, description
FROM (VALUES
       ("0x5e4e65926ba27467555eb562121fac00d24e9dd2", 'Optimism', 'Canonical Transaction Chain','to_address','2','Optimism: Canonical Transaction Chain')
      ,("0xbe5dab4a2e9cd0f27300db4ab94bee3a233aeb19", "Optimism", "State Commitment Chain","to_address","2","Optimism: State Commitment Chain")
      ,("0x4bf681894abec828b212c906082b444ceb2f6cf6", "Optimism", "Canonical Transaction Chain","to_address","1","Optimism: OVM Canonical Transaction Chain")
      ,("0x473300df21d047806a082244b417f96b32f13a33", "Optimism", "State Commitment Chain","to_address","1","Optimism: OVM State Commitment Chain")

      ,("0x6887246668a3b87f54deb3b94ba47a6f63f32985", "Optimism", "Canonical Transaction Chain","from_address","","Optimism: Sequencer")
      ,("0xe969c2724d2448f1d1a6189d3e2aa1f37d5998c1", "Optimism", "State Commitment Chain","from_address","","Optimism: State Root Proposer")

      ,("0xa4b10ac61e79ea1e150df70b8dda53391928fd14", "Arbitrum","SequencerInbox","from_address","","Arbitrum: Sequencer")
      ,("0xcce5c6cff61c49b4d53dd6024f8295f3c5230513", "Arbitrum","SequencerInbox","from_address","","Arbitrum: Sequencer 2")
      ,("0x4c6f947ae67f572afa4ae0730947de7c874f95ef", "Arbitrum","SequencerInbox","to_address","1","Arbitrum: Old Sequencer Inbox")
      ,("0x1c479675ad559dc151f6ec7ed3fbf8cee79582b6", "Arbitrum","SequencerInbox","to_address","2","Arbitrum: Sequencer Inbox")

      ,("0xfa46908b587f9102e81ce6c43b7b41b52881c57f", "Boba", "Canonical Transaction Chain","from_address","2","")
      ,("0x702ad5c5fb87aace54978143a707d565853d6fd5", "Boba", "Canonical Transaction Chain","from_address","","")
      ,("0xfbd2541e316948b259264c02f370ed088e04c3db", "Boba", "Canonical Transaction Chain","to_address","2","")
      ,("0x4b5d9e5a6b1a514eba15a2f949531dccd7c272f2", "Boba", "Canonical Transaction Chain","to_address","1","")

      ,("0xcdf02971871b7736874e20b8487c019d28090019", "Metis", "Canonical Transaction Chain","from_address","","")
      ,("0x9cb01d516d930ef49591a05b09e0d33e6286689d", "Metis", "State Commitment Chain","from_address","","")
      ,("0x56a76bcc92361f6df8d75476fed8843edc70e1c9", "Metis", "Canonical Transaction Chain","to_address","1","")
      ,("0x6a1db7d799fba381f2a518ca859ed30cb8e1d41a", "Metis", "Canonical Transaction Chain","to_address","2","")
      ,("0xf209815e595cdf3ed0aaf9665b1772e608ab9380", "Metis", "State Commitment Chain","to_address","","")
      ) AS x (address, protocol_name, submitter_type, role_type, version, description)