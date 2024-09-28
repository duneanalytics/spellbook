{{
  config(
    alias='ccip_operator_meta',
	materialized = 'view'
  )
}}

SELECT
   'bnb' as blockchain,
   node_address,
   operator_name
FROM (values
  (0xF2073874894E3a3a96473bf6E5Fc0063eaa7D213, 'cryptomanufaktur'),
  (0xa04Ed470720D6053DDC8e52a5ccD932c20cA7CD7, 'dextrac'),
  (0x8CdDbCCC4Cd312Ed7172b40031f4cbB130CfD370, 'everstake'),
  (0x25e0E48f7d456342016111251FF299192e6606bc, 'fiews'),
  (0x6B0A9d7Bd3eafa53723f8133267B2B92b150bF5e, 'kytzu'),
  (0xF0f0a40035cAA716ac5ec082F2488D3fF55b0D7f, 'linkforest'),
  (0x90a59345a36fcAAc56d8813C46Be9a4442d9E2Ec, 'linkpool'),
  (0x6afA1D3Eb24106EE4aAf57D80950c4ccBD1300b9, 'linkriver'),
  (0xBCac4d2009203BFDf664D5f456F8c4C8A8882a5A, 'newroad'),
  (0x0fc372E74aB2E087811fBdDCF8AaaCb2682563c9, 'northwestnodes'),
  (0x445De071A254ded62D80c5295a5a96cAf9aD77A8, 'omniscience'),
  (0x85d6849c34780A18Edf0D780304A593F3B6834c8, 'p2porg'),
  (0x4e793dB6da959C33F754D2239f2b4BCBd94f025a, 'piertwo'),
  (0x180eE3fb4BD9e6f874f01A1C7c44598585f3599D, 'prophet'),
  (0x376aDb3316991F09dCC7771273258eB333a6cEd1, 'syncnode'),
  (0x91b68A402e44364c55Dd242CE34982bB2d9FA7a9, 'validationcloud'),
  (0x21F500EB03C79ff2260E2Eb045e37eFE16b460D8, 'wetez')
) a (node_address, operator_name)
