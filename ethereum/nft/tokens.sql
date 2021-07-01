-- notes
-- input from:
-- https://raw.githubusercontent.com/vasa-develop/nft-tokenlist/master/trimmed_3300_nfts.tokenlist.json
--
-- 1. `name` is a non-reserved SQL keyword
-- See https://www.postgresql.org/docs/8.1/sql-keywords-appendix.html
--
-- 2. Some lines contain unusual or special Unicode characters e.g.
-- '
-- $
-- emoji 
CREATE TABLE IF NOT EXISTS nft.tokens (
	contract_address bytea PRIMARY KEY,
	name text,
	symbol text,
	standard text
);

BEGIN;
DELETE FROM nft.tokens *;

COPY nft.tokens (contract_address, name, symbol, standard) FROM stdin;
\\xbc4ca0eda7647a8ab7c2061c2e118a18a936f13d	Bored Ape Yacht Club	BAYC	erc721
\\xb47e3cd837ddf8e4c57f05d70ab865de6e193bbb	CryptoPunks	PUNK	cryptopunks
\\xa3aee8bce55beea1951ef834b99f3ac60d1abeeb	VeeFriends	VFT	erc721
\\xa7d8d9ef8d8ce8992df33d8b8cf4aebabd5bd270	Art Blocks Curated	BLOCKS	erc721
\\x629a673a8242c2ac4b7b8c5d8735fbeac21a6205	Sorare	SOR	erc721
\\xd07dc4262bcdbf85190c01c996b4c06a461d2430	Rarible		erc1155
\\xba30e5f9bb24caa003e9f2f0497ad287fdf95623	Bored Ape Kennel Club	BAKC	erc721
\\x3f0785095a660fee131eebcd5aa243e529c21786	Super Yeti	defra	erc721
\\x959e104e1a4db6317fa58f8295f586e1a978c297	Decentraland	EST	erc721
\\x4581649af66bccaee81eebae3ddc0511fe4c5312	The Alien Boy	TABOY	erc721
\\xedb61f74b0d09b2558f1eeb79b247c1f363ae452	Gutter Cat Gang		erc1155
\\xf1b33ac32dbc6617f7267a349be6ebb004feccff	Dreamloops	DRMLOOPS	erc721
\\x7bd29408f11d2bfc23c34f18275bbf23bb716bc7	Meebits	⚇	erc721
\\xf17131a4c85e8a75ba52b3f91ce8c32f6f163924	The Sandbox	SAND	erc721
\\x5ab21ec0bfa0b29545230395e3adaca7d552c948	PUNKS Comic	COMIC	erc721
\\xf78296dfcf01a2612c2c847f68ad925801eeed80	My Fucking Pickle	FUCKINGPICKLES	erc721
\\x8c9f364bf7a56ed058fc63ef81c6cf09c833e656	SuperRare		unknown
\\xbfcd68ded9d511a632d45333155350a1907d4977	BoredNessApeClub	BNAC	erc721
\\x11bdfb09bebf4f0ab66dd1d6b85d0ef58ef1ba6c	MakersPlace	MKT	erc721
\\x17acddb7053be19e852e64dc5a344b599756f557	F1® Delta Time	F1DT.LCK	erc20
\\xc2c747e0f7004f9e8817db2ca4997657a7746928	Hashmasks	HM	erc721
\\xf2febe0fc74ebea939240851686d5b5455d200ab	PunkBabies	PNKB	erc721
\\xc3f733ca98e0dad0386979eb96fb1722a1a05e69	Official MoonCats - Acclimated	😺	erc721
\\x79986af15539de2db9a5086382daeda917a9cf0c	Cryptovoxels	CVPA	erc721
\\x3b3bc9b1dd9f3c8716fff083947b8769e2ff9781	Arabian Camels	CAMELS	erc721
\\xec9c519d49856fd2f8133a0741b4dbe002ce211b	Bonsai by ZENFT	BNSI	erc721
\\x12f28e2106ce8fd8464885b80ea865e98b465149	BEEPLE - GENESIS COLLECTION	BEEPLE	erc721
\\x10daa9f4c0f985430fde4959adb2c791ef2ccf83	The Meta Key	MetaKey	erc1155
\\x76be3b62873462d2142405439777e971754e8e77	Parallel Alpha	LL	erc1155
\\xd754937672300ae6708a51229112de4017810934	Deafbeef V2	DEAFBEEF	erc721
\\x2e734269c869bda3ea6550f510d2514f2d66de71	StrongBlock NFTs		erc1155
\\xc0cf5b82ae2352303b2ea02c3be88e23f2594171	The Fungible by Pak	THEFUNGIBLEOPENEDITIONSBYPAK	erc721
\\xbc17cd7f1a58bda5d6181618090813b3050416b5	Framergence	FRAM	erc721
\\x85f0e02cb992aa1f9f47112f815f519ef1a59e2d	Polychain Monsters	PMONC	erc721
\\x905b180268f2773022e1a10e204b0858b2e60dcf	Pulsquares	PULS	erc721
\\x57f1887a8bf19b14fc0df6fd9b2acc9af147ea85	Ethereum Name Service (ENS)	ENS	erc721
\\x1eff5ed809c994ee2f500f076cef22ef3fd9c25d	PEGZ	PEGZ	erc721
\\x0120b3add758e0b7f667eae25fc14c3326d44d12	Meta Bots	MBOTS	erc721
\\xc4749f416c7dc27e09f67ac02f23a90e0ba6ad21	Aetherian Deed		erc721
\\xfbeef911dc5821886e1dda71586d90ed28174b7d	Known Origin	KODA	erc721
\\x6ac07b7c4601b5ce11de8dfe6335b871c7c4dd4d	Urbit ID	AZP	erc721
\\xf980759616a795b2d692a5c0a0f1bad651984bc1	Somnium Space VR	WORLD	erc721
\\x4530ed5907ceb4e14b0550a28e7d300cc773b92e	Exoplanets. The 4001 Project.	EXOP	erc721
\\x9030807ba4c71831808408cbf892bfa1261a6e7d	GRAYCRAFT	GRAY	erc721
\\xec031b8abe78ecab0a6636b92af53d7013a97a37	SatoshiFaces	FACES	erc721
\\x63c0691d05f441f42915ca6ca0a6f60d8ce148cd	BEEPLE: EVERYDAYS - THE 2020 COLLECTION	BEEPLE2	erc721
\\x0e3a2a1f2146d86a604adc220b4967a898d7fe07	Gods Unchained		erc721
\\xf126eb8284110a37c18e726a7e3b9fc21e68e897	FEWOCiOUS	FEWO	erc721
\\x9ad431c6253baa3e909eef955b7292d1ec63428e	HashGarage Minter	HGM	erc721
\\x72bb198baab62e1f1f6b60d2bb37c63a303a58ad	Antimasks	ANTI	erc721
\\x06012c8cf97bead5deae237070f9587f8e7a266d	CryptoKitties	CKITTY	erc721
\\xbd8d13d3116a40d754a64f36dffe6d815fb2617b	4BULLS.GAME V2	4BGAME	erc1155
\\xe3435edbf54b5126e817363900234adfee5b3cee	Voxies	VOXIES	erc721
\\x7c40c393dc0f283f318791d746d894ddd3693572	Wrapped MoonCatsRescue - Unofficial	WMCR	erc721
\\xe4605d46fd0b3f8329d936a8b258d69276cba264	Meme Ltd.	MEMES	erc1155
\\x32a984f84e056b6e553cd0c3729fddd2d897769c	MillionPieces	MILLION-PIECES	erc721
\\x7cbf474bf3e354a536804ce6aa1804d1eaf478e9	Genuine Human Art Contract Collection	GHArt	erc721
\\xa9cfc59a96eaf67f8e1b8bc494d3863863c1f8ed	DeNations	NATION	erc1155
\\x4ad2d817b60db6ee79c1ae016429251a0d25423f	CryptoTrunks	CT	erc721
\\x97ca7fe0b0288f5eb85f386fed876618fb9b8ab8	Ether Cards Founder	ECF	erc721
\\x6fc2ded385e6f848f614b729ada686772f90e514	MeowBits Collection	MEOW	erc1155
\\x8b2bb0773ffae3f5aed484a929ed81f728c908dd	RealCryptoPunks by VT3.com	RCP	erc721
\\x73da73ef3a6982109c4d5bdb0db9dd3e3783f313	Curio.Cards		erc1155
\\x401b3474a0a2fbcb00d516c1ffc01854b9d9dafe	The BondlyVerse Elite	TBE	erc1155
\\x8280d56ac92b5bff058d60c99932fdecdcc9441a	BCCG	BCCG	erc1155
\\x57a204aa1042f6e66dd7730813f4024114d74f37	CyberKongz	KONGZ	erc721
\\xc36cf0cfcb5d905b8b513860db0cfe63f6cf9f5c	Town Star		erc1155
\\x213a57c79ef27c079f7ac98c4737333c51a95b02	pplpleasr V2	PLSR	erc721
\\xcd1dbc840e1222a445be7c1d8ecb900f9d930695	RTFKT x JeffStaple		erc1155
\\x0427743df720801825a5c82e0582b1e915e0f750	0xmons	0XMON	erc721
\\x3c28de567d1412b06f43b15e9f75129625fa6e8c	Justin Aversano - Twin Flames - Collection of 100 Twin Portraits	Justin Aversano	erc721
\\x2a2ad5a2eacf35194d39afe1d727c39710ff0379	ChainGuardians	CGT	erc721
\\x31385d3520bced94f77aae104b406994d8f2168c	BASTARD GAN PUNKS V2	BGANPUNKV2	erc721
\\x7f72528229f85c99d8843c0317ef91f4a2793edf	1111 by Kevin Abosch	1111	erc1155
\\x6cd2abb5d4cd191236d2ed4f0612dcfe9e98844a	AxieChat	AxieChat	erc1155
\\x9126b817ccca682beaa9f4eae734948ee1166af1	BASTARD GAN PUNKS		erc721
\\x2216d47494e516d8206b70fca8585820ed3c4946	Waifusion	WF	erc721
\\xd1e5b0ff1287aa9f9a268759062e4ab08b9dacbe	Unstoppable Domains		erc721
\\xafba8c6b3875868a90e5055e791213258a9fe7a7	VoxoDeus	VXO	erc721
\\xd3f152a5dbd3b932b751f50df6e7b5939bb3ed9a	HAIL DRACONIS!	HAIL	erc721
\\x5e86f887ff9676a58f25a6e057b7a6b8d65e1874	Bitchcoin		erc1155
\\x778cc248cdddfd926bfba49850098eac16b0d12a	MegaCryptoPolis		erc721
\\x1da1d8af678d70b97aa827627bec102f4abc189c	Zedd x Silly Gabe x Fvckrender	ANTIPODEBYZEDDXFVCKRENDER	erc721
\\x617913dd43dbdf4236b85ec7bdf9adfd7e35b340	MyCryptoHeroes	MCHL	erc721
\\xc375f1056f30f26a6915f67f0a19b9476f8937dc	Infinethum	INF	erc721
\\x0b0f6bc78ea9fb88dd58fdfe4c03f0c78721f649	J48BATRINKETS	TRINKET	erc721
\\xeff8450c6a69217bf98f95143a8c76166b25164d	Rosen Big Drop	Rosen	erc721
\\xf3e778f839934fc819cfa1040aabacecba01e049	Avastars		erc721
\\xd4835ff99da97ea688ddaea7b9e2119df02ccbc1	SHIKI31	SHIKI31	erc721
\\xfd04e14334d876635e7ea46ae636d4a45d8ffdde	XCOPY	XCOPY	erc721
\\x6e4c6d9b0930073e958abd2aba516b885260b8ff	Influenceth Asteroids	INFA	erc721
\\x5a654e6ae9d33fbf9b1295dfc3799007302527ca	Arteon Graphics Card Genesis Edition	GENESIS	erc721
\\x2250d7c238392f4b575bb26c672afe45f0adcb75	FEWOCiOUS x RTFKT	FEWOWORLDOPENEDITIONBYFEWOCIOUSXRTFKT	erc721
\\x2d820afb710681580a55ca8077b57fba6dd9fd72	Artifex	ARTIFEX	erc721
\\xdd991ec0a7cd7c7334e3e4553aa5e4fb850168d9	D'EVOLs	devols	erc721
\\xc0b4777897a2a373da8cb1730135062e77b7baec	Sevens Genesis Grant	ART	erc721
\\x1db61fc42a843bad4d91a2d788789ea4055b8613	Chubbies	CHUBBIES	erc721
\\xba8cdaa1c4c294ad634ab3c6ee0fa82d0a019727	PolkaPets TCG	PPBS	erc1155
\\xb798a123fe75292c3873768d01d7f5fc79e05efc	Roots of Rage by ZENFT	RAGE	erc1155
\\x4a1880e0916b84d65b04e06e795651368cd31ee4	EM! Collection	EMI	erc721
\\x342dfd3da656e5dd76968867cb06c09f43e97c37	HINOTION CURATED	HINO	erc1155
\\x0a058afb26d6b0e7d3c2e557b7fd0baa90e59a9f	Ballistic Romance	BLRM	erc1155
\\x067ab2fbdbed63401af802d1dd786e6d83b0ff1b	NFTBoxes	[BOX]	erc721
\\x3e34556b7d6a6c79320168140e14c10d7a1febb7	Daniel Arsham	ROME	erc721
\\xb6dae651468e9593e4581705a09c10a76ac1e0c8	Async Art	ASYNC-V2	erc721
\\x5108d8c8efbd392ec093701d6215f580380d6d18	P4L	P4l	erc1155
\\x4813c06eb9919db20634c431565d6b88f35501ff	REDLIONEYE GAZETTE	rlesubs	erc1155
\\x2998d346c66259e3e73dd7410899948371a28e94	Unique One Multiple	UNE	erc1155
\\x32d813d74836f2054a8820560a8b5604a2697360	Jake's World	JAKE	erc721
\\xc9eef4c46abcb11002c9bb8a47445c96cdbcaffb	Nametag	NT	erc721
\\x082903f4e94c5e10a2b116a4284940a36afaed63	Pixls Official	PIXL	erc721
\\xc7e5e9434f4a71e6db978bd65b4d61d3593e5f27	Alpaca City		erc1155
\\x5c3daa7a35d7def65bfd9e99120d5fa07f63f555	HEX TOYS	TOYS	erc1155
\\x09d2a34aa67b407a925b1b67536bfad80b375306	The VIDEO Store		erc721
\\x6f7623663a651cccafe15ab7b8e8b7d3504aaae9	Crypto from Space	CFS	erc1155
\\xc7ba3955a6f2053701d33cbff8f5540385d92a38	Wang Changcun	AYRTBH	erc721
\\xc78337ccbb2d08492ec152e501491d3a76cd5172	J48BAFORMS	JABBA	erc721
\\x69501ad818637ff86bba0c92add782f8bae57cb2	Steven Baltay	THEJOURNEYBYSTEVENBALTAY	erc721
\\xc67b4203b42fa1bec5a80680ff86f8c23e2ee812	NFT Fuck Bubbles	NFTFB	erc721
\\x7cdc0421469398e0f3aa8890693d86c840ac8931	DokiDoki Degacha Collection	MOMIJI	erc1155
\\xb32979486938aa9694bfc898f35dbed459f44424	Nyan Cat (Official)	NYAN	erc1155
\\xe7afb4189603a901b74f8085f775931a60996166	eBoy Blockbob	BLOB	erc1155
\\xd227df494367a8b769b4bf2bbebd0388a8758741	Degen'$ Farm	CREAT	erc721
\\x1e4f8365ebfc13702dd767ef4889fbfda4c0a43d	Battle Racers		erc20
\\x8b459723c519c66ebf95b4f643ba4aa0f9b0e925	YFU Cards	YFUC	erc1155
\\x9f9c171afde4cc6bbf6d38ae4012c83633653b85	Polyient Games Founders Keys	PGFK	erc721
\\xafb44cef938b1be600a4331bf9904f6cec2fcac3	EllioTrades NFT Collection		erc1155
\\xfce4bf28ab33a38bfb8bf3604ae6f09fa78ff6ce	Mad Dog Jones	MDJ	erc721
\\x9c008a22d71b6182029b694b0311486e4c0e53db	Apymon	APYMON	erc721
\\x580a29fa60b86aaff102743de5cba60bb5f9de75	RadiCards V2	RADI	erc721
\\xe4597f9182ba947f7f3bf8cbc6562285751d5aee	SuperFarm Genesis Series		erc1155
\\x026eb4b7857bab5ed48570c90237561d7706eaff	lsd - VE2y1GQlTd	lsd	erc1155
\\x6e29503bbc237ed8dc472c766a7415b4cfb226b2	Lympo	LYMPO	erc1155
\\xc92ca2b5b8a996ad2a6fdd97c6d7ed038e61c725	SSX3LAU	IRIDESCENTOPENEDITIONBYSSXLAU	erc721
\\xd9a3fca910f76c82be37c0e78dd2fc435ff2a77a	Ethersparks	ETHERSPARKS	erc721
\\x6fa769eed284a94a73c15299e1d3719b29ae2f52	Brave Frontier Heroes		erc721
\\xa383d6bb58e0583bb6cee40f3c442e1b16e6958f	E1337	E1337	erc721
\\x5397e20a4d19ff2579e257c15a3daee05c4c3f53	Superchief Gallery NFT	Union Sq NYC	erc721
\\x068f74749c24a42058563035f8c786362fc96494	Fabricated Fairytales by FEWOCiOUS x parrott_ism x Odious x Jonathan Wolfe	FABRICATEDFAIRYTALESOPENEDITIONSBYFEWOCIOUSXPARROTTISMXODIOUSXJONATHANWOLFE	erc721
\\x2d6e10561b320c4f31a903bf0fa92a1ed58637c0	War Riders		erc721
\\x83c797f35d818a46c28b3b1b7b382d50a5e085a2	NEXUS NXS	NXS	erc721
\\x4ae57798aef4af99ed03818f83d2d8aca89952c7	Rare Pizzas Box	ZABOX	erc721
\\x9ef754b35b1ec880f091b923bb22550ff98f2df1	CypherBot V2	CBOT	erc1155
\\xfcae6b3c114dcb6b4793c6375707f850710b10a7	Metaheads	MH	erc1155
\\xc1caf0c19a8ac28c41fe59ba6c754e4b9bd54de9	CryptoSkulls	CryptoSkulls	erc721
\\x2aeaffc99cef9f6fc0869c1f16f890abdfcc222b	League of Kingdoms	LOKR	erc721
\\x818737eec8a5350756da40d5ddafda8a84ade107	Crypto stamp		erc1155
\\x40bd6c4d83dcf55c4115226a7d55543acb8a73a6	Ainsoph V2	ALEPH	erc721
\\xd08f2d720e0d00ff92c7f9ab2073cee4c208cee2	CryptoSnake Coins	CSC	erc1155
\\x7ccdc136619cddf744122a938b4448eda1590fe1	The Creation		erc721
\\xed9e2b88feb7a94a682e7e3a59631e15e8442711	Mogul Productions		erc1155
\\xf5dee1416988ec404000c6d8626910ecb3f62d6c	Max Osiris Legacy		erc721
\\xe68e6e7e805b45fa8bbda2f514a3456b0aa8294a	Distorted Reality - g6uvyQCJyY	ART	erc721
\\x2ffee6e2a7c30795933153e2400e470a47e04745	CUTE ALIEN V3	CA	erc721
\\xba35430ffd0313d83f87317e9024293128eb98d7	Sunday Soundbites with Satman	Sat	erc1155
\\xe19edd187a5842166daa125cfa95aaf2b19f792a	Giant Swan	SWAN	erc721
\\xf90aeef57ae8bc85fe8d40a3f4a45042f4258c67	The SURF Shop	SURF	erc721
\\xb772099b6312a9795f6a6cc4ed2324b7660d9ce2	Fvckrender	FVCK	erc721
\\x60f3680350f65beb2752788cb48abfce84a4759e	Colorglyphs		erc721
\\x69048fe9b3dfdf9fa6420617fa1acf2619696694	MOON - sNQFdIXtO1	⚈	erc1155
\\x63984ef7a91ea69cdad6c7071890ee4d2fccab4b	My Head In Your Hands by FEWOCiOUS x Two Feet	MYHEADINYOURHANDSOPENEDITIONBYFEWOCIOUSXTWOFEET	erc721
\\x3d34d2a719c2666e603650858ac159d12e623b77	R66 Toys	R66.Toys	erc1155
\\x68ad300954e0750138cdc1da93aa40ddf217303c	LuckyPiggy Token	LPT	erc721
\\xa5efd7a7b3bb0de53bc3f2aa59004722d1431a15	Crypto Geishas	CG	erc1155
\\x747b1606da0adf2a00c3fba0204b0ea6f2047244	Hash Garage	HGS	erc721
\\xdf964d2ba881d546f404def959aa02eba455ee7a	successleavesclues	SLC	erc1155
\\x374d6f9fb661f8b801f1a1ea7f4e89d83bffd028	bitDOTs	BitD	erc1155
\\x8da0b54dfe61ef2650fdfbde440d0d3983b7a0f9	Cometh Spaceships	MSF	erc721
\\xe6c811804b505915017c67380d93be4f245b5a2b	Kittball	KITT	erc1155
\\xf4680c917a873e2dd6ead72f9f433e74eb9c623c	Twerky Club	TWYBGS	erc1155
\\x1410460d9e4b5c8d34314850a2db3957aeaef23b	CryptoDates NFT	DATE	erc721
\\xce5b23f11c486be7f8be4fac3b4ee6372d7ee91e	BitsForAI		erc721
\\xa82f3a61f002f83eba7d184c50bb2a8b359ca1ce	Phunks	PHUNK	erc721
\\x360c33d6c134c04c3af5096f38d26f478bb60620	3D Baby Punks	3DBBPK	erc721
\\xcf62f98e15db91c1500b294454663beaf9c73753	PocketRooms	PKT	erc1155
\\x3d0c4e9bde15f2681902f9f291b3c905b7ea46f9	Gener8tive K-Compositions	GꞢ	erc721
\\x22a40a30b5e581f5bdc6afea7afedb3fa735d9d9	Digital Trash		erc721
\\x9227a3d959654c8004fa77dffc380ec40880fff6	Spells of Genesis		erc721
\\xb20217bf3d89667fa15907971866acd6ccd570c8	Polka City 3D Asset	PC3D	erc721
\\x729cd6226751279030757f61b2cac4798c949fa1	Lux Cards	Lux Cards	erc1155
\\xf1e0beca4eac65f902466881cdfdd0099d91e47b	Ape Only	APES	erc721
\\x7746e0989d4ba2a033ad1d1f09630e29222b15d3	Rare Cassettes	RCassettes	erc1155
\\xa75a3f5b447d3d929e5b2ca157cfe7046bc15b37	WAIFU Token	WAIFUG2	erc1155
\\xb7f7f6c52f2e2fdb1963eab30438024864c313f6	Wrapped Cryptopunks	WPUNKS	erc721
\\x505df5ace201098b1d198aad048cb61c9f0246d4	SEX	SEX	erc1155
\\xe79c969200a17f4ca8cec6780780a564ccdb3038	The Mop'eds	¯\_(ツ)_/¯	erc721
\\x73f6056ec5c6b131d94b59f8dada727af92b2c91	Tweaks	TWEAK	erc721
\\x96ea1251452fa1f74c5b92ee29fce8f10a8a669e	ProjectWITH	PWN	erc721
\\x4222b2a98daa443c6a0a761300d7d6bfd9161e52	Illuvium	ILV-NFT	erc1155
\\x85f740958906b317de6ed79663012859067e745b	The Wicked Craniums	TWC	erc721
\\x876295342f2cda8d83e05f395063efa676535c43	Legends Of Cryptonia	CRYPTONIA	erc1155
\\x9abb7bddc43fa67c76a62d8c016513827f59be1b	POW NFT	POW	erc721
\\x171b76d8cb082cc52458856cbb3e5908e6a1fa19	Antoni Tudisco	CRYPTONIOPENEDITIONSBYANTONITUDISCO	erc721
\\xc36442b4a4522e871399cd717abdd847ab11fe88	Uniswap V3 Positions	UNI-V3-POS	erc721
\\x8fda42090a5ac9dde01fd2ba0431fe22fc72dc65	OptionRoom Genesis		erc1155
\\x82262bfba3e25816b4c720f1070a71c7c16a8fc4	Solvency by Ezra Miller	SOLVENCY	erc721
\\x6e0baaa0d1c11dc160143c0c7213df76acdf4e17	Apymon Monsters	APYM	erc721
\\xf849e438893e2b2a591bd8e3e42e401adeb2e352	Unidentified contract - My0o0IrFGC		erc1155
\\xe529178bf1ad4f8e01f09037a3c6e96131cff5f3	ROBNESS PRINTS	RBNS	erc1155
\\x38f17c1a95561b80393d65e1b528742ebd40da45	Crypto Airships	airships	erc1155
\\x51e613727fdd2e0b91b51c3e5427e9440a7957e4	Crypto Corgis	CORGI	erc1155
\\x4d3814d4da8083b41861dec2f45b4840e8b72d68	CryptoSpaceCommanders	CSCNFT	erc721
\\x77959e650500872c9599c66e8e8086103f51c8d9	crypto pepes	PEPES	erc1155
\\xe3f0d3c68faa6bc752b820fda3de4cbbc8a18b2b	THE SUMPSONS	SMPS	erc721
\\x97cc6bc80fe6c88f054e04593081e9ff9881e364	Tokens Equal Text	TET998	erc721
\\x7243c264db251ffdcbdcf0fc4f990054c893fefa	Unidentified contract - YaKgqDeHkx		erc1155
\\x48b63941f12cb14a41afc062af8ecef84a58b940	Skulls On ETH	SOE	erc721
\\x02f03ef74bf40d11b96ce2961cc2bb58630754ba	Everything by 3LAU	EVERYTHINGBYLAU	erc721
\\x3cd2410eaa9c2dce50af6ccab72dc93879a09c1f	Charged Particles - LEPTONv2	LEPTON2	erc721
\\xd3eb68e89baffc42265352f9beae6ce34e0ec142	Bearz	brz	erc1155
\\x4928466e7a766c3c40ea5d41deb4f1c8ff7a32ab	Ksoids	KSOIDSPACK	erc721
\\xc6c11f32d3ccc3beaac68793bc3bfbe82838ca9f	CryptoBonds	CBOND	erc721
\\x417cf58dc18edd17025689d13af2b85f403e130c	Crypteriors	Crypterior	erc721
\\x0825f050e9b021a0e9de8cb1fb10b6c9f41e834c	LetsWalk	#LetsWalk	erc1155
\\xa8abf045fe1a9ef0583e436393a6e4e0b483f717	CryptoSpells		erc721
\\xc57fe30ca31ff25c74608d7be6433f11c1b2be50	CRYPT0xPINS	PINS	erc1155
\\x47ccad36ae77ab963746c8db8ad301d48235ce81	PROOF OF {ART}WORK	POAW	erc721
\\xec947c734a7fd3b23f49c9b7acca09b89e59bfc3	CryptoJars	JAR	erc1155
\\x76ad70096b373dce5c2bf44eb9a9f8ecbb1c0b93	MoonHeads		erc1155
\\xfc03bbdb02d64dea4ba20f9b0e275ea2f6b2bf48	RATS V4	RATS	erc1155
\\xb3faf6e8af293bd2f87662e785b06ccd35af03bc	WEIRD	WEIRD	erc1155
\\x5d00d312e171be5342067c09bae883f9bcb2003b	Ethermon	MON	erc721
\\x6ca3f528fab2748c4032be479e2177e4177b0833	LooserGuys Minter	LSM	erc721
\\x99981b8d6722893fa93e42a1b1e814ceddef62b1	NiftyPins 721 Collection	NIFTYPIN	erc721
\\xc1def47cf1e15ee8c2a92f4e0e968372880d18d1	CryptoAvatars	AVNFT	erc721
\\xf8fde75740814c6fadcc75d2b56ad2d8ec28e08e	Cartwars	CWars	erc1155
\\xce4f570801a50ee1833b7480ef16c4fd1b3caa8f	munichNFT	MUC	erc721
\\x0574c34385b039c2bb8db898f61b7767024a9449	JOYWORLD JOYtoys	JOYtoy	erc721
\\x46c31b6b330c4522d6b37cb3cecdf2da9ff46f61	Etherpoems	ETHP	erc721
\\x22c1f6050e56d2876009903609a2cc3fef83b415	POAP	The Proof of Attendance Protocol	erc721
\\xfaff15c6cdaca61a4f87d329689293e07c98f578	Zapper NFTs	ZPR_NFT	erc1155
\\xd3f69f10532457d35188895feaa4c20b730ede88	RTFKT Capsule  Space Drip	DRIP	erc721
\\x9619dabdc2eb3679943b51afbc134dec31b74fe8	Ethermore	ETE	erc721
\\x0cb53474ac2939b5797aaf397474d44eba5872d9	CryptoAmulets	AMULETS	erc721
\\x585a2c37858d3b03824bc683829e4dbbf58969ee	CryptoJunks.wtf	JUNK	erc721
\\xf6e716ba2a2f4acb3073d79b1fc8f1424758c2aa	Sevens Genesis Grant Nominees	ART	erc721
\\x5457ef2d4e248d62b7194f57901361024af9a3f0	Alotta Money		erc721
\\x98e547837eaadb188c88ee31616df85b847fb64d	DISAGREE ART Collection.	DISA	erc1155
\\xd1a0e534091b02eaf52b3173a1108e2b8be5d7a9	3LAU - Dont Worry	3LAUDONTWORRY	erc721
\\xa58b5224e2fd94020cb2837231b2b0e4247301a6	Cryptovoxel Wearables		erc1155
\\x497457437e0444e1cf459d9025890949215d2383	Holo Things	HOLO	erc1155
\\xa9fdb3f96fae7c12d70393659867c6115683ada0	CryptoFoxes	CFXS	erc721
\\x61bd6b10c7bf3e548f8659d016079e099510a4dc	Rarebit Bunnies	RAREBIT	erc1155
\\x34388e53113fcf84c85c17b719c3c73ef202051a	TYSO Cards	TYSO	erc1155
\\x9c92f6761473ebc5ada625f2f699fe7e053f572a	CryptoPortals V2	Portals	erc1155
\\xefb43cee005f8c56eddd81ab8058a37b7558a534	NFTY Labs	NFTY	erc1155
\\x39fe518928ac82a33ee8d1038e16223cdfbb0d99	HaruKomoda	HARU	erc721
\\x1b8fbb35393bbbd82caeb0087fae30d887e4b880	Bitverse	BPUNKS	erc721
\\xb3307dca37a2b71df435a9789a7a98e2cb52f6f7	Satoshi Gallery x Hype Art		erc1155
\\x1367963d0fb938b14f073d12101f1157536f3875	Superplastic x Janky x Guggimon	JANKYASSCRYPTOCOLLECTIONBYSUPERPLASTICXJANKYXGUGGIMON	erc721
\\x2a187453064356c898cae034eaed119e1663acb8	Decentraland Names		erc721
\\x64e6e7163717a4b09412a38345d05488020919fb	Justin Roiland	KRIGGLESBYJUSTINROILAND	erc721
\\x6d8cc2d52de2ac0b1c6d8bc964ba66c91bb756e1	KINGS OF LEON x YELLOWHEART NFTS	KLNFT	erc721
\\xcc9f683c2fdbed1801bc8d68a663577d4f3259cd	Doki Doki Finance	DOKI	erc1155
\\x86cf785a64f93c99549e4de9cdd29825ca22f42a	I Want My NFT V2	FLIX	erc1155
\\x8e49000dff43b131d6f2e0e8cc3d081476f068dc	Excerpts of Gridlock	HROG_E	erc721
\\x1dd2c47496fbd9d38ac9a884d15d816b062e1f6e	ETH-MEN Legacy	E-M	erc1155
\\xd6a30176bb3bc72385d517ea9d44970a06214fdf	adidas		erc1155
\\x7f96f68fa766d4e9b037417fe4967511152b9272	SOLOS	SOLOS	erc721
\\x5351105753bdbc3baa908a0c04f1468535749c3d	RUDE BOYS	RUDE	erc1155
\\x1986f4c2e3ad5fe9778da67b1b836cf53b9e20cd	Pokemon Salute	TCG	erc721
\\x66018a2ac8f28f4d68d1f018680957f2f22528da	Etherland	LANDID	erc721
\\x2f167764891ef106dda002d61039edcd2823aee4	Vintage Ships	VS	erc1155
\\x1c6b549594da651de717e1d327fae11fef1503a1	moonholders	MOON	erc1155
\\xdeae8ab87bb8aaec74effa54483817c12b4932c3	NFTats	TATS	erc1155
\\x34037a293ffb6e29057ea7f36a453a39dccd130a	Drippies	Drippies™	erc1155
\\x33b83b6d3179dcb4094c685c2418cab06372ed89	ETH-MEN RELOADED	EM2	erc1155
\\x1cfcc172a9ddcdf91b16d990cc0857c9b027e592	UAM - Street Art	UAM	erc1155
\\x467439207d53530505e5eb40edd6debbaff3e063	Mars Genesis	MARS	erc721
\\xc541fc1aa62384ab7994268883f80ef92aac6399	RTFKT Capsule  Space Drip 1.2	DRIP	erc721
\\x75a0a708c65956508160cbd913134f731c0b3b65	WEEB3	WEEB3	erc1155
\\x82fdd32e998536609b624e033a16a879d2ebefe5	Goddess	GODDESS	erc1155
\\xf807c903a68fda037ff93e25dfb6254ff2ccdc83	NEKOPARA Collection	NEKOPARA	erc1155
\\x99b9791d1580bf504a1709d310923a46237c8f2c	Million Dollar Rat	MDRAT	erc721
\\x8853b05833029e3cf8d3cbb592f9784fa43d2a79	Codex Record	CR	erc721
\\x4f41d10f7e67fd16bde916b4a6dc3dd101c57394	Flowerpatch	FLOWER	erc721
\\x3cae99a6e0bb3ea6a497598f30d46478cc1039a4	LORD ANTHONY APEALOT AUDIO BOOKS	LAAAB	erc1155
\\x8f65d845d795fc3381fef0525e283cfe67358678	CryptoCreepsNFT	CREEP	erc1155
\\x5628d3eec4349e70541721bfc9f6ef7d70969d1b	Metis Shard NFT		erc1155
\\x4995ff1c49baa6576029b7398479068ba664449c	EtherThings	ETHERTHINGS	erc721
\\x569406401da35a393cfcc69d9a82111691892a3a	Weensy Meme Cards	MEME	erc1155
\\xdbd4264248e2f814838702e0cb3015ac3a7157a1	TradeSquads	TS	erc721
\\x838061a398029f1fab6bd00692fb1182501baab2	Diplo x FriendsWithYou	CLOUDCHARITYDROPBYDIPLOXFRIENDSWITHYOU	erc721
\\xd23043ce917ac39309f49dba82f264994d3ade76	Meme Factory	MEME	erc721
\\x998d7ca377d0b74f8f5d390a0518db2f92f7270b	Dogecoin Journey I	DogeJI	erc1155
\\xf2407e08c0b9948800d6675e7d549b2d7761443b	The UA Collaboration.	TUAC	erc1155
\\xd2d2a84f0eb587f70e181a0c4b252c2c053f80cb	Toshimon	ToshimonMinter	erc1155
\\x91047abf3cab8da5a9515c8750ab33b4f1560a7a	ChainFaces		erc721
\\x0044840e9b4e6a08cba475cc89ced69ec3381266	Johnny Dollar Limited Editions	J$-s	erc1155
\\xf1e3af9152ad1f93be2ab2f8766d41744fad823a	CryptoGogos	GOG	erc721
\\xf78b3aff2a32de2aa89ed724d6daa38ba497e270	BlockMan ChipTunes	CHIPTUNES	erc721
\\xb80199a27dba0f91b422f0a5afaf02eacc004e51	Elvin Gnome	GNOMES	erc1155
\\x5c1749bc5734b8f9ea7cda7e38b47432c6cffb66	Gods Unchained Collectibles		erc721
\\xe5545fa0636a82c0b37c7db62e2104e69a11d062	Genbit Bunnies	GENBIT	erc721
\\x54e0395cfb4f39bef66dbcd5bd93cca4e9273d56	Alchemist Crucible v1	CRUCIBLE-V1	erc721
\\x86c4bb362e90bd859ac60dacd7c7a02954ceda63	Franklin Mint V3	FMNT	erc1155
\\x44d6e8933f8271abcf253c72f9ed7e0e4c0323b3	DRM - Dont Rug Me - Cryptocurrency CCG Official Cards	DRM	erc1155
\\x3b3ee1931dc30c1957379fac9aba94d1c48a5405	Foundation (FND)	FNDNFT	erc721
\\x95f862fba8879a2795a67ae556eefc3bd6302bce	Cryptimon	FXIV	erc1155
\\x206e6fbb93e45841f6ed0d88cc63fad4ed3bcebe	 TESSELLATION	TESL	erc1155
\\x24b3345ae9928c9a4bcd93f8539792d7468d396a	Jose Delbo	DEATHFULLMOONBYJOSEDELBO	erc721
\\xcb6768a968440187157cfe13b67cac82ef6cc5a4	PepemonWorld	PEPEMON	erc1155
\\xcbde56007bc63bab181071adc31d339cad9c22af	Tohoku Zunko and Friends Special Limited Edition NFT	tZNK	erc721
\\xf7a6e15dfd5cdd9ef12711bd757a9b6021abf643	CryptoBots	CBT	erc721
\\xdeedc630afd7f9a939dfd49d7bc9060ad6fd772c	CryptoDracula	CDRA	erc721
\\x2a6e931c1bf12ea508402f1faf86c4c4c08aad18	Digital Zoo Gallery	DZG	erc1155
\\xd06f51965a7ff3d33f47e216b617b2eaab02215e	Second Realm Studio		erc721
\\x0db8c099b426677f575d512874d45a767e9acc3c	reNFT - Genesis Cards		erc1155
\\xba71ab50e15abcb258c322cd57640f77a01f8710	Its Louie Baby	ILB!	erc1155
\\x87d598064c736dd0c712d329afcfaa0ccc1921a1	CryptoFighters	FIGHTER	erc721
\\xa7a05e655cbed5356d2fa851e96f7f68e4a6f954	REVV Motorsport	REVV-I	erc1155
\\xfc7380e9c58b4ce947a33550a7a41443b0d9a554	Curren$y Digital Collectibles	CDC	erc1155
\\x4243a8413a77eb559c6f8eaffa63f46019056d08	Cryptovoxels Names	NAME	erc721
\\x78d551cd9e5ad303b6bb60df75e21ad22480de93	Tokenized Photo & Art	TPA	erc721
\\x43fcf9b20886f2d184b8176b433831570164b011	Trevor Lawrence x Topps Chrome	TLTC	erc721
\\xe47e91fa8d76128cb680c845945377bf0d00a3bb	Ozuna x Orlinski	OxO	erc1155
\\xb9291c9909734ccb206ba1ef0d64892336757172	Baeige	ROOMSPACKSBYBAEIGE	erc721
\\xe1d05ad0de9690cb553bde0705bccd1ee983f924	Degen Farm Lands	LAND	erc721
\\x1d963688fe2209a98db35c67a041524822cf04ff	MarbleCards	MRBLNFT	erc721
\\x6b7a2fa07706d5c18f07acb163fb33270b0ca41b	Minerva	MNRV	erc721
\\xabefbc9fd2f806065b4f3c237d4b59d9a97bcac7	Zora	ZORA	erc721
\\x464e8c44b8a326ddf5505819b1dca5199f75750f	Cats Will Eat You	CWEY	erc721
\\xdb4a62d58220db3c23d63c7887fb7b9208018425	Mankind	SPECULATIONOPENEDITIONSBYMANKIND	erc721
\\x46cec00d100b63b6447d3475f27894aa16894966	Crypto Kid	CRK	erc1155
\\x21e89016b10e3c46f77a86325e942e5c53813b2d	-GLOWA-	GLW	erc1155
\\xc2610cfa7e117390fe0daec4096f0e8af000db5c	NFT Art	NFT	erc721
\\x88000cd082328e1627f09db72b7762572565afc7	Erto Collectibles	ERTO	erc1155
\\x2ceb85a2402c94305526ab108e7597a102d6c175	Fortune Teller Collection	TNFT	erc721
\\x36ba769a5dad72a7a1add8a4b22eb407093b5f8d	prokopevone.eth		erc721
\\xb8a15c37af224c9ae5bccf84bc4cb46707ace3a3	The CryptoPoops	POOPS	erc721
\\x6b111b58bef7723dbaf9f7c5245667a685854a9f	Trip Kitties	TRIP	erc721
\\x07d501ead494cbec63ce035636d3dcf1ff830570	CarBits	CARBITS	erc1155
\\xe9509e17c40642f4c86fffd6cd44ba918c2e1d63	KeikoKitahara	KEI	erc721
\\x0e92164df9bc46855c9476384abc4e8fd2c47edc	Porphy	PHY	erc1155
\\xc41764b1f33e186ae5a3bc6a49134b51722531f0	GYB’s Club Nifty		erc721
\\xe39a238d74bdd95a895026fc25ec97fb8a4b1959	Spike	spke	erc1155
\\xfa1448843a03f2fc9a8c7ce7c87b5a5dab3e000a	Doctor Who - Worlds Apart	DWWA	erc721
\\x2edd5dca914637ad2842d38eca6a6350ef854cf9	KamaGang	KAMA	erc721
\\x771770f23e0947898d519d83735311cc41d29758	NOFF	NOFF	erc1155
\\x1adaa23a3a5255bf9052c8a8f56354f3215b795b	CONTINUUM 020	CNT	erc721
\\xba288cc1aa5f7de8a6b3db963dee332e3532cf12	CRYPTOWHEELS	$CWHLS	erc1155
\\x8a259ef311ee873d4f4915edab2835206191cd7a	RoboBits	RBTS	erc1155
\\x9b56b89e5d45ea4abe8565423790544f83a480fc	Ethernity's Master Collection		erc1155
\\xac1aee5027fcc98d40a26588ac0841a44f53a8fe	WORD		erc721
\\x985af49e515488c6556e3d9d24d464ce95ea3088	MotoGP NinjaStickers	MGPNS	erc721
\\x677b1ab2ad398e2afe93cc9a91b9dcaabf3d702c	ETH-MEN Exclusive	EME	erc1155
\\x9e1f3e8db4d1119894624632499eaed1e56d2b1d	PixelChain	PXCM	erc721
\\x8c9b261faef3b3c2e64ab5e58e04615f8c788099	MLB Champions	MLBCB	erc721
\\xafae97dc068735bf4592d1b8a731080cc0015691	The Weeknd x Strangeloop Studios	ACEPHALOUSBYTHEWEEKNDXSTRANGELOOPSTUDIOS	erc721
\\x0e49144b8b8d228eb7d266f15b2e140fea72a54e	The ABSTRACT Store V2	DAVIS	erc721
\\xc2cf795ffb22e8cdb3f96143422247769ab6920a	Pest Demand	BANKSY	erc1155
\\x09faf3f119d6b162c9d6732b8aeeb08e52efbd26	United CryptoPunks	UCP	erc721
\\x8bc67d00253fd60b1afcce88b78820413139f4c6	CryptoFlowers	CF	erc721
\\xcc23f109b0cd50577aa50dc6c7608fd6da5059f2	Rarible Verification Collab	RVC	erc1155
\\x1b0338f498963d90744ab59a916344a3af9f1fed	New Morals	NWML	erc721
\\x35ea8162eca2a8066c274c4c7cb3be58a6bb1424	Weensy	WNS	erc1155
\\xd4b73571ac7ef37b70e90b5f18bf10c2a56e9117	Brellias Memories	BRELLIAS	erc1155
\\xe1ff05ca2aaa7c99a551ff951557014055c1002c	John Guydo	RITUALOPENEDITIONSBYJOHNGUYDO	erc721
\\xa983b3d938eedf79783ce88ed227a47b6861a3e9	Aavegotchi Collabs		erc1155
\\x7a6e63979d05b22d9da02acc7380f5a92266d953	BlockFame	BFM	erc721
\\x4392ae0ba1e06cf8d0b7f098fec016dedd85b519	Punks Mavericks and Mishegas	$mishegas	erc1155
\\x8b4616926705fb61e9c4eeac07cd946a5d4b0760	Luchadores.io	LUCHADORES	erc721
\\x995020804986274763df9deb0296b754f2659ca1	EtherTulips	ETHT	erc721
\\xb9d7d4cdf8ce0c00d60205f3baa34924b2b3ff61	Fluffy Perry	$FLFY	erc1155
\\xa4a8f534bd858f518111e7d8e48138a4967dc5ad	GreekFreak	GRK	erc1155
\\x8a5e616abd226db865b99b9d98a4b2d45b3565d3	DeFi NOW	DFN	erc1155
\\x1eac31a0b93e81bd093d116f5d36a83be08f381b	SneakrCred Drop 1: EthAIReum		erc721
\\x5ceb7e1bd70690667b19bd2832ca85aef221a79a	ETH-MEN Comics	EMC	erc1155
\\xcbf43ef91f5eb6ef28e54978baad078ae03417c1	CRyPt0wAVE Reflections Under the Radar	CRyPt0wAVE	erc1155
\\xabed38bfe2161b294bb79449a8cba358afd36740	Tide Estates	TideEstate	erc1155
\\xa0763b660246142baa712745b422f6420953688a	ShroomTopiaOfficial		erc721
\\x457db25e00bc1ec8805daf86c35f779b883efccf	FRTZN by Peer Kriesel	FRTZN	erc721
\\x2fb5d7dda4f1f20f974a0fdd547c38674e8d940c	KnightStory		erc721
\\xc691a078f0345e11e325f195b002e0f84dee43bd	Todd & Rahul's Angel Fund	TRAF	erc721
\\x1231583b112777261e9173aaeceb1f9e49a1e543	Aquarium V2	aqua	erc1155
\\x605d026d82c5a777d93f0dc3bf45a46b93fa2ac3	CryptoConstellations	Galaxy	erc721
\\x8f737a4af5d15eb67f1145f1e95cb575e23c2724	Moatz	Mt.Z	erc721
\\x569adda02f3d8078ccde18e928779e79fb4dee3e	VADER MOOD	VDMOOD	erc721
\\x4350c5bcfb38cd9e2c342832e2cff50c0251c59e	Skeletons Through History	UnFy	erc1155
\\xd70f41dd5875eee7fa9dd8048567bc932124a8d2	DeepBlack		erc721
\\xe9cff11da6b8b8cbfc01c79d9394c5d9dc39b15c	Crypto Sushis	CS	erc1155
\\xf38d82a967ee9224edd3856eb00e046d86d0c460	StarFaces	SFS	erc721
\\x3f7e48a065899fb01878fc91657d2bb0bf1be993	B33PLE	B33	erc1155
\\xe81a4fdec5188c0019fb87a63dee7248ee953b81	Astrocryptids	ASTROCRYPTIDS	erc721
\\xff3559412c4618af7c6e6f166c74252ff6364456	EtherCats		erc1155
\\x8cbb7491ca0a68eed8bdb1f6a2c3f07dbc6672db	sino	SINO	erc721
\\x8add32877b082c32e7f3c16f3f9aa65062d8e544	CopyCats	CAT	erc721
\\x8d67ecc9cdf0c19e170629a6557d210fbb8e6123	Together Collection	together	erc1155
\\x3a0c25422d086950880a976557d53d924935d878	ETH-MEN Avatars	E-MA	erc721
\\x7970bf235e818001906057bf29c047d084e8052a	Air Dolls	😈	erc1155
\\xb0d09c8aa8a9c7c04f03d4139714b9354b8987e0	Crypto Birbs	BIRB	erc721
\\x5283fc3a1aac4dac6b9581d3ab65f4ee2f3de7dc	The ASCIIPunks	ASC	erc721
\\xf7ea8b80bc01986f32f2cb8f92d024b0dae3fa55	Sentinels	Sentinels	erc1155
\\xdf61bef714aa22e1054d4be6312053e1c610bdaf	Furballs A Community NFT Story	FURBALLS	erc1155
\\xa0cfbd6395d53b6cf10e6a2904d1e6020e1df332	Broslavskiymultiple	Bro	erc1155
\\x9680223f7069203e361f55fefc89b7c1a952cdcc	Inventory V2	ITEM	erc721
\\x27b4bc90fbe56f02ef50f2e2f79d7813aa8941a7	Dego Finance	GEGO	erc721
\\x9b9637ebaa40f13553b250d6c0b2c0a21023e2f5	Bulbasaur 9	BULB9	erc721
\\x7685376af33104dd02be287ed857a19bb4a24ea2	Crypto Cockatoos	COCKA	erc1155
\\x9357a3b394798c1575218d18910e926b275ea07a	WrappedEtherWaifu	WWFU	erc721
\\xa571995a60c04471baff944ab4c360c7ed1019c1	KIDS LOVE FUN	KLF	erc1155
\\x67bcbc1c0e120d0a700eb38a2d769c20a1dfb8f6	NiftyPins	NIFTYPINS	erc1155
\\xe6d68d8ca443fca02bbeabd127d7b5f41600ce5c	Crypto Garden	GARDEN	erc1155
\\x2bd2dd2500f256c638aac06b62d410cd094c29d8	VUDU	VUDU	erc1155
\\xa55f065e3132b6982f335f176db3a28a7344a08b	GucciGhost	Ghost	erc1155
\\xdd9dae51ec0b2514b088bd8b0a53b466d5ae628c	WWE NFT		erc1155
\\xf4a8523509b7d2b8aa3173c665aef282547417a9	DΞFY	DEFY	erc1155
\\x828e2cb8d03b52d408895e0844a6268c4c7ef3ad	Scapes (Official Seascape NFT)	SCAPES	erc721
\\xfaafdc07907ff5120a76b34b731b278c38d6043c	Enjin		erc1155
\\xb0e409b7b0313402a10caa00f53bcb6858552fda	Hashrunes	RUNE	erc721
\\x2fa1802b9fffa363e98adb7ab5a08560478b00f6	Flossing Doggie Club	FDC	erc1155
\\xb9250c9581e4594b7c6914897823ad18d6b78e96	LORDLESS	Bounty	erc721
\\x12db57f7250409f4d34113e211e69b8198bc7229	Crystal Mello	MELLOCRYSTAL	erc721
\\x188a084a62e60d22682c6000821df70aa292bb61	CT Prints	CTPrints	erc1155
\\xe66525e6736d2759a570dd167c51b8480124ea60	Shinicards Collection	SHCRD	erc1155
\\x262cb7ae42d44a870f92242c9667a73c4e296670	GANMasks	GM	erc721
\\xb85070695a7599e3f6a8d46e8bd716d1923769b8	Thorchain Collectibles	THOR	erc1155
\\xd5d8503ccf8b95ed877258eaa91d12297ead501e	The Jenk Collection	Jenk	erc1155
\\xa9d57298ef07e99522e6fd273220acc24654e4dc	Ritual Gift by John Guydo	RITUALGIFTBYJOHNGUYDO	erc721
\\xf313abde5239b24abaf54b789b4d0e266d01edb8	Last Chance V2	LC	erc1155
\\xd851fbd96d16d6b818c622cbc23e4197adce4058	Burn Before Reading		erc721
\\x4cc5c817529daed7b787246c001b58caaf5a0d94	The Type Collection	TYPE	erc1155
\\xfd89ea92f6ec07d955e2adbba2400ca1a6369028	SuperWorld	SUPERWORLD	erc721
\\xab089ddf4ceb6a27a5348dcc432933228b725ac0	CryptoCrawlerz	CRAWLZ	erc1155
\\x33a4cfc925ad40e5bb2b9b2462d7a1a5a5da4476	PILLS		erc1155
\\x492c0d0a51cbf9dc225a485f9c722413615fc570	POTATOES		erc1155
\\x2f76aba5731918f9489a639dcdec713a4d1980cf	Dexamol Swarm	dxml	erc1155
\\x8417dd819db2da6df4d0ef43346692b5c3d22b85	LABYRINTH - R3e5ZlqeJL	LBRNT	erc1155
\\x157ad40f43e26e72909f16bb07f2ca97466df849	Footbattle	FBC	erc721
\\x830d9d144da6381ccfaff09f902a433d2f11ffa7	Blobbies	Blobbie	erc1155
\\xf8f470728b795e2f365f0c17cc26c42712ce372d	NFT Morning	NFTMorning	erc1155
\\x47dd306909fcd7cd22dfbf2548f7e43a8594b243	Tory Lanez Digital Collectibles	TLDC	erc1155
\\x99822384b5f341b09a22a6ef62bf3d4c50ab7fe6	4BULLS.GAME	4BULLSGAME	erc1155
\\x36aa8bf7d868067ed8dd82230f7d241a824d0b0e	LSD Gummy Bears	LSD	erc1155
\\x9a88c80aa2f73f4b9687cad5e076a225bc43ecdf	DAILY ART		erc721
\\x5eb308137f5b3ba109fb06cf960b9538fb869971	Broslavskiy Specials	👽	erc1155
\\x85b5bd374c11d8f6e0864a3927028c33bb39646e	Zarniwoop V2	ZART	erc1155
\\x5f5f458ce6c0ccd9e107013c8d674937ad61bc99	OG NFT V3	SNOO	erc721
\\x8810cbf37fee8d9eb6a92cc5b207a9594159fd3a	VideoStore	VideoStore	erc721
\\xb29cd903a2e960569baa6f74792a12d1885aff13	Crypto Slang	CS	erc1155
\\x0a8eb54b0123778291a3cddd2074c9ce8b2cfae5	NEKO Official	NEKO	erc721
\\x3ad503084f1bd8d15a7f5ebe7a038c064e1e3fa1	CryptoArt.Ai	CART	erc721
\\x2aea4add166ebf38b63d09a75de1a7b94aa24163	Kudos	KDO	erc721
\\x1a0e863a94933ad6435345731b790ed699b4cb89	SPACEWALKERS	SW	erc1155
\\x9b26616ee0cbd466e072e86a99b4bfa4a3489bf4	DIGITAL TRASH V2	TRASH	erc1155
\\xa48b440fc00ac8cd53e73cc39f5f7da931fd064f	NFFT	NFFT	erc721
\\x7afc0b40af8f6f28b7f86c4b05fe8de5847dda99	Magusz	MGZ	erc1155
\\xc328520a8b1cead2489d59c16b7752cb60ebb53d	Neon District		erc721
\\x92e6dfe2d1f93cbc045c44b157861c47f0cd6164	cryptojewels	jewels	erc1155
\\xcfbc9103362aec4ce3089f155c2da2eea1cb7602	CryptoCrystal	CC	erc721
\\xdbce179cf2fae19c2261c8cb88ea9470e61815f0	Magic World	⛤Magic⛤	erc1155
\\x644c10cc2b33e201a714a6b69bff1d59d34442f3	Zori.ai		erc721
\\x06a6a7af298129e3a2ab396c9c06f91d3c54aba8	0xUniverse: Galaxy Home (Ethereum)	PLANET	erc721
\\x962a71bb0d2bb118fbe021e320f58c8f9236a700	ETHFighter	ETHF	erc721
\\x6b507272b81e6260d7f3c7c7cdee1101cecdafcc	Crypto Randomics	RAND	erc1155
\\x7d7b627799a09176b75fb8bd5d7e411eead17653	Star Token	GST	erc721
\\x7743452fcdaeba368765cc74dcf56f513f36d3d7	Autosterogram Art		erc721
\\xfe3fdb06226eb7a95ee33730cecc780c5f357d44	TheDogeGlory	Dogecoin Art 	erc721
\\x56b21617fcff1cd70a614128f7ccafa1776b3c99	SpaceDudes NFT	SDS	erc721
\\x12ead3396df7e868398fa27bc60f62ad3b5a184e	The Crypto Buds	CBUDS	erc721
\\xbf14aa3033eb9485efe003e4b0252f9ea73ea31d	PEOPLE FOR ANIMAL	Animals	erc1155
\\x79741245a85b6ef4d22cf928b87ee69ef6f872c6	Relax Pepe Collection	RELAX	erc1155
\\x8c57774696ad32f230b2299eab13a19dc54baef6	Chrome Castle	CC	erc1155
\\xa05d1fea5688688db808c17d2f3d8faa9026a41f	Buccaneers	BUCCANEERS	erc721
\\x055d9683acf78f097249de7321be9a58c29800ca	CJOFighter	CJOA	erc721
\\x960f401aed58668ef476ef02b2a2d43b83c261d8	Dragonereum	DRAGON	erc721
\\xb845da8301c59f742f82bb8a5e00f5160823bd79	Crypto Dolphins	CD	erc1155
\\x5470bad79b5f80149bed3cd2676e3ddcc4730334	MyCryptoGemx	mcgmx	erc1155
\\x3910d4afdf276a0dc8af632ccfceccf5ba04a3b7	Mutant Monsters		erc721
\\x155cbbca1ab35eab09b66270046317803919e555	CryptoTendies	TENDCARD	erc1155
\\x58e80f54c86b6df98814cc274893534c0f7785e8	BitBulls	CTN	erc721
\\xd73be539d6b2076bab83ca6ba62dfe189abc6bbe	BlockchainCuties	BC	erc721
\\x1ff29b26c796451d8d79a498e28b58f65a8e07e4	Cryptomon Creations	CMC	erc1155
\\x25df24b41656b71f3e6da94a74ff2c60e9253f5c	THE SIGNALS	TS	erc1155
\\xe8592cde5f5c2c078586632fbd94659b2745151b	Streets - tQ4ta6P2Wy	STRS	erc1155
\\xeed8da42bd71a51a4e71cf29b7fea6a7d74fed21	CryptoUnisus	UNISUS	erc1155
\\x552d72f86f04098a4eaeda6d7b665ac12f846ad2	Darkwinds	DW1STR	erc721
\\x797ce6d5a2e4ba7ed007b01a42f797a050a3cbd8	US 2020 Election NFT	USNFT	erc721
\\xe53ac6a7fe5b25b434319eade8a03a304129beb9	ProjectWITH EVENT	PWE	erc721
\\x31fe290644df7aef49274ae4b11ffdcc4e83f3cd	TheCrypDonuts	DONUTS	erc1155
\\x663e4229142a27f00bafb5d087e1e730648314c3	PandaEarth	PE	erc721
\\x14c4293d7e7325cec8c52cea3df37d91aa9cc7b6	CryptoServal	CSG	erc721
\\x6357864c93119eb47a06e0283376d4dd85adf821	RARI GANG	GANG	erc1155
\\x87cb562c65c288f2ca678535bcd26441bf696d55	DoomCatRescue	DOOM	erc721
\\xf734bbfbb5d17270a33d47df521958bfdfddff5a	BCrypt0x	PNTR	erc1155
\\x0370905fefa8ce3ac068505c0b4de8db717cdb27	Crypto Guardians	GUARDIANS	erc1155
\\x4722cd3cd807ba31056514dbcbb69d0aed936c7e	Mad Heidi	MHT	erc1155
\\xe3870569f9e1836960bf30b5a45fc743cef0ab0e	Splinterlands		erc721
\\x1452e7d45bf4df6b8c722e7a2a2c0255f6c43996	Concrete Park	CPARK	erc721
\\xdb68df0e86bc7c6176e6a2255a5365f51113bce8	Rope Makers United	RMU	erc1155
\\xe57dd787f0014563a1f82083d90b5857856036fb	Prod_Final	PRF	erc721
\\x0aee9395897083c75d233e98e239f677bf24d3bd	TunaChum V2	TUNA	erc1155
\\x3b05f81864c943590c3364e5fe06632dc531cc1c	TeaCupNFT	TCNFT	erc1155
\\x60909c259cb13021c834c9653c55057ff589fce0	Mona Cards		erc721
\\xab843fb5c8aacd611c9b894908977fe949440857	Lux Expression™	Lux	erc1155
\\xcebf2f59cde3b7a7eaf03e6a7ad2dc8a7d6b82a5	Mars	MARS	erc1155
\\xf7ba76b7492103a50ba02decddcbbfaad8735b4f	Leonardo Glauso V2	collection	erc1155
\\xf26a23019b4699068bb54457f32dafcf22a9d371	CryptoDerby	CDH	erc721
\\xfbfaa77f7455ea280b5cdb2ac8c105b3c5430201	FRTZN EDITION	FRTZN EDTN	erc1155
\\x9faccd9f9661dddec3971c1ee146516127c34fc1	KRAFTERSPACE	KS	erc721
\\x872a2d8d43f4eaed69188a40b373dde945709ad8	NEON PEPE	NP	erc1155
\\xdf58c36af16bc9cbc9c15b435abbbd4ea3931815	Pix Puppies V1	PixPuppies	erc721
\\xbd766b49b90a2c6f936e917bead65f8f81063e76	OnlyOne1	NUM1	erc721
\\xc805658931f959abc01133aa13ff173769133512	Chonker Finance		erc1155
\\xd64faf9f6a72bf8ccc0345f8fb6219de4a340aa1	tolehico-crypto	tlc	erc721
\\x9efa7962ccb25884587526c0ce8bffab2622976b	FoxPunk Jrs	FOXPUNKJRS	erc721
\\x317fc7584386cbf36fb8b29beb5ee23bbf052c5d	max - nf9eCJ8HaR		erc1155
\\x7fdcd2a1e52f10c28cb7732f46393e297ecadda1	HyperDragons	HD	erc721
\\xa5655ffc8c1de3081bd7bed9b66ce26e3117cca5	glia.icu		erc721
\\xbb0b92334a3096e6de8377649dd5ef52eb2866bc	Opensea.Shop	ART	erc721
\\xcb4bf0a3a12e382ee2b9c28587bf18a22a25a143	yea.hammer	yeah	erc721
\\xd9c570f5e932e088c9a3b44a697b969efcf03658	DNN	DNN	erc1155
\\xdab8553cb9b596b0ec0059cde2cd042a6cdb9f00	PureNFT V2	NFT	erc721
\\x0733411463d71e33a41c73b63367389b886c1d12	hawnter99	H99	erc721
\\xf52fbd056ed1f1ab01d4296fce136d6c0a2bbfaf	MethodNFT	MTHDNFT	erc721
\\xf4b0a9ac156b4b63d9e48333e93d7b6e4dfff45a	Eternal World	ETRNL	erc1155
\\x530d2e307a4f65871fed29a5e45451e8d6610d2f	elemeNFT V2	eNFT	erc1155
\\x432354065061940ca85b1640708a35d73443b133	BoxKey V6	BXK	erc721
\\x583efb41c808474f26e1efcaaf29008a11e9e7ed	ESPIRITU MADRE CACAO	CACAO	erc721
\\x2022476a8495d0aef39450586e8945328f2b301a	Genghis Khan		erc721
\\x1931eec9a375d3d388681727c197ad97d7cfbac1	HOPR gems		erc721
\\xe4bc953e6415c6df04afabc397b1750453092a4c	T-Shirt Project		erc721
\\x809865309dfa75d3f18b4ffa219b396452076ae8	CryptoShark	CSHP	erc721
\\x36b4267a4f472e25919bf51a9aff32bd1ee60d62	4Birders	Bird Collectibles and Photography	erc721
\\x4d1004c81a18006b567ea8aa2eb7680b3e960781	Know-how-1 V2	KNW1	erc721
\.


COMMIT;

-- expectation how this table will be used most of the time:
-- As part of a JOIN operation using 'contract_address' as join key to get entries for columns 'name' or 'standard'
CREATE INDEX IF NOT EXISTS nft_tokens_contract_address_name_idx ON nft.tokens (contract_address, name);
CREATE INDEX IF NOT EXISTS nft_tokens_contract_address_standard_idx ON nft.tokens (contract_address, standard);

-- SEE BELOW ALTERNATIVE OPTION for indexes 
-- Because the column 'name' is text of variable length, this is risky 
-- especially if new additions to this table are added via pull requests .
-- from https://www.postgresql.org/docs/13/sql-createindex.html :
-- "It's wise to be conservative about adding non-key columns to an index, especially wide columns.
-- If an index tuple exceeds the maximum size allowed for the index type, data insertion will fail.""
-- CREATE INDEX IF NOT EXISTS tokens_contract_address_name_idx ON nft.tokens USING btree (contract_address) INCLUDE (name);
-- CREATE INDEX IF NOT EXISTS tokens_contract_address_standard_idx ON nft.tokens USING btree (contract_address) INCLUDE (standard);