select 
    blockchain,
    token_address,
    tokenId,
    count(wallet_address) as holder_count --should always be 1
from {{ ref('balances_ethereum_erc721_day') }}
group by blockchain, token_address, tokenId
having count(wallet_address) > 1



neur0x#5783 André
Riverr#3801 Niles
Mats#2586 Mats (Dune CTO)
0xBoxer#7679 Boxer
TheEdgeOfRage#1049 TheEdgeOfRage
waddah0000#6527 waddah0000
Antonio Mendes#3677 Antonio Mendes
andrew.i#0298 andrew hong - @andrewhong5297
jdude#9117 jdude
hagaetc#2397 hagaetc
mfilipe#8982 mfilipe
akhila#3845 akhila | product @ Dune
augustog#3178 augusto | pm spellbook @ dune
sk0ji#4485 sk0ji
bernat#7252 bernat
xsvfat#2009 Sean
agaperste#9869 agaperste
Richard K#4097 Richard
Kylling#2055 Kylling
philipp#2195 Phil
davidkell#0994 davidkell
afletsas#8826 afletsas
belen#2037 belen
Ilias#9034 Ilias