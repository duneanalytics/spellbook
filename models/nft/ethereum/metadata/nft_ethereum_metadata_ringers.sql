{{ config(
        alias = 'ringers',
        partition_by = ['token_id'],
        materialized = 'view',
        unique_key = ['token_id']
        )
}}

select 13000000 as token_id, 0 as token_id_short,'White' as background,'White' as body,'N/A' as extra_color,'4x4 grid' as peg_layout,'Uniform' as peg_scaling,'Solid' as peg_style,'Normal' as size,'Balanced' as wrap_orientation,'Weave' as wrap_style,16 as peg_count,8 as pegs_used,'' as padded_pegs,'' as dark_mode union all
select 13000001,1,'White','Black','N/A','4x4 grid','Bigger near center','Solid','smol boi','Balanced','Loop',11,11,'','' union all
select 13000002,2,'White','Black','N/A','4x4 grid','Uniform','Solid','Normal','Balanced','Weave',9,9,'','' union all
select 13000003,3,'White','Black','N/A','Recursive grid','Bigger near center','Solid','Normal','Balanced','Weave',36,25,'','' union all
select 13000004,4,'White','Black','N/A','3x3 grid','Uniform','Solid','Normal','Balanced','Loop',7,7,'','' union all
select 13000005,5,'White','Black','N/A','5x5 grid','Uniform','Solid','Normal','Balanced','Weave',18,18,'Yes','' union all
select 13000006,6,'White','Black','Blue','4x4 grid','Uniform','Solid','Normal','Off-center','Weave',16,12,'','' union all
select 13000007,7,'White','Black','N/A','5x5 grid','Smaller near center','Solid','Normal','Balanced','Loop',25,14,'','' union all
select 13000008,8,'White','White','N/A','Tiled 4-5','Uniform','Solid','Normal','Balanced','Weave',22,11,'','' union all
select 13000009,9,'White','Black','Red','3x3 grid','Bigger near center','Solid','Normal','Balanced','Weave',9,4,'','' union all
select 13000010,10,'Yellow','White','N/A','4x4 grid','Bigger near center','Solid','Normal','Off-center','Weave',16,11,'','' union all
select 13000011,11,'White','White','N/A','5x5 grid','Uniform','Solid','Normal','Balanced','Loop',25,18,'','' union all
select 13000012,12,'White','White','N/A','Recursive grid','Uniform','Solid','Normal','Balanced','Loop',30,30,'','' union all
select 13000013,13,'White','White','N/A','4x4 grid','Uniform','Solid','Normal','Off-center','Weave',16,12,'','' union all
select 13000014,14,'White','Black','N/A','Recursive grid','Uniform','Solid','Normal','Off-center','Weave',29,21,'','' union all
select 13000015,15,'White','White','N/A','3x3 grid','Uniform','Solid','Normal','Balanced','Loop',4,4,'','' union all
select 13000016,16,'White','White','N/A','5x5 grid','Uniform','Solid','Normal','Balanced','Loop',16,16,'','' union all
select 13000017,17,'White','Black','N/A','5x5 grid','Uniform','Bullseye','Normal','Balanced','Weave',25,18,'','' union all
select 13000018,18,'White','White','N/A','4x4 grid','Uniform','Solid','Normal','Balanced','Loop',11,11,'','' union all
select 13000019,19,'White','Black','N/A','3x3 grid','Uniform','Solid','Normal','Balanced','Loop',9,6,'','' union all
select 13000020,20,'White','Black','N/A','3x3 grid','Uniform','Solid','Normal','Balanced','Loop',9,5,'','' union all
select 13000021,21,'Red','White','Red','4x4 grid','Uniform','Solid','Normal','Balanced','Weave',16,12,'','' union all
select 13000022,22,'White','Black','N/A','3x3 grid','Uniform','Solid','Normal','Balanced','Loop',9,6,'','' union all
select 13000023,23,'White','Yellow','N/A','3x3 grid','Uniform','Solid','Normal','Balanced','Loop',5,5,'','' union all
select 13000024,24,'White','Black','N/A','Tiled 3-2','Uniform','Solid','Normal','Balanced','Loop',5,5,'','' union all
select 13000025,25,'White','Black','N/A','4x4 grid','Uniform','Solid','Normal','Balanced','Loop',10,10,'','' union all
select 13000026,26,'White','White','N/A','3x3 grid','Smaller near center','Solid','Normal','Balanced','Loop',6,6,'','' union all
select 13000027,27,'Beige','White','N/A','5x5 grid','Uniform','Solid','Normal','Balanced','Loop',18,18,'','' union all
select 13000028,28,'White','Black','N/A','3x3 grid','Uniform','Solid','Normal','Off-center','Weave',9,6,'','' union all
select 13000029,29,'White','White','Green','5x5 grid','Uniform','Solid','Normal','Balanced','Loop',25,14,'','' union all
select 13000030,30,'White','Yellow','N/A','Recursive grid','Bigger near center','Solid','Normal','Balanced','Loop',26,17,'','' union all
select 13000031,31,'White','White','Red','4x4 grid','Bigger near center','Solid','Normal','Off-center','Weave',16,8,'','' union all
select 13000032,32,'White','Black','N/A','Recursive grid','Uniform','Solid','Normal','Off-center','Loop',39,26,'Yes','' union all
select 13000033,33,'White','Red','N/A','5x5 grid','Uniform','Solid','Normal','Off-center','Loop',25,19,'','' union all
select 13000034,34,'White','White','N/A','5x5 grid','Uniform','Solid','Normal','Off-center','Loop',25,14,'','' union all
select 13000035,35,'White','Black','N/A','Recursive grid','Uniform','Solid','Normal','Balanced','Loop',20,20,'','' union all
select 13000036,36,'White','White','N/A','Recursive grid','Uniform','Solid','Normal','Balanced','Weave',7,5,'','' union all
select 13000037,37,'White','White','N/A','3x3 grid','Uniform','Solid','Normal','Off-center','Weave',4,4,'','' union all
select 13000038,38,'Beige','Black','N/A','4x4 grid','Uniform','Solid','Normal','Balanced','Weave',16,10,'','' union all
select 13000039,39,'White','White','N/A','Tiled 3-2','Uniform','Bullseye','Normal','Balanced','Weave',5,5,'','' union all
select 13000040,40,'White','Black','N/A','5x5 grid','Bigger near center','Solid','Normal','Balanced','Weave',18,18,'','' union all
select 13000041,41,'White','White','N/A','5x5 grid','Uniform','Solid','Normal','Balanced','Weave',18,18,'','' union all
select 13000042,42,'White','Black','N/A','Tiled 3-4','Uniform','Solid','Normal','Balanced','Weave',10,10,'','' union all
select 13000043,43,'White','White','N/A','Tiled 4-3','Uniform','Solid','Normal','Off-center','Loop',14,10,'','' union all
select 13000044,44,'White','Yellow','N/A','Recursive grid','Smaller near center','Solid','smol boi','Off-center','Weave',45,27,'','' union all
select 13000045,45,'Yellow','White','N/A','Recursive grid','Uniform','Solid','Normal','Balanced','Loop',10,7,'','' union all
select 13000046,46,'White','White','N/A','Tiled 3-2','Uniform','Solid','Normal','Off-center','Weave',4,4,'','' union all
select 13000047,47,'White','Yellow','N/A','Tiled 3-2','Uniform','Solid','Normal','Balanced','Loop',5,5,'','' union all
select 13000048,48,'White','White','N/A','3x3 grid','Uniform','Solid','Normal','Off-center','Loop',9,6,'','' union all
select 13000049,49,'White','White','N/A','4x4 grid','Uniform','Bullseye','Normal','Balanced','Loop',9,9,'','' union all
select 13000050,50,'Black','Black','Blue','Tiled 4-5','Uniform','Solid','Normal','Balanced','Weave',22,12,'','Dark mode' union all
select 13000051,51,'White','Black','Red','3x3 grid','Uniform','Solid','Normal','Balanced','Loop',9,5,'','' union all
select 13000052,52,'Blue','Black','Blue','Recursive grid','Uniform','Solid','Normal','Off-center','Weave',36,21,'','' union all
select 13000053,53,'White','White','N/A','5x5 grid','Bigger near center','Solid','Normal','Balanced','Weave',25,15,'','' union all
select 13000054,54,'White','Black','Red','3x3 grid','Uniform','Solid','Normal','Off-center','Loop',9,5,'','' union all
select 13000055,55,'White','White','N/A','Tiled 4-5','Uniform','Solid','Normal','Balanced','Loop',22,14,'','' union all
select 13000056,56,'White','White','Red','Recursive grid','Uniform','Solid','Normal','Balanced','Loop',20,11,'','' union all
select 13000057,57,'White','Black','N/A','Recursive grid','Uniform','Bullseye','Normal','Balanced','Weave',20,20,'','' union all
select 13000058,58,'White','Black','N/A','4x4 grid','Uniform','Solid','Normal','Balanced','Loop',11,11,'','' union all
select 13000059,59,'Red','White','N/A','4x4 grid','Uniform','Solid','Normal','Balanced','Loop',16,9,'','' union all
select 13000060,60,'White','White','N/A','Recursive grid','Uniform','Solid','Normal','Off-center','Weave',16,16,'Yes','' union all
select 13000061,61,'White','Yellow','N/A','Tiled 2-3','Smaller near center','Solid','Normal','Balanced','Weave',7,4,'','' union all
select 13000062,62,'White','White','N/A','Tiled 3-4','Smaller near center','Solid','Normal','Balanced','Loop',9,9,'','' union all
select 13000063,63,'White','Red','N/A','4x4 grid','Smaller near center','Solid','Normal','Off-center','Weave',8,8,'','' union all
select 13000064,64,'White','Black','N/A','Tiled 4-3','Uniform','Bullseye','Normal','Balanced','Loop',7,7,'','' union all
select 13000065,65,'White','Black','N/A','3x3 grid','Uniform','Solid','Normal','Balanced','Weave',5,5,'','' union all
select 13000066,66,'White','Black','Red','Tiled 3-2','Uniform','Solid','Normal','Off-center','Loop',8,5,'','' union all
select 13000067,67,'White','Red','N/A','4x4 grid','Uniform','Solid','Normal','Balanced','Weave',16,9,'','' union all
select 13000068,68,'White','Red','N/A','3x3 grid','Uniform','Solid','Normal','Balanced','Loop',4,4,'','' union all
select 13000069,69,'White','White','N/A','4x4 grid','Smaller near center','Solid','Normal','Balanced','Weave',9,9,'','' union all
select 13000070,70,'White','Black','N/A','3x3 grid','Bigger near center','Solid','Normal','Balanced','Weave',7,7,'','' union all
select 13000071,71,'White','Black','N/A','Recursive grid','Uniform','Solid','Normal','Balanced','Loop',34,34,'','' union all
select 13000072,72,'Yellow','Yellow','N/A','Tiled 3-2','Bigger near center','Solid','Normal','Balanced','Weave',8,4,'','' union all
select 13000073,73,'White','Black','N/A','5x5 grid','Uniform','Solid','Normal','Balanced','Weave',25,17,'','' union all
select 13000074,74,'White','Black','N/A','3x3 grid','Smaller near center','Solid','Normal','Balanced','Loop',5,5,'','' union all
select 13000075,75,'White','Black','N/A','3x3 grid','Uniform','Solid','Normal','Balanced','Weave',5,5,'','' union all
select 13000076,76,'White','Black','N/A','5x5 grid','Smaller near center','Solid','Normal','Balanced','Loop',25,18,'','' union all
select 13000077,77,'Red','Black','Red','3x3 grid','Uniform','Solid','Normal','Off-center','Weave',9,4,'','' union all
select 13000078,78,'White','Black','N/A','5x5 grid','Uniform','Solid','Normal','Balanced','Weave',16,16,'','' union all
select 13000079,79,'Beige','White','N/A','4x4 grid','Uniform','Solid','Normal','Balanced','Loop',11,11,'','' union all
select 13000080,80,'Black','Black','N/A','4x4 grid','Bigger near center','Solid','Normal','Balanced','Weave',16,11,'','Dark mode' union all
select 13000081,81,'White','Black','N/A','Recursive grid','Bigger near center','Bullseye','Normal','Off-center','Weave',29,23,'','' union all
select 13000082,82,'White','Black','Blue','5x5 grid','Uniform','Solid','Normal','Balanced','Weave',25,13,'','' union all
select 13000083,83,'White','White','Blue','5x5 grid','Uniform','Solid','Normal','Balanced','Loop',25,19,'','' union all
select 13000084,84,'White','White','N/A','Tiled 2-3','Uniform','Solid','Normal','Off-center','Loop',4,4,'','' union all
select 13000085,85,'White','White','N/A','3x3 grid','Uniform','Solid','Normal','Balanced','Loop',6,6,'','' union all
select 13000086,86,'White','White','N/A','Recursive grid','Bigger near center','Solid','Normal','Balanced','Loop',18,12,'','' union all
select 13000087,87,'White','White','N/A','5x5 grid','Uniform','Solid','Normal','Balanced','Loop',13,13,'','' union all
select 13000088,88,'White','Black','N/A','Tiled 5-4','Uniform','Solid','Normal','Balanced','Weave',23,13,'','' union all
select 13000089,89,'White','White','N/A','5x5 grid','Smaller near center','Solid','Normal','Balanced','Loop',16,16,'','' union all
select 13000090,90,'Beige','White','N/A','Tiled 5-4','Smaller near center','Solid','Normal','Balanced','Loop',15,15,'','' union all
select 13000091,91,'White','White','N/A','3x3 grid','Uniform','Solid','Normal','Off-center','Weave',6,6,'','' union all
select 13000092,92,'White','White','N/A','5x5 grid','Uniform','Solid','Normal','Off-center','Loop',18,18,'','' union all
select 13000093,93,'White','Black','N/A','4x4 grid','Bigger near center','Solid','Normal','Balanced','Loop',16,9,'','' union all
select 13000094,94,'Yellow','Black','N/A','3x3 grid','Uniform','Bullseye','Normal','Balanced','Weave',5,5,'','' union all
select 13000095,95,'White','White','Blue','Tiled 4-5','Uniform','Solid','Normal','Balanced','Weave',22,13,'','' union all
select 13000096,96,'White','Black','N/A','5x5 grid','Bigger near center','Solid','Normal','Off-center','Weave',16,16,'','' union all
select 13000097,97,'White','White','N/A','Tiled 2-3','Uniform','Solid','Normal','Balanced','Weave',5,5,'','' union all
select 13000098,98,'White','Yellow','Red','5x5 grid','Uniform','Solid','Normal','Off-center','Loop',25,16,'','' union all
select 13000099,99,'Beige','White','N/A','Tiled 4-3','Uniform','Solid','Normal','Off-center','Loop',10,10,'','' union all
select 13000100,100,'White','White','N/A','Recursive grid','Uniform','Solid','Normal','Balanced','Loop',27,27,'','' union all
select 13000101,101,'White','Black','N/A','Recursive grid','Smaller near center','Solid','Normal','Off-center','Loop',10,10,'','' union all
select 13000102,102,'Beige','White','N/A','5x5 grid','Uniform','Solid','Normal','Balanced','Loop',12,12,'','' union all
select 13000103,103,'White','White','N/A','5x5 grid','Uniform','Solid','Normal','Off-center','Weave',25,15,'','' union all
select 13000104,104,'Yellow','Black','N/A','5x5 grid','Uniform','Solid','Normal','Balanced','Weave',25,16,'','' union all
select 13000105,105,'White','Yellow','N/A','4x4 grid','Uniform','Solid','Normal','Off-center','Weave',10,10,'','' union all
select 13000106,106,'White','Black','Red','5x5 grid','Uniform','Solid','Normal','Balanced','Loop',25,16,'','' union all
select 13000107,107,'White','White','N/A','5x5 grid','Uniform','Solid','Normal','Off-center','Weave',25,17,'','' union all
select 13000108,108,'White','White','N/A','Tiled 3-4','Uniform','Solid','Normal','Balanced','Weave',10,10,'','' union all
select 13000109,109,'White','White','Red','Recursive grid','Uniform','Bullseye','Normal','Balanced','Loop',37,28,'','' union all
select 13000110,110,'White','Black','N/A','5x5 grid','Bigger near center','Bullseye','Normal','Balanced','Weave',14,14,'','' union all
select 13000111,111,'White','White','N/A','3x3 grid','Uniform','Solid','Normal','Balanced','Weave',9,4,'','' union all
select 13000112,112,'White','Black','N/A','4x4 grid','Uniform','Solid','smol boi','Balanced','Weave',16,9,'','' union all
select 13000113,113,'White','White','N/A','5x5 grid','Smaller near center','Solid','Normal','Balanced','Loop',12,12,'','' union all
select 13000114,114,'White','White','N/A','3x3 grid','Uniform','Solid','Normal','Balanced','Loop',6,6,'','' union all
select 13000115,115,'White','White','N/A','3x3 grid','Uniform','Solid','Normal','Balanced','Loop',9,5,'','' union all
select 13000116,116,'White','White','N/A','4x4 grid','Uniform','Solid','Normal','Off-center','Loop',9,9,'','' union all
select 13000117,117,'White','Black','N/A','Tiled 3-4','Uniform','Bullseye','Normal','Balanced','Weave',14,10,'','' union all
select 13000118,118,'White','Black','N/A','5x5 grid','Uniform','Solid','Normal','Balanced','Weave',17,17,'','' union all
select 13000119,119,'White','Black','N/A','Tiled 3-4','Uniform','Solid','Normal','Balanced','Weave',10,10,'','' union all
select 13000120,120,'White','White','N/A','5x5 grid','Uniform','Solid','Normal','Off-center','Loop',25,17,'','' union all
select 13000121,121,'Red','White','N/A','Tiled 3-4','Uniform','Solid','Normal','Balanced','Weave',14,9,'','' union all
select 13000122,122,'White','White','N/A','5x5 grid','Uniform','Solid','Normal','Balanced','Weave',25,13,'','' union all
select 13000123,123,'Blue','Blue','N/A','3x3 grid','Uniform','Solid','Normal','Balanced','Weave',9,6,'','' union all
select 13000124,124,'White','Black','N/A','3x3 grid','Uniform','Solid','Normal','Balanced','Weave',5,5,'','' union all
select 13000125,125,'White','White','N/A','5x5 grid','Uniform','Solid','Normal','Balanced','Loop',25,17,'','' union all
select 13000126,126,'White','White','N/A','Tiled 4-5','Uniform','Solid','Normal','Balanced','Weave',22,13,'','' union all
select 13000127,127,'Yellow','White','N/A','Recursive grid','Uniform','Solid','Normal','Off-center','Weave',9,9,'','' union all
select 13000128,128,'Yellow','Black','N/A','4x4 grid','Uniform','Solid','Normal','Balanced','Loop',11,11,'Yes','' union all
select 13000129,129,'White','Black','N/A','4x4 grid','Uniform','Solid','Normal','Balanced','Weave',9,9,'','' union all
select 13000130,130,'White','White','N/A','6x6 grid','Smaller near center','Solid','Normal','Balanced','Loop',18,18,'Yes','' union all
select 13000131,131,'White','Black','N/A','3x3 grid','Uniform','Solid','Normal','Balanced','Loop',6,6,'','' union all
select 13000132,132,'White','Red','N/A','Recursive grid','Bigger near center','Solid','Normal','Balanced','Weave',12,6,'Yes','' union all
select 13000133,133,'White','White','N/A','5x5 grid','Uniform','Solid','Normal','Off-center','Weave',14,14,'','' union all
select 13000134,134,'White','Black','N/A','4x4 grid','Uniform','Solid','Normal','Balanced','Loop',9,9,'','' union all
select 13000135,135,'White','White','Blue','3x3 grid','Uniform','Solid','Normal','Balanced','Loop',9,6,'','' union all
select 13000136,136,'White','White','N/A','Tiled 2-3','Uniform','Solid','Normal','Balanced','Loop',7,4,'','' union all
select 13000137,137,'White','Black','N/A','Tiled 2-3','Uniform','Solid','Normal','Balanced','Weave',5,5,'','' union all
select 13000138,138,'White','Black','Red','4x4 grid','Uniform','Solid','Normal','Off-center','Weave',16,10,'','' union all
select 13000139,139,'White','White','N/A','4x4 grid','Uniform','Solid','Normal','Balanced','Loop',12,12,'','' union all
select 13000140,140,'White','White','N/A','3x3 grid','Uniform','Solid','Normal','Off-center','Weave',6,6,'','' union all
select 13000141,141,'White','White','N/A','Recursive grid','Bigger near center','Solid','Normal','Balanced','Weave',15,9,'','' union all
select 13000142,142,'White','White','N/A','Tiled 2-3','Uniform','Solid','Normal','Balanced','Loop',4,4,'','' union all
select 13000143,143,'Beige','White','N/A','5x5 grid','Uniform','Solid','Normal','Balanced','Loop',16,16,'','' union all
select 13000144,144,'White','White','N/A','5x5 grid','Uniform','Solid','Normal','Off-center','Loop',17,17,'','' union all
select 13000145,145,'White','White','N/A','4x4 grid','Bigger near center','Solid','Normal','Balanced','Loop',16,8,'','' union all
select 13000146,146,'White','Blue','N/A','Recursive grid','Uniform','Solid','Normal','Balanced','Weave',20,12,'','' union all
select 13000147,147,'White','White','N/A','5x5 grid','Uniform','Solid','Normal','Off-center','Weave',19,19,'','' union all
select 13000148,148,'Red','White','N/A','5x5 grid','Uniform','Solid','Normal','Balanced','Weave',18,18,'','' union all
select 13000149,149,'White','Red','N/A','5x5 grid','Smaller near center','Solid','Normal','Off-center','Loop',17,17,'','' union all
select 13000150,150,'White','White','N/A','3x3 grid','Uniform','Solid','Normal','Balanced','Weave',4,4,'','' union all
select 13000151,151,'White','Black','Blue','Tiled 3-4','Uniform','Solid','Normal','Balanced','Loop',14,7,'','' union all
select 13000152,152,'White','White','N/A','4x4 grid','Uniform','Solid','smol boi','Balanced','Weave',16,9,'','' union all
select 13000153,153,'White','Black','Red','3x3 grid','Bigger near center','Solid','Normal','Off-center','Loop',9,6,'','' union all
select 13000154,154,'Beige','Black','N/A','4x4 grid','Uniform','Solid','Normal','Balanced','Loop',8,8,'','' union all
select 13000155,155,'White','White','N/A','5x5 grid','Bigger near center','Solid','Normal','Balanced','Weave',16,16,'','' union all
select 13000156,156,'White','White','N/A','3x3 grid','Uniform','Solid','Normal','Balanced','Loop',9,5,'','' union all
select 13000157,157,'White','Black','N/A','4x4 grid','Bigger near center','Solid','Normal','Balanced','Weave',9,9,'','' union all
select 13000158,158,'White','White','N/A','Recursive grid','Uniform','Solid','Normal','Off-center','Weave',12,12,'','' union all
select 13000159,159,'White','Black','Blue','4x4 grid','Uniform','Bullseye','smol boi','Off-center','Weave',16,9,'','' union all
select 13000160,160,'White','Black','N/A','Recursive grid','Uniform','Bullseye','smol boi','Off-center','Weave',13,13,'','' union all
select 13000161,161,'White','White','N/A','4x4 grid','Smaller near center','Solid','Normal','Balanced','Loop',9,9,'','' union all
select 13000162,162,'White','White','N/A','4x4 grid','Uniform','Solid','Normal','Balanced','Weave',16,8,'','' union all
select 13000163,163,'White','White','N/A','Recursive grid','Uniform','Solid','Normal','Balanced','Loop',26,26,'','' union all
select 13000164,164,'White','White','Red','Recursive grid','Uniform','Solid','Normal','Balanced','Loop',29,19,'','' union all
select 13000165,165,'Blue','White','N/A','3x3 grid','Uniform','Solid','Normal','Off-center','Weave',7,7,'','' union all
select 13000166,166,'Yellow','Black','N/A','Tiled 5-4','Smaller near center','Solid','Normal','Balanced','Weave',23,12,'','' union all
select 13000167,167,'White','Yellow','N/A','3x3 grid','Uniform','Solid','Normal','Balanced','Loop',9,5,'','' union all
select 13000168,168,'White','Yellow','N/A','3x3 grid','Uniform','Solid','Normal','Balanced','Loop',9,5,'','' union all
select 13000169,169,'Yellow','White','N/A','4x4 grid','Uniform','Solid','Normal','Balanced','Weave',10,10,'','' union all
select 13000170,170,'Red','Black','N/A','4x4 grid','Uniform','Solid','Normal','Off-center','Weave',16,9,'','' union all
select 13000171,171,'White','Black','N/A','5x5 grid','Uniform','Solid','Normal','Balanced','Weave',25,18,'','' union all
select 13000172,172,'White','Blue','Blue','4x4 grid','Uniform','Solid','Normal','Balanced','Loop',16,12,'','' union all
select 13000173,173,'Beige','Black','N/A','4x4 grid','Bigger near center','Solid','Normal','Balanced','Weave',8,8,'','' union all
select 13000174,174,'White','Black','N/A','3x3 grid','Uniform','Solid','Normal','Balanced','Loop',9,4,'','' union all
select 13000175,175,'White','Black','N/A','5x5 grid','Uniform','Solid','Normal','Off-center','Weave',25,14,'','' union all
select 13000176,176,'Red','White','N/A','Recursive grid','Uniform','Bullseye','Normal','Balanced','Weave',28,20,'','' union all
select 13000177,177,'White','White','Red','Recursive grid','Uniform','Solid','Normal','Balanced','Weave',4,2,'','' union all
select 13000178,178,'Yellow','Yellow','N/A','Recursive grid','Uniform','Solid','Normal','Balanced','Weave',9,9,'','' union all
select 13000179,179,'White','White','N/A','4x4 grid','Uniform','Bullseye','Normal','Balanced','Weave',9,9,'','' union all
select 13000180,180,'Yellow','Black','N/A','3x3 grid','Uniform','Bullseye','Normal','Balanced','Loop',6,6,'','' union all
select 13000181,181,'White','White','N/A','Recursive grid','Uniform','Solid','Normal','Balanced','Weave',42,23,'','' union all
select 13000182,182,'White','Black','N/A','5x5 grid','Uniform','Solid','Normal','Balanced','Loop',14,14,'','' union all
select 13000183,183,'White','Black','N/A','4x4 grid','Uniform','Solid','Normal','Balanced','Loop',16,9,'','' union all
select 13000184,184,'White','Black','N/A','Tiled 5-4','Uniform','Bullseye','Normal','Balanced','Weave',23,17,'','' union all
select 13000185,185,'White','White','N/A','Tiled 5-4','Bigger near center','Solid','Normal','Balanced','Loop',23,15,'','' union all
select 13000186,186,'Yellow','White','N/A','Tiled 3-2','Uniform','Solid','Normal','Balanced','Loop',8,4,'','' union all
select 13000187,187,'White','White','N/A','4x4 grid','Bigger near center','Solid','Normal','Off-center','Loop',16,10,'','' union all
select 13000188,188,'White','White','Red','4x4 grid','Uniform','Solid','Normal','Balanced','Loop',16,10,'','' union all
select 13000189,189,'White','Black','N/A','4x4 grid','Uniform','Solid','Normal','Balanced','Loop',16,10,'','' union all
select 13000190,190,'Beige','White','N/A','5x5 grid','Uniform','Solid','Normal','Balanced','Weave',14,14,'','' union all
select 13000191,191,'Blue','White','N/A','4x4 grid','Uniform','Solid','Normal','Off-center','Loop',8,8,'','' union all
select 13000192,192,'Red','White','N/A','Tiled 3-4','Uniform','Solid','Normal','Off-center','Loop',14,10,'','' union all
select 13000193,193,'White','Yellow','N/A','4x4 grid','Bigger near center','Solid','Normal','Balanced','Loop',16,8,'','' union all
select 13000194,194,'Red','Black','N/A','4x4 grid','Uniform','Solid','Normal','Balanced','Loop',16,9,'','' union all
select 13000195,195,'White','White','N/A','5x5 grid','Uniform','Solid','Normal','Balanced','Loop',25,16,'','' union all
select 13000196,196,'White','White','N/A','Recursive grid','Uniform','Solid','Normal','Balanced','Weave',34,34,'','' union all
select 13000197,197,'White','White','N/A','4x4 grid','Uniform','Solid','Normal','Off-center','Loop',9,9,'','' union all
select 13000198,198,'Yellow','Black','N/A','4x4 grid','Uniform','Solid','Normal','Balanced','Loop',10,10,'','' union all
select 13000199,199,'White','Black','N/A','Tiled 2-3','Uniform','Solid','Normal','Balanced','Weave',7,4,'','' union all
select 13000200,200,'White','White','N/A','5x5 grid','Uniform','Solid','Normal','Balanced','Weave',13,13,'','' union all
select 13000201,201,'White','White','N/A','3x3 grid','Uniform','Solid','Normal','Balanced','Loop',5,5,'','' union all
select 13000202,202,'White','Black','N/A','3x3 grid','Uniform','Solid','Normal','Balanced','Loop',5,5,'','' union all
select 13000203,203,'White','White','N/A','5x5 grid','Uniform','Solid','Normal','Off-center','Loop',25,19,'','' union all
select 13000204,204,'White','Black','N/A','Tiled 4-5','Uniform','Solid','Normal','Off-center','Weave',22,17,'','' union all
select 13000205,205,'White','Black','N/A','4x4 grid','Uniform','Solid','Normal','Balanced','Loop',16,11,'','' union all
select 13000206,206,'White','White','N/A','Recursive grid','Uniform','Bullseye','Normal','Off-center','Weave',13,13,'','' union all
select 13000207,207,'White','White','N/A','Tiled 3-4','Smaller near center','Solid','Normal','Off-center','Weave',7,7,'','' union all
select 13000208,208,'White','Black','N/A','Recursive grid','Uniform','Solid','Normal','Balanced','Weave',31,31,'','' union all
select 13000209,209,'White','Black','N/A','3x3 grid','Uniform','Solid','Normal','Balanced','Weave',4,4,'','' union all
select 13000210,210,'Red','Black','N/A','5x5 grid','Uniform','Solid','Normal','Balanced','Weave',14,14,'','' union all
select 13000211,211,'Yellow','Yellow','N/A','5x5 grid','Bigger near center','Solid','Normal','Balanced','Loop',17,17,'','' union all
select 13000212,212,'White','White','N/A','Tiled 3-4','Uniform','Solid','Normal','Balanced','Loop',14,10,'','' union all
select 13000213,213,'White','Yellow','Red','3x3 grid','Uniform','Solid','smol boi','Balanced','Loop',9,6,'','' union all
select 13000214,214,'White','Black','N/A','Tiled 3-4','Uniform','Solid','Normal','Balanced','Loop',8,8,'','' union all
select 13000215,215,'White','White','N/A','5x5 grid','Smaller near center','Solid','Normal','Balanced','Loop',25,16,'','' union all
select 13000216,216,'White','Black','N/A','Tiled 5-4','Uniform','Solid','Normal','Balanced','Loop',17,17,'','' union all
select 13000217,217,'White','White','N/A','5x5 grid','Uniform','Solid','Normal','Balanced','Weave',16,16,'','' union all
select 13000218,218,'White','Black','N/A','Tiled 2-3','Uniform','Solid','Normal','Balanced','Loop',7,4,'','' union all
select 13000219,219,'White','White','Blue','4x4 grid','Uniform','Solid','Normal','Balanced','Weave',16,8,'','' union all
select 13000220,220,'Black','Black','N/A','Tiled 2-3','Uniform','Solid','Normal','Balanced','Loop',5,5,'','Dark mode' union all
select 13000221,221,'White','White','Red','4x4 grid','Uniform','Solid','Normal','Balanced','Loop',16,12,'','' union all
select 13000222,222,'White','White','N/A','Recursive grid','Uniform','Solid','Normal','Balanced','Loop',10,6,'','' union all
select 13000223,223,'White','White','N/A','3x3 grid','Uniform','Solid','Normal','Balanced','Loop',6,6,'','' union all
select 13000224,224,'White','Black','N/A','5x5 grid','Uniform','Solid','smol boi','Balanced','Weave',12,12,'','' union all
select 13000225,225,'White','White','N/A','3x3 grid','Smaller near center','Solid','Normal','Balanced','Loop',5,5,'','' union all
select 13000226,226,'Black','White','N/A','4x4 grid','Uniform','Solid','Normal','Balanced','Loop',11,11,'','Dark mode' union all
select 13000227,227,'White','White','N/A','3x3 grid','Uniform','Solid','Normal','Balanced','Loop',9,5,'','' union all
select 13000228,228,'White','Black','N/A','3x3 grid','Uniform','Solid','Normal','Off-center','Loop',5,5,'','' union all
select 13000229,229,'Beige','Black','N/A','4x4 grid','Uniform','Solid','Normal','Off-center','Loop',11,11,'','' union all
select 13000230,230,'White','Black','N/A','3x3 grid','Uniform','Solid','Normal','Off-center','Weave',6,6,'Yes','' union all
select 13000231,231,'White','White','N/A','5x5 grid','Uniform','Solid','Normal','Balanced','Loop',13,13,'','' union all
select 13000232,232,'White','Black','N/A','Tiled 5-4','Uniform','Solid','Normal','Balanced','Weave',16,16,'','' union all
select 13000233,233,'Black','Black','Red','3x3 grid','Uniform','Solid','Normal','Balanced','Weave',9,5,'','Dark mode' union all
select 13000234,234,'White','White','N/A','Recursive grid','Uniform','Solid','Normal','Balanced','Weave',15,11,'','' union all
select 13000235,235,'White','Yellow','N/A','Recursive grid','Uniform','Solid','Normal','Balanced','Loop',11,11,'','' union all
select 13000236,236,'White','White','N/A','Tiled 3-2','Uniform','Solid','Normal','Off-center','Loop',5,5,'','' union all
select 13000237,237,'White','Black','N/A','5x5 grid','Uniform','Solid','Normal','Balanced','Loop',14,14,'','' union all
select 13000238,238,'White','White','N/A','4x4 grid','Uniform','Bullseye','Normal','Balanced','Weave',9,9,'','' union all
select 13000239,239,'White','White','N/A','Tiled 5-4','Uniform','Solid','Normal','Balanced','Weave',11,11,'','' union all
select 13000240,240,'White','Black','Red','3x3 grid','Uniform','Solid','Normal','Off-center','Weave',9,5,'','' union all
select 13000241,241,'White','White','N/A','Tiled 3-2','Smaller near center','Solid','Normal','Balanced','Weave',8,5,'','' union all
select 13000242,242,'White','Black','N/A','4x4 grid','Uniform','Solid','Normal','Balanced','Weave',8,8,'','' union all
select 13000243,243,'White','White','Blue','5x5 grid','Uniform','Solid','Normal','Balanced','Weave',25,19,'','' union all
select 13000244,244,'White','White','Red','5x5 grid','Uniform','Solid','Normal','Balanced','Loop',25,19,'','' union all
select 13000245,245,'White','Black','N/A','5x5 grid','Uniform','Bullseye','Normal','Balanced','Weave',13,13,'','' union all
select 13000246,246,'White','Black','N/A','3x3 grid','Uniform','Solid','Normal','Balanced','Loop',4,4,'','' union all
select 13000247,247,'White','White','N/A','4x4 grid','Uniform','Solid','Normal','Balanced','Loop',8,8,'','' union all
select 13000248,248,'White','Black','N/A','3x3 grid','Uniform','Solid','Normal','Balanced','Loop',7,7,'','' union all
select 13000249,249,'White','White','N/A','3x3 grid','Bigger near center','Solid','Normal','Balanced','Loop',5,5,'','' union all
select 13000250,250,'Red','Yellow','N/A','4x4 grid','Uniform','Solid','Normal','Balanced','Weave',16,9,'','' union all
select 13000251,251,'White','Black','N/A','Tiled 3-2','Uniform','Solid','Normal','Off-center','Loop',4,4,'','' union all
select 13000252,252,'Red','White','Red','Tiled 4-5','Uniform','Solid','Normal','Balanced','Weave',22,17,'','' union all
select 13000253,253,'White','Black','N/A','3x3 grid','Uniform','Solid','Normal','Balanced','Weave',9,5,'','' union all
select 13000254,254,'Black','White','N/A','4x4 grid','Uniform','Solid','Normal','Balanced','Loop',8,8,'','Dark mode' union all
select 13000255,255,'White','Black','N/A','3x3 grid','Smaller near center','Solid','Normal','Balanced','Weave',6,6,'','' union all
select 13000256,256,'White','White','N/A','5x5 grid','Bigger near center','Solid','Normal','Balanced','Weave',14,14,'','' union all
select 13000257,257,'White','White','N/A','4x4 grid','Uniform','Solid','Normal','Balanced','Weave',16,10,'','' union all
select 13000258,258,'White','White','N/A','Recursive grid','Uniform','Solid','Normal','Balanced','Loop',12,8,'','' union all
select 13000259,259,'White','Black','N/A','4x4 grid','Uniform','Solid','Normal','Balanced','Loop',16,9,'','' union all
select 13000260,260,'White','Black','N/A','5x5 grid','Uniform','Solid','Normal','Balanced','Loop',25,16,'','' union all
select 13000261,261,'Yellow','Black','N/A','3x3 grid','Uniform','Solid','Normal','Balanced','Weave',9,5,'','' union all
select 13000262,262,'Red','White','N/A','3x3 grid','Smaller near center','Solid','Normal','Balanced','Weave',9,5,'','' union all
select 13000263,263,'Black','White','N/A','5x5 grid','Smaller near center','Solid','Normal','Balanced','Loop',25,18,'','Dark mode' union all
select 13000264,264,'White','White','N/A','4x4 grid','Bigger near center','Solid','Normal','Balanced','Loop',10,10,'','' union all
select 13000265,265,'White','White','N/A','5x5 grid','Uniform','Solid','Normal','Balanced','Loop',16,16,'','' union all
select 13000266,266,'White','White','N/A','5x5 grid','Bigger near center','Solid','Normal','Off-center','Weave',17,17,'','' union all
select 13000267,267,'White','White','Red','Tiled 4-3','Uniform','Solid','Normal','Balanced','Weave',14,7,'','' union all
select 13000268,268,'White','White','N/A','Tiled 3-2','Uniform','Solid','smol boi','Balanced','Weave',8,5,'','' union all
select 13000269,269,'White','White','N/A','5x5 grid','Bigger near center','Solid','Normal','Balanced','Weave',25,12,'','' union all
select 13000270,270,'White','Black','N/A','4x4 grid','Bigger near center','Solid','Normal','Balanced','Loop',16,12,'','' union all
select 13000271,271,'White','White','N/A','Recursive grid','Uniform','Solid','Normal','Balanced','Weave',24,24,'','' union all
select 13000272,272,'White','Black','Red','5x5 grid','Bigger near center','Solid','Normal','Off-center','Loop',25,17,'','' union all
select 13000273,273,'Red','White','N/A','3x3 grid','Smaller near center','Solid','Normal','Balanced','Weave',5,5,'','' union all
select 13000274,274,'White','White','N/A','3x3 grid','Uniform','Solid','Normal','Off-center','Loop',5,5,'','' union all
select 13000275,275,'Beige','Yellow','N/A','4x4 grid','Smaller near center','Solid','Normal','Off-center','Loop',9,9,'','' union all
select 13000276,276,'White','White','N/A','3x3 grid','Uniform','Solid','Normal','Balanced','Weave',9,5,'','' union all
select 13000277,277,'White','Black','N/A','Tiled 5-6','Bigger near center','Solid','Normal','Balanced','Loop',33,23,'','' union all
select 13000278,278,'White','Black','N/A','Recursive grid','Uniform','Bullseye','Normal','Balanced','Loop',23,18,'','' union all
select 13000279,279,'White','Black','Red','Tiled 3-2','Bigger near center','Solid','Normal','Balanced','Loop',8,4,'','' union all
select 13000280,280,'Yellow','White','N/A','Recursive grid','Uniform','Solid','Normal','Balanced','Loop',55,39,'','' union all
select 13000281,281,'White','White','N/A','Recursive grid','Uniform','Solid','Normal','Balanced','Loop',12,12,'','' union all
select 13000282,282,'White','White','N/A','5x5 grid','Uniform','Solid','Normal','Balanced','Loop',25,13,'','' union all
select 13000283,283,'White','Black','Blue','5x5 grid','Bigger near center','Solid','Normal','Balanced','Weave',25,19,'','' union all
select 13000284,284,'White','White','N/A','4x4 grid','Uniform','Solid','Normal','Balanced','Loop',16,8,'Yes','' union all
select 13000285,285,'White','White','N/A','3x3 grid','Uniform','Solid','Normal','Balanced','Weave',9,6,'','' union all
select 13000286,286,'Red','White','N/A','Tiled 3-2','Smaller near center','Bullseye','smol boi','Off-center','Loop',4,4,'','' union all
select 13000287,287,'White','White','N/A','Tiled 5-4','Uniform','Solid','smol boi','Balanced','Weave',12,12,'','' union all
select 13000288,288,'White','Yellow','N/A','5x5 grid','Uniform','Solid','Normal','Balanced','Weave',18,18,'','' union all
select 13000289,289,'White','White','N/A','4x4 grid','Uniform','Bullseye','Normal','Balanced','Loop',8,8,'','' union all
select 13000290,290,'White','White','N/A','5x5 grid','Uniform','Solid','Normal','Off-center','Loop',16,16,'','' union all
select 13000291,291,'Beige','Black','N/A','4x4 grid','Smaller near center','Solid','smol boi','Off-center','Loop',9,9,'','' union all
select 13000292,292,'White','Black','N/A','4x4 grid','Uniform','Solid','Normal','Balanced','Weave',16,12,'','' union all
select 13000293,293,'White','Yellow','N/A','4x4 grid','Uniform','Solid','Normal','Balanced','Weave',8,8,'Yes','' union all
select 13000294,294,'White','Black','N/A','Tiled 3-2','Uniform','Solid','Normal','Off-center','Weave',8,5,'','' union all
select 13000295,295,'White','White','N/A','3x3 grid','Uniform','Solid','Normal','Balanced','Loop',6,6,'','' union all
select 13000296,296,'White','White','N/A','3x3 grid','Uniform','Solid','Normal','Balanced','Weave',5,5,'','' union all
select 13000297,297,'White','Red','N/A','4x4 grid','Smaller near center','Solid','Normal','Balanced','Weave',11,11,'','' union all
select 13000298,298,'White','White','N/A','4x4 grid','Uniform','Solid','Normal','Off-center','Loop',8,8,'','' union all
select 13000299,299,'Beige','White','Red','5x5 grid','Uniform','Solid','Normal','Off-center','Weave',25,19,'','' union all
select 13000300,300,'White','Black','Red','3x3 grid','Uniform','Solid','Normal','Balanced','Weave',9,5,'','' union all
select 13000301,301,'White','White','Red','4x4 grid','Smaller near center','Solid','Normal','Balanced','Loop',16,8,'','' union all
select 13000302,302,'White','White','N/A','3x3 grid','Uniform','Solid','Normal','Balanced','Loop',5,5,'','' union all
select 13000303,303,'Yellow','Black','N/A','Tiled 3-2','Smaller near center','Solid','Normal','Balanced','Weave',6,6,'','' union all
select 13000304,304,'White','White','N/A','3x3 grid','Uniform','Solid','Normal','Balanced','Loop',5,5,'','' union all
select 13000305,305,'White','Black','N/A','4x4 grid','Uniform','Solid','Normal','Balanced','Weave',12,12,'','' union all
select 13000306,306,'White','Black','N/A','Tiled 2-3','Uniform','Solid','Normal','Balanced','Loop',3,3,'','' union all
select 13000307,307,'White','White','N/A','Tiled 2-3','Uniform','Solid','Normal','Off-center','Weave',5,5,'','' union all
select 13000308,308,'White','White','N/A','5x5 grid','Bigger near center','Solid','Normal','Balanced','Loop',25,13,'','' union all
select 13000309,309,'White','White','Red','4x4 grid','Bigger near center','Solid','Normal','Off-center','Loop',16,9,'','' union all
select 13000310,310,'White','Black','N/A','3x3 grid','Smaller near center','Solid','Normal','Off-center','Weave',9,6,'','' union all
select 13000311,311,'White','White','Red','Recursive grid','Uniform','Solid','Normal','Balanced','Weave',25,19,'','' union all
select 13000312,312,'White','Black','N/A','4x4 grid','Uniform','Solid','Normal','Balanced','Loop',10,10,'','' union all
select 13000313,313,'Black','White','Red','Recursive grid','Uniform','Solid','Normal','Balanced','Weave',9,5,'','Dark mode' union all
select 13000314,314,'Red','Black','Red','4x4 grid','Uniform','Solid','Normal','Balanced','Loop',16,11,'','' union all
select 13000315,315,'White','White','N/A','4x4 grid','Uniform','Solid','Normal','Balanced','Loop',16,9,'','' union all
select 13000316,316,'White','White','N/A','5x5 grid','Uniform','Solid','Normal','Off-center','Weave',14,14,'','' union all
select 13000317,317,'White','White','N/A','5x5 grid','Uniform','Solid','Normal','Balanced','Weave',12,12,'','' union all
select 13000318,318,'White','White','N/A','3x3 grid','Uniform','Bullseye','Normal','Balanced','Weave',5,5,'','' union all
select 13000319,319,'White','Black','N/A','Recursive grid','Uniform','Bullseye','Normal','Balanced','Loop',22,22,'','' union all
select 13000320,320,'Yellow','White','N/A','5x5 grid','Uniform','Solid','Normal','Balanced','Weave',25,18,'','' union all
select 13000321,321,'White','White','N/A','4x4 grid','Bigger near center','Solid','Normal','Balanced','Loop',10,10,'','' union all
select 13000322,322,'White','Black','N/A','3x3 grid','Uniform','Solid','Normal','Off-center','Loop',5,5,'','' union all
select 13000323,323,'White','Black','N/A','5x5 grid','Uniform','Solid','Normal','Balanced','Loop',17,17,'','' union all
select 13000324,324,'Yellow','Black','N/A','5x5 grid','Uniform','Solid','Normal','Balanced','Weave',15,15,'','' union all
select 13000325,325,'Beige','Black','N/A','4x4 grid','Uniform','Solid','Normal','Balanced','Weave',16,12,'','' union all
select 13000326,326,'White','Black','N/A','3x3 grid','Uniform','Solid','Normal','Balanced','Weave',9,4,'','' union all
select 13000327,327,'Yellow','White','Red','4x4 grid','Uniform','Solid','Normal','Balanced','Loop',16,9,'Yes','' union all
select 13000328,328,'Yellow','Black','N/A','5x5 grid','Uniform','Solid','Normal','Off-center','Weave',19,19,'','' union all
select 13000329,329,'White','White','N/A','3x3 grid','Uniform','Solid','Normal','Off-center','Weave',6,6,'','' union all
select 13000330,330,'White','White','N/A','3x3 grid','Uniform','Solid','Normal','Balanced','Loop',6,6,'','' union all
select 13000331,331,'Blue','Black','N/A','4x4 grid','Smaller near center','Solid','Normal','Off-center','Weave',8,8,'','' union all
select 13000332,332,'White','White','N/A','4x4 grid','Uniform','Solid','Normal','Balanced','Loop',12,12,'','' union all
select 13000333,333,'Yellow','Red','N/A','Tiled 4-3','Bigger near center','Solid','Normal','Balanced','Weave',14,9,'','' union all
select 13000334,334,'White','Black','N/A','3x3 grid','Uniform','Solid','Normal','Balanced','Weave',9,6,'','' union all
select 13000335,335,'Blue','White','N/A','5x5 grid','Uniform','Solid','Normal','Balanced','Loop',25,14,'','' union all
select 13000336,336,'White','White','N/A','Recursive grid','Uniform','Solid','Normal','Balanced','Loop',16,16,'','' union all
select 13000337,337,'White','Black','N/A','5x5 grid','Uniform','Solid','Normal','Balanced','Weave',13,13,'','' union all
select 13000338,338,'Yellow','White','Red','5x5 grid','Uniform','Solid','Normal','Balanced','Weave',25,17,'','' union all
select 13000339,339,'Yellow','White','N/A','6x6 grid','Uniform','Solid','Normal','Balanced','Weave',36,27,'','' union all
select 13000340,340,'White','Black','N/A','Tiled 3-2','Uniform','Solid','Normal','Off-center','Weave',8,4,'','' union all
select 13000341,341,'White','White','Red','5x5 grid','Uniform','Solid','Normal','Off-center','Loop',25,14,'','' union all
select 13000342,342,'White','Yellow','N/A','4x4 grid','Uniform','Solid','Normal','Balanced','Loop',10,10,'','' union all
select 13000343,343,'White','Yellow','N/A','4x4 grid','Uniform','Solid','Normal','Off-center','Weave',9,9,'','' union all
select 13000344,344,'White','Blue','N/A','5x5 grid','Uniform','Solid','Normal','Balanced','Weave',25,14,'','' union all
select 13000345,345,'White','White','N/A','5x5 grid','Uniform','Bullseye','Normal','Balanced','Loop',14,14,'','' union all
select 13000346,346,'White','Black','N/A','3x3 grid','Uniform','Solid','Normal','Off-center','Weave',5,5,'','' union all
select 13000347,347,'White','White','N/A','3x3 grid','Uniform','Solid','Normal','Balanced','Loop',9,6,'','' union all
select 13000348,348,'White','Black','N/A','5x5 grid','Uniform','Solid','Normal','Balanced','Weave',25,14,'','' union all
select 13000349,349,'White','White','Red','Recursive grid','Uniform','Solid','Normal','Balanced','Weave',12,7,'','' union all
select 13000350,350,'White','White','N/A','Recursive grid','Bigger near center','Solid','Normal','Balanced','Loop',10,10,'','' union all
select 13000351,351,'White','Black','N/A','3x3 grid','Uniform','Solid','Normal','Balanced','Loop',9,6,'','' union all
select 13000352,352,'White','White','Red','4x4 grid','Uniform','Solid','Normal','Off-center','Weave',16,9,'','' union all
select 13000353,353,'Beige','Black','N/A','Recursive grid','Uniform','Solid','Normal','Off-center','Weave',12,12,'','' union all
select 13000354,354,'White','Black','Blue','Tiled 5-4','Uniform','Solid','Normal','Balanced','Weave',23,16,'','' union all
select 13000355,355,'White','White','N/A','3x3 grid','Uniform','Solid','Normal','Balanced','Loop',5,5,'','' union all
select 13000356,356,'Red','White','Red','3x3 grid','Uniform','Solid','Normal','Balanced','Loop',9,6,'','' union all
select 13000357,357,'White','Black','N/A','3x3 grid','Uniform','Solid','Normal','Balanced','Loop',5,5,'','' union all
select 13000358,358,'White','White','N/A','6x6 grid','Uniform','Solid','Normal','Balanced','Weave',27,27,'','' union all
select 13000359,359,'White','Black','N/A','3x3 grid','Bigger near center','Solid','Normal','Balanced','Weave',9,6,'','' union all
select 13000360,360,'White','Red','N/A','Recursive grid','Uniform','Bullseye','Normal','Balanced','Loop',29,29,'','' union all
select 13000361,361,'White','Yellow','N/A','3x3 grid','Bigger near center','Solid','Normal','Balanced','Weave',9,7,'','' union all
select 13000362,362,'White','Black','N/A','Tiled 2-3','Uniform','Solid','Normal','Balanced','Weave',7,5,'','' union all
select 13000363,363,'White','White','N/A','4x4 grid','Uniform','Solid','Normal','Balanced','Loop',11,11,'','' union all
select 13000364,364,'Red','White','N/A','Recursive grid','Uniform','Solid','Normal','Balanced','Weave',4,4,'','' union all
select 13000365,365,'White','Red','Red','Tiled 3-4','Uniform','Bullseye','Normal','Balanced','Loop',14,8,'','' union all
select 13000366,366,'White','Black','N/A','3x3 grid','Uniform','Solid','smol boi','Balanced','Weave',6,6,'','' union all
select 13000367,367,'White','White','N/A','5x5 grid','Uniform','Solid','Normal','Balanced','Weave',19,19,'','' union all
select 13000368,368,'White','Black','Red','5x5 grid','Uniform','Solid','Normal','Balanced','Loop',25,15,'','' union all
select 13000369,369,'White','White','N/A','Tiled 4-5','Bigger near center','Solid','Normal','Balanced','Loop',22,12,'','' union all
select 13000370,370,'White','Black','N/A','3x3 grid','Smaller near center','Solid','Normal','Balanced','Weave',6,6,'','' union all
select 13000371,371,'White','White','N/A','3x3 grid','Smaller near center','Solid','Normal','Off-center','Weave',4,4,'','' union all
select 13000372,372,'White','Black','N/A','4x4 grid','Uniform','Solid','Normal','Off-center','Loop',16,8,'','' union all
select 13000373,373,'White','Yellow','N/A','Recursive grid','Uniform','Solid','Normal','Balanced','Weave',8,8,'','' union all
select 13000374,374,'White','White','N/A','Tiled 2-3','Uniform','Solid','Normal','Off-center','Weave',4,4,'','' union all
select 13000375,375,'White','Yellow','Red','5x5 grid','Uniform','Solid','Normal','Balanced','Loop',25,19,'','' union all
select 13000376,376,'White','Blue','Blue','3x3 grid','Uniform','Solid','Normal','Balanced','Weave',9,6,'','' union all
select 13000377,377,'Black','Black','N/A','Tiled 5-4','Uniform','Solid','Normal','Balanced','Loop',23,11,'','Dark mode' union all
select 13000378,378,'White','Black','N/A','Recursive grid','Uniform','Solid','Normal','Off-center','Loop',39,26,'','' union all
select 13000379,379,'White','White','N/A','5x5 grid','Uniform','Solid','Normal','Balanced','Weave',15,15,'','' union all
select 13000380,380,'White','White','N/A','3x3 grid','Uniform','Solid','Normal','Off-center','Loop',6,6,'','' union all
select 13000381,381,'White','White','N/A','5x5 grid','Uniform','Solid','Normal','Balanced','Loop',25,19,'','' union all
select 13000382,382,'White','Black','N/A','Recursive grid','Uniform','Solid','smol boi','Off-center','Weave',29,17,'','' union all
select 13000383,383,'White','Black','N/A','4x4 grid','Uniform','Solid','Normal','Balanced','Weave',16,11,'','' union all
select 13000384,384,'White','White','N/A','4x4 grid','Bigger near center','Solid','Normal','Balanced','Loop',9,9,'','' union all
select 13000385,385,'White','White','N/A','3x3 grid','Uniform','Solid','Normal','Balanced','Weave',6,6,'Yes','' union all
select 13000386,386,'White','White','N/A','4x4 grid','Uniform','Solid','Normal','Balanced','Loop',16,11,'','' union all
select 13000387,387,'White','White','N/A','4x4 grid','Uniform','Solid','Normal','Balanced','Weave',16,10,'','' union all
select 13000388,388,'White','Black','Red','4x4 grid','Uniform','Solid','Normal','Balanced','Weave',16,9,'','' union all
select 13000389,389,'White','Black','N/A','3x3 grid','Uniform','Solid','Normal','Off-center','Weave',9,4,'','' union all
select 13000390,390,'Yellow','White','Red','Recursive grid','Uniform','Solid','Normal','Balanced','Weave',39,23,'','' union all
select 13000391,391,'White','Black','N/A','5x5 grid','Smaller near center','Solid','Normal','Balanced','Loop',12,12,'','' union all
select 13000392,392,'White','Black','N/A','4x4 grid','Uniform','Solid','Normal','Balanced','Loop',10,10,'','' union all
select 13000393,393,'White','Black','N/A','5x5 grid','Uniform','Solid','Normal','Balanced','Loop',15,15,'','' union all
select 13000394,394,'Red','Black','N/A','Tiled 3-2','Uniform','Solid','Normal','Balanced','Weave',4,4,'','' union all
select 13000395,395,'White','White','N/A','5x5 grid','Uniform','Solid','Normal','Balanced','Loop',17,17,'','' union all
select 13000396,396,'White','Black','N/A','Recursive grid','Uniform','Solid','Normal','Balanced','Weave',14,14,'','' union all
select 13000397,397,'White','White','N/A','4x4 grid','Bigger near center','Solid','Normal','Balanced','Weave',16,12,'','' union all
select 13000398,398,'White','White','N/A','Tiled 2-3','Uniform','Solid','Normal','Balanced','Weave',4,4,'','' union all
select 13000399,399,'White','White','N/A','Recursive grid','Bigger near center','Solid','Normal','Balanced','Loop',20,15,'','' union all
select 13000400,400,'Red','Black','N/A','3x3 grid','Uniform','Bullseye','Normal','Balanced','Weave',4,4,'','' union all
select 13000401,401,'White','White','N/A','Tiled 3-4','Uniform','Solid','Normal','Balanced','Weave',10,10,'','' union all
select 13000402,402,'White','White','N/A','3x3 grid','Bigger near center','Solid','Normal','Balanced','Loop',9,7,'','' union all
select 13000403,403,'White','White','N/A','3x3 grid','Uniform','Solid','Normal','Balanced','Loop',6,6,'','' union all
select 13000404,404,'White','Black','N/A','Tiled 3-2','Uniform','Solid','Normal','Balanced','Loop',8,5,'','' union all
select 13000405,405,'Yellow','White','N/A','Recursive grid','Uniform','Solid','Normal','Balanced','Loop',7,5,'','' union all
select 13000406,406,'Beige','Black','N/A','Recursive grid','Uniform','Solid','Normal','Balanced','Weave',11,11,'','' union all
select 13000407,407,'White','White','N/A','3x3 grid','Uniform','Solid','Normal','Balanced','Loop',7,7,'','' union all
select 13000408,408,'White','White','N/A','4x4 grid','Uniform','Solid','Normal','Balanced','Loop',11,11,'','' union all
select 13000409,409,'Blue','Black','N/A','Recursive grid','Bigger near center','Solid','Normal','Balanced','Weave',28,28,'','' union all
select 13000410,410,'White','Yellow','Red','3x3 grid','Uniform','Solid','Normal','Off-center','Weave',9,4,'','' union all
select 13000411,411,'White','Black','N/A','4x4 grid','Uniform','Solid','Normal','Balanced','Weave',11,11,'','' union all
select 13000412,412,'White','White','Red','Tiled 3-4','Smaller near center','Solid','Normal','Balanced','Weave',14,8,'','' union all
select 13000413,413,'White','Black','Red','3x3 grid','Uniform','Solid','Normal','Balanced','Weave',9,5,'','' union all
select 13000414,414,'White','White','N/A','Tiled 5-4','Uniform','Solid','Normal','Balanced','Weave',23,14,'','' union all
select 13000415,415,'Green','Black','N/A','3x3 grid','Bigger near center','Solid','Normal','Balanced','Weave',9,6,'','' union all
select 13000416,416,'White','White','N/A','3x3 grid','Smaller near center','Solid','Normal','Balanced','Weave',9,6,'','' union all
select 13000417,417,'Black','Black','N/A','3x3 grid','Uniform','Bullseye','Normal','Off-center','Weave',9,6,'','Dark mode' union all
select 13000418,418,'Red','White','N/A','Tiled 3-4','Uniform','Solid','Normal','Off-center','Loop',14,7,'','' union all
select 13000419,419,'White','Black','N/A','5x5 grid','Uniform','Solid','Normal','Balanced','Loop',15,15,'','' union all
select 13000420,420,'Yellow','White','N/A','5x5 grid','Uniform','Solid','Normal','Balanced','Weave',19,19,'Yes','' union all
select 13000421,421,'White','White','N/A','5x5 grid','Uniform','Solid','Normal','Balanced','Loop',13,13,'','' union all
select 13000422,422,'White','White','N/A','5x5 grid','Smaller near center','Solid','Normal','Off-center','Loop',13,13,'','' union all
select 13000423,423,'White','White','Red','Recursive grid','Uniform','Solid','Normal','Balanced','Weave',39,28,'','' union all
select 13000424,424,'White','Black','Red','5x5 grid','Uniform','Solid','Normal','Balanced','Loop',25,13,'','' union all
select 13000425,425,'White','Yellow','N/A','Tiled 3-2','Uniform','Solid','Normal','Balanced','Weave',6,6,'','' union all
select 13000426,426,'White','Black','Red','4x4 grid','Uniform','Solid','Normal','Balanced','Loop',16,9,'','' union all
select 13000427,427,'White','Black','N/A','3x3 grid','Uniform','Bullseye','Normal','Balanced','Weave',9,5,'','' union all
select 13000428,428,'White','White','N/A','Tiled 3-2','Uniform','Solid','Normal','Balanced','Weave',8,5,'','' union all
select 13000429,429,'White','Red','N/A','3x3 grid','Uniform','Solid','Normal','Off-center','Weave',6,6,'','' union all
select 13000430,430,'White','White','N/A','5x5 grid','Bigger near center','Solid','Normal','Balanced','Loop',19,19,'','' union all
select 13000431,431,'White','White','N/A','Recursive grid','Uniform','Solid','Normal','Balanced','Loop',16,16,'','' union all
select 13000432,432,'White','White','N/A','5x5 grid','Uniform','Solid','Normal','Balanced','Weave',25,13,'','' union all
select 13000433,433,'White','White','N/A','4x4 grid','Uniform','Solid','Normal','Balanced','Loop',16,9,'','' union all
select 13000434,434,'White','White','N/A','4x4 grid','Uniform','Solid','Normal','Balanced','Loop',16,10,'','' union all
select 13000435,435,'White','White','N/A','Recursive grid','Bigger near center','Solid','smol boi','Off-center','Weave',9,9,'','' union all
select 13000436,436,'White','White','N/A','Recursive grid','Uniform','Solid','Normal','Balanced','Loop',17,17,'','' union all
select 13000437,437,'White','White','Red','5x5 grid','Uniform','Solid','Normal','Balanced','Weave',25,13,'','' union all
select 13000438,438,'White','Black','Red','4x4 grid','Bigger near center','Solid','Normal','Off-center','Loop',16,8,'','' union all
select 13000439,439,'White','Black','N/A','5x5 grid','Uniform','Solid','Normal','Balanced','Weave',19,19,'','' union all
select 13000440,440,'White','White','N/A','3x3 grid','Uniform','Solid','Normal','Off-center','Weave',6,6,'','' union all
select 13000441,441,'White','White','N/A','5x5 grid','Bigger near center','Solid','Normal','Balanced','Weave',18,18,'','' union all
select 13000442,442,'White','White','N/A','4x4 grid','Uniform','Solid','Normal','Balanced','Loop',16,11,'','' union all
select 13000443,443,'Blue','Yellow','Blue','4x4 grid','Uniform','Solid','Normal','Balanced','Weave',16,9,'','' union all
select 13000444,444,'Yellow','Black','N/A','3x3 grid','Uniform','Solid','Normal','Balanced','Loop',9,5,'','' union all
select 13000445,445,'White','Black','N/A','Tiled 3-2','Uniform','Solid','Normal','Balanced','Loop',5,5,'','' union all
select 13000446,446,'White','White','N/A','5x5 grid','Uniform','Solid','Normal','Balanced','Weave',25,15,'','' union all
select 13000447,447,'White','White','N/A','4x4 grid','Smaller near center','Solid','Normal','Balanced','Weave',16,8,'','' union all
select 13000448,448,'White','Black','N/A','Tiled 3-2','Uniform','Solid','Normal','Balanced','Weave',6,6,'','' union all
select 13000449,449,'White','White','Red','3x3 grid','Uniform','Solid','Normal','Off-center','Loop',9,6,'','' union all
select 13000450,450,'White','Black','N/A','3x3 grid','Bigger near center','Solid','Normal','Off-center','Loop',5,5,'','' union all
select 13000451,451,'Yellow','Black','N/A','5x5 grid','Uniform','Solid','Normal','Balanced','Loop',25,17,'','' union all
select 13000452,452,'Red','Yellow','N/A','5x5 grid','Uniform','Solid','Normal','Balanced','Weave',17,17,'','' union all
select 13000453,453,'Yellow','Black','N/A','Recursive grid','Uniform','Solid','Normal','Balanced','Weave',18,11,'','' union all
select 13000454,454,'Red','White','N/A','5x5 grid','Uniform','Solid','Normal','Balanced','Weave',15,15,'','' union all
select 13000455,455,'White','White','N/A','Tiled 4-5','Uniform','Solid','Normal','Balanced','Loop',15,15,'','' union all
select 13000456,456,'White','Yellow','N/A','3x3 grid','Uniform','Solid','Normal','Off-center','Loop',6,6,'','' union all
select 13000457,457,'Yellow','Black','N/A','5x5 grid','Uniform','Solid','Normal','Balanced','Loop',13,13,'','' union all
select 13000458,458,'Yellow','White','N/A','3x3 grid','Uniform','Solid','Normal','Balanced','Weave',6,6,'','' union all
select 13000459,459,'White','Black','Blue','4x4 grid','Uniform','Solid','Normal','Balanced','Weave',16,10,'Yes','' union all
select 13000460,460,'White','Yellow','N/A','5x5 grid','Uniform','Solid','Normal','Balanced','Weave',25,16,'','' union all
select 13000461,461,'Beige','White','N/A','Tiled 2-3','Bigger near center','Bullseye','Normal','Off-center','Weave',3,3,'','' union all
select 13000462,462,'White','Black','N/A','3x3 grid','Uniform','Solid','Normal','Balanced','Weave',9,4,'','' union all
select 13000463,463,'White','Yellow','N/A','3x3 grid','Smaller near center','Solid','Normal','Balanced','Loop',7,7,'','' union all
select 13000464,464,'White','Black','N/A','5x5 grid','Uniform','Solid','Normal','Balanced','Loop',25,16,'','' union all
select 13000465,465,'Yellow','White','Red','4x4 grid','Uniform','Solid','Normal','Balanced','Weave',16,8,'','' union all
select 13000466,466,'White','Black','Blue','3x3 grid','Smaller near center','Solid','Normal','Balanced','Loop',9,5,'','' union all
select 13000467,467,'White','Black','N/A','3x3 grid','Uniform','Solid','Normal','Off-center','Weave',6,6,'','' union all
select 13000468,468,'Beige','White','N/A','3x3 grid','Uniform','Bullseye','Normal','Balanced','Loop',5,5,'','' union all
select 13000469,469,'White','White','N/A','3x3 grid','Uniform','Solid','Normal','Balanced','Loop',5,5,'','' union all
select 13000470,470,'White','Black','N/A','Tiled 3-4','Uniform','Solid','Normal','Balanced','Loop',9,9,'','' union all
select 13000471,471,'Red','Black','N/A','3x3 grid','Bigger near center','Solid','Normal','Balanced','Loop',9,6,'','' union all
select 13000472,472,'White','White','N/A','5x5 grid','Smaller near center','Solid','Normal','Off-center','Loop',25,18,'','' union all
select 13000473,473,'White','Black','N/A','3x3 grid','Uniform','Solid','Normal','Balanced','Loop',6,6,'','' union all
select 13000474,474,'White','White','N/A','5x5 grid','Uniform','Solid','Normal','Off-center','Loop',17,17,'','' union all
select 13000475,475,'White','White','N/A','Tiled 4-5','Uniform','Bullseye','Normal','Balanced','Loop',14,14,'','' union all
select 13000476,476,'Yellow','Black','N/A','5x5 grid','Uniform','Solid','Normal','Balanced','Loop',17,17,'Yes','' union all
select 13000477,477,'Red','White','N/A','3x3 grid','Bigger near center','Solid','Normal','Balanced','Weave',9,4,'','' union all
select 13000478,478,'White','Black','Blue','Recursive grid','Uniform','Solid','Normal','Off-center','Loop',28,20,'Yes','' union all
select 13000479,479,'White','Black','N/A','3x3 grid','Uniform','Solid','Normal','Balanced','Loop',4,4,'','' union all
select 13000480,480,'White','White','N/A','5x5 grid','Smaller near center','Solid','Normal','Balanced','Loop',13,13,'','' union all
select 13000481,481,'White','Black','N/A','5x5 grid','Uniform','Solid','Normal','Balanced','Weave',18,18,'','' union all
select 13000482,482,'White','Black','N/A','3x3 grid','Uniform','Solid','Normal','Balanced','Loop',9,4,'','' union all
select 13000483,483,'Yellow','White','N/A','Tiled 5-4','Uniform','Bullseye','Normal','Balanced','Weave',14,14,'','' union all
select 13000484,484,'White','Black','N/A','5x5 grid','Uniform','Solid','Normal','Off-center','Loop',17,17,'','' union all
select 13000485,485,'White','Black','N/A','3x3 grid','Uniform','Solid','Normal','Balanced','Weave',5,5,'','' union all
select 13000486,486,'White','White','N/A','5x5 grid','Uniform','Solid','Normal','Balanced','Loop',16,16,'','' union all
select 13000487,487,'White','White','N/A','4x4 grid','Bigger near center','Solid','Normal','Balanced','Loop',16,12,'','' union all
select 13000488,488,'White','White','N/A','Tiled 4-5','Uniform','Solid','Normal','Off-center','Loop',22,17,'','' union all
select 13000489,489,'White','White','Blue','3x3 grid','Uniform','Solid','Normal','Balanced','Loop',9,6,'','' union all
select 13000490,490,'White','White','Red','3x3 grid','Uniform','Solid','Normal','Off-center','Weave',9,6,'','' union all
select 13000491,491,'White','Black','N/A','Tiled 4-3','Uniform','Solid','Normal','Balanced','Weave',14,8,'','' union all
select 13000492,492,'White','Black','N/A','4x4 grid','Uniform','Solid','smol boi','Balanced','Loop',12,12,'','' union all
select 13000493,493,'White','Black','N/A','5x5 grid','Uniform','Solid','Normal','Balanced','Weave',13,13,'','' union all
select 13000494,494,'White','Black','N/A','4x4 grid','Smaller near center','Solid','Normal','Balanced','Loop',9,9,'','' union all
select 13000495,495,'Beige','White','N/A','3x3 grid','Smaller near center','Solid','Normal','Balanced','Loop',5,5,'','' union all
select 13000496,496,'Beige','White','N/A','5x5 grid','Uniform','Solid','Normal','Balanced','Weave',25,15,'','' union all
select 13000497,497,'White','White','N/A','Recursive grid','Uniform','Solid','Normal','Balanced','Weave',36,28,'','' union all
select 13000498,498,'White','Black','Blue','Tiled 3-2','Uniform','Solid','Normal','Off-center','Loop',8,6,'','' union all
select 13000499,499,'White','Black','N/A','4x4 grid','Uniform','Solid','Normal','Off-center','Loop',10,10,'','' union all
select 13000500,500,'White','White','N/A','Tiled 5-4','Uniform','Solid','Normal','Balanced','Weave',23,15,'','' union all
select 13000501,501,'White','White','Red','Recursive grid','Uniform','Solid','Normal','Balanced','Loop',15,11,'','' union all
select 13000502,502,'White','Black','Red','5x5 grid','Uniform','Solid','Normal','Balanced','Weave',25,18,'','' union all
select 13000503,503,'White','Black','N/A','Recursive grid','Uniform','Solid','Normal','Balanced','Weave',15,15,'','' union all
select 13000504,504,'White','Black','N/A','5x5 grid','Uniform','Solid','Normal','Balanced','Loop',12,12,'','' union all
select 13000505,505,'White','White','N/A','4x4 grid','Bigger near center','Solid','Normal','Balanced','Loop',9,9,'','' union all
select 13000506,506,'White','White','N/A','Recursive grid','Bigger near center','Solid','Normal','Balanced','Loop',26,26,'','' union all
select 13000507,507,'Red','White','N/A','3x3 grid','Uniform','Solid','Normal','Balanced','Loop',9,5,'','' union all
select 13000508,508,'White','White','N/A','5x5 grid','Uniform','Solid','Normal','Balanced','Weave',13,13,'','' union all
select 13000509,509,'White','White','N/A','Tiled 5-4','Uniform','Solid','Normal','Balanced','Loop',23,17,'','' union all
select 13000510,510,'Blue','Black','N/A','4x4 grid','Uniform','Solid','Normal','Balanced','Loop',16,12,'','' union all
select 13000511,511,'White','White','N/A','5x5 grid','Uniform','Solid','Normal','Balanced','Weave',12,12,'','' union all
select 13000512,512,'Beige','White','N/A','4x4 grid','Uniform','Solid','Normal','Balanced','Weave',9,9,'','' union all
select 13000513,513,'White','Black','N/A','4x4 grid','Uniform','Solid','Normal','Balanced','Weave',9,9,'','' union all
select 13000514,514,'White','White','N/A','5x5 grid','Smaller near center','Solid','Normal','Balanced','Loop',25,14,'Yes','' union all
select 13000515,515,'White','Black','N/A','3x3 grid','Uniform','Solid','Normal','Balanced','Weave',5,5,'','' union all
select 13000516,516,'White','White','N/A','5x5 grid','Uniform','Solid','Normal','Balanced','Weave',18,18,'','' union all
select 13000517,517,'White','White','Blue','3x3 grid','Uniform','Solid','Normal','Off-center','Loop',9,7,'','' union all
select 13000518,518,'Yellow','White','N/A','5x5 grid','Uniform','Solid','Normal','Balanced','Weave',14,14,'','' union all
select 13000519,519,'White','White','N/A','5x5 grid','Uniform','Solid','Normal','Balanced','Weave',13,13,'','' union all
select 13000520,520,'White','White','N/A','3x3 grid','Bigger near center','Solid','Normal','Off-center','Weave',5,5,'','' union all
select 13000521,521,'Yellow','White','N/A','4x4 grid','Uniform','Solid','Normal','Balanced','Loop',12,12,'','' union all
select 13000522,522,'Yellow','White','N/A','Recursive grid','Uniform','Solid','Normal','Balanced','Weave',4,2,'','' union all
select 13000523,523,'White','Black','N/A','3x3 grid','Uniform','Solid','Normal','Off-center','Weave',9,5,'','' union all
select 13000524,524,'White','White','Red','3x3 grid','Uniform','Solid','Normal','Balanced','Weave',9,6,'','' union all
select 13000525,525,'White','Black','N/A','4x4 grid','Uniform','Solid','Normal','Balanced','Loop',12,12,'','' union all
select 13000526,526,'White','White','N/A','5x5 grid','Bigger near center','Solid','Normal','Balanced','Weave',18,18,'','' union all
select 13000527,527,'White','White','N/A','Tiled 4-5','Uniform','Bullseye','Normal','Balanced','Loop',17,17,'','' union all
select 13000528,528,'Red','Black','N/A','5x5 grid','Uniform','Solid','Normal','Balanced','Loop',16,16,'','' union all
select 13000529,529,'Black','Black','N/A','3x3 grid','Uniform','Solid','Normal','Balanced','Loop',5,5,'Yes','Dark mode' union all
select 13000530,530,'White','Black','N/A','5x5 grid','Uniform','Solid','Normal','Off-center','Weave',16,16,'','' union all
select 13000531,531,'White','White','N/A','4x4 grid','Uniform','Solid','Normal','Balanced','Loop',8,8,'','' union all
select 13000532,532,'Blue','White','N/A','Tiled 4-3','Uniform','Solid','Normal','Balanced','Weave',9,9,'','' union all
select 13000533,533,'White','Black','N/A','3x3 grid','Uniform','Solid','Normal','Off-center','Weave',9,6,'','' union all
select 13000534,534,'Yellow','White','Green','4x4 grid','Uniform','Solid','Normal','Balanced','Loop',16,11,'','' union all
select 13000535,535,'White','White','N/A','4x4 grid','Uniform','Solid','Normal','Balanced','Loop',11,11,'','' union all
select 13000536,536,'White','White','N/A','Recursive grid','Smaller near center','Solid','Normal','Off-center','Weave',28,28,'','' union all
select 13000537,537,'White','White','N/A','5x5 grid','Uniform','Solid','Normal','Balanced','Loop',25,14,'','' union all
select 13000538,538,'White','White','N/A','5x5 grid','Uniform','Solid','Normal','Balanced','Loop',13,13,'','' union all
select 13000539,539,'White','Black','N/A','4x4 grid','Uniform','Solid','Normal','Balanced','Loop',16,12,'','' union all
select 13000540,540,'White','White','N/A','4x4 grid','Uniform','Solid','Normal','Balanced','Loop',16,11,'','' union all
select 13000541,541,'White','White','N/A','5x5 grid','Bigger near center','Solid','Normal','Balanced','Loop',18,18,'','' union all
select 13000542,542,'White','White','N/A','5x5 grid','Bigger near center','Solid','Normal','Balanced','Loop',25,13,'','' union all
select 13000543,543,'White','White','N/A','5x5 grid','Uniform','Solid','Normal','Balanced','Weave',25,14,'','' union all
select 13000544,544,'White','White','N/A','Tiled 3-2','Uniform','Solid','Normal','Off-center','Weave',5,5,'','' union all
select 13000545,545,'Beige','Black','N/A','Recursive grid','Uniform','Solid','Normal','Balanced','Weave',21,21,'','' union all
select 13000546,546,'White','Black','N/A','Recursive grid','Uniform','Solid','Normal','Balanced','Weave',25,25,'','' union all
select 13000547,547,'White','Black','N/A','3x3 grid','Uniform','Solid','Normal','Balanced','Weave',5,5,'','' union all
select 13000548,548,'White','Black','N/A','3x3 grid','Uniform','Solid','Normal','Balanced','Weave',9,4,'','' union all
select 13000549,549,'Yellow','Black','N/A','4x4 grid','Uniform','Solid','smol boi','Balanced','Loop',10,10,'','' union all
select 13000550,550,'White','Red','N/A','Recursive grid','Smaller near center','Solid','Normal','Balanced','Loop',6,6,'','' union all
select 13000551,551,'Yellow','Black','Red','3x3 grid','Uniform','Solid','Normal','Balanced','Loop',9,6,'','' union all
select 13000552,552,'White','White','N/A','4x4 grid','Uniform','Bullseye','Normal','Balanced','Weave',16,9,'','' union all
select 13000553,553,'White','White','N/A','4x4 grid','Uniform','Solid','Normal','Balanced','Loop',10,10,'','' union all
select 13000554,554,'Red','White','N/A','3x3 grid','Uniform','Solid','Normal','Balanced','Weave',9,5,'','' union all
select 13000555,555,'White','White','N/A','Tiled 2-3','Uniform','Solid','Normal','Balanced','Weave',7,4,'Yes','' union all
select 13000556,556,'White','Black','N/A','5x5 grid','Uniform','Solid','Normal','Off-center','Loop',19,19,'','' union all
select 13000557,557,'White','Black','N/A','Tiled 4-5','Uniform','Solid','Normal','Balanced','Weave',15,15,'','' union all
select 13000558,558,'White','White','Red','3x3 grid','Uniform','Solid','Normal','Balanced','Loop',9,4,'','' union all
select 13000559,559,'White','Yellow','N/A','Tiled 4-3','Smaller near center','Solid','Normal','Balanced','Weave',9,9,'','' union all
select 13000560,560,'White','White','Red','4x4 grid','Uniform','Solid','Normal','Balanced','Loop',16,9,'','' union all
select 13000561,561,'Beige','White','N/A','Tiled 4-3','Bigger near center','Solid','Normal','Balanced','Weave',10,10,'','' union all
select 13000562,562,'White','Black','N/A','4x4 grid','Uniform','Solid','Normal','Balanced','Weave',11,11,'','' union all
select 13000563,563,'White','Black','Red','4x4 grid','Uniform','Solid','Normal','Balanced','Loop',16,9,'','' union all
select 13000564,564,'Beige','Black','N/A','Tiled 3-2','Uniform','Solid','Normal','Balanced','Weave',8,5,'','' union all
select 13000565,565,'White','White','N/A','3x3 grid','Uniform','Bullseye','Normal','Off-center','Weave',5,5,'','' union all
select 13000566,566,'White','White','Red','3x3 grid','Uniform','Solid','Normal','Balanced','Weave',9,5,'','' union all
select 13000567,567,'White','White','Red','Tiled 4-5','Uniform','Solid','Normal','Balanced','Loop',22,16,'','' union all
select 13000568,568,'White','Black','Red','3x3 grid','Uniform','Solid','Normal','Balanced','Weave',9,5,'','' union all
select 13000569,569,'White','White','N/A','3x3 grid','Uniform','Solid','Normal','Balanced','Weave',7,7,'','' union all
select 13000570,570,'Beige','White','N/A','5x5 grid','Smaller near center','Solid','Normal','Balanced','Loop',25,19,'','' union all
select 13000571,571,'Yellow','Black','Red','5x5 grid','Uniform','Solid','Normal','Off-center','Loop',25,14,'','' union all
select 13000572,572,'White','White','Red','5x5 grid','Uniform','Solid','Normal','Balanced','Weave',25,17,'','' union all
select 13000573,573,'White','Yellow','N/A','5x5 grid','Uniform','Bullseye','Normal','Balanced','Loop',25,13,'','' union all
select 13000574,574,'White','Black','N/A','3x3 grid','Uniform','Solid','Normal','Balanced','Weave',9,5,'','' union all
select 13000575,575,'Red','Black','Red','5x5 grid','Smaller near center','Solid','Normal','Balanced','Loop',25,16,'','' union all
select 13000576,576,'White','Black','N/A','3x3 grid','Uniform','Solid','Normal','Balanced','Weave',5,5,'','' union all
select 13000577,577,'White','Black','N/A','4x4 grid','Uniform','Solid','Normal','Balanced','Loop',10,10,'','' union all
select 13000578,578,'White','Black','N/A','Tiled 2-3','Uniform','Solid','Normal','Balanced','Weave',7,3,'Yes','' union all
select 13000579,579,'White','White','N/A','Tiled 4-5','Bigger near center','Bullseye','Normal','Balanced','Loop',22,15,'','' union all
select 13000580,580,'White','White','N/A','4x4 grid','Uniform','Bullseye','Normal','Balanced','Weave',16,9,'','' union all
select 13000581,581,'White','Black','N/A','4x4 grid','Uniform','Bullseye','Normal','Balanced','Weave',16,11,'','' union all
select 13000582,582,'Black','White','N/A','4x4 grid','Uniform','Solid','Normal','Balanced','Weave',8,8,'','Dark mode' union all
select 13000583,583,'White','Black','N/A','Recursive grid','Uniform','Solid','smol boi','Balanced','Loop',15,9,'','' union all
select 13000584,584,'White','White','N/A','3x3 grid','Uniform','Solid','Normal','Balanced','Loop',5,5,'','' union all
select 13000585,585,'White','Red','N/A','5x5 grid','Uniform','Solid','Normal','Balanced','Loop',25,17,'','' union all
select 13000586,586,'White','Black','N/A','3x3 grid','Uniform','Solid','Normal','Balanced','Loop',9,5,'','' union all
select 13000587,587,'Beige','White','N/A','4x4 grid','Bigger near center','Bullseye','Normal','Balanced','Loop',9,9,'','' union all
select 13000588,588,'White','Black','N/A','4x4 grid','Uniform','Solid','Normal','Balanced','Loop',11,11,'','' union all
select 13000589,589,'White','Black','N/A','4x4 grid','Uniform','Solid','Normal','Off-center','Weave',8,8,'','' union all
select 13000590,590,'White','Yellow','Red','Recursive grid','Uniform','Solid','Normal','Off-center','Loop',4,2,'','' union all
select 13000591,591,'White','White','N/A','4x4 grid','Bigger near center','Solid','Normal','Balanced','Loop',16,8,'','' union all
select 13000592,592,'White','White','N/A','3x3 grid','Uniform','Solid','Normal','Off-center','Weave',9,4,'','' union all
select 13000593,593,'Red','White','N/A','3x3 grid','Uniform','Solid','Normal','Balanced','Loop',4,4,'Yes','' union all
select 13000594,594,'White','White','N/A','Tiled 4-3','Bigger near center','Solid','Normal','Off-center','Loop',7,7,'','' union all
select 13000595,595,'White','Black','N/A','4x4 grid','Smaller near center','Solid','Normal','Balanced','Weave',8,8,'','' union all
select 13000596,596,'White','White','N/A','5x5 grid','Uniform','Solid','Normal','Off-center','Loop',25,15,'','' union all
select 13000597,597,'White','White','Red','Recursive grid','Uniform','Solid','Normal','Balanced','Loop',18,10,'','' union all
select 13000598,598,'White','White','N/A','5x5 grid','Smaller near center','Solid','Normal','Balanced','Loop',25,15,'','' union all
select 13000599,599,'White','Black','N/A','Recursive grid','Uniform','Solid','Normal','Off-center','Loop',19,19,'','' union all
select 13000600,600,'White','White','N/A','3x3 grid','Bigger near center','Solid','Normal','Off-center','Weave',5,5,'Yes','' union all
select 13000601,601,'White','Black','Red','5x5 grid','Uniform','Solid','Normal','Balanced','Loop',25,17,'','' union all
select 13000602,602,'White','White','N/A','3x3 grid','Uniform','Solid','Normal','Balanced','Weave',7,7,'','' union all
select 13000603,603,'White','White','N/A','3x3 grid','Uniform','Solid','Normal','Balanced','Weave',5,5,'','' union all
select 13000604,604,'White','Black','N/A','5x5 grid','Uniform','Solid','Normal','Balanced','Loop',18,18,'','' union all
select 13000605,605,'Red','White','N/A','3x3 grid','Uniform','Bullseye','Normal','Balanced','Weave',6,6,'','' union all
select 13000606,606,'White','Black','N/A','Recursive grid','Uniform','Solid','Normal','Off-center','Loop',20,10,'','' union all
select 13000607,607,'White','White','N/A','Recursive grid','Uniform','Solid','Normal','Balanced','Loop',15,10,'','' union all
select 13000608,608,'White','Yellow','N/A','3x3 grid','Uniform','Solid','Normal','Balanced','Loop',9,6,'','' union all
select 13000609,609,'White','White','N/A','3x3 grid','Smaller near center','Solid','Normal','Balanced','Loop',9,6,'','' union all
select 13000610,610,'White','White','N/A','4x4 grid','Uniform','Solid','Normal','Balanced','Weave',16,8,'','' union all
select 13000611,611,'White','White','N/A','4x4 grid','Uniform','Solid','Normal','Balanced','Loop',16,12,'','' union all
select 13000612,612,'White','Black','N/A','4x4 grid','Uniform','Solid','Normal','Balanced','Weave',8,8,'','' union all
select 13000613,613,'White','Black','N/A','5x5 grid','Smaller near center','Solid','Normal','Balanced','Weave',12,12,'','' union all
select 13000614,614,'Red','Black','N/A','Tiled 4-5','Uniform','Solid','Normal','Balanced','Loop',22,16,'','' union all
select 13000615,615,'White','White','Red','3x3 grid','Uniform','Solid','Normal','Balanced','Loop',9,6,'','' union all
select 13000616,616,'White','White','Green','4x4 grid','Uniform','Solid','Normal','Balanced','Loop',16,10,'','' union all
select 13000617,617,'White','Black','N/A','3x3 grid','Uniform','Bullseye','Normal','Balanced','Weave',9,5,'','' union all
select 13000618,618,'White','White','N/A','3x3 grid','Bigger near center','Solid','Normal','Balanced','Weave',9,6,'','' union all
select 13000619,619,'White','Black','N/A','5x5 grid','Smaller near center','Solid','Normal','Balanced','Weave',25,14,'','' union all
select 13000620,620,'White','Black','Red','5x5 grid','Uniform','Solid','Normal','Balanced','Loop',25,13,'','' union all
select 13000621,621,'Red','White','N/A','Recursive grid','Uniform','Solid','Normal','Balanced','Loop',23,15,'','' union all
select 13000622,622,'White','White','N/A','Recursive grid','Uniform','Solid','Normal','Balanced','Weave',10,10,'','' union all
select 13000623,623,'White','Black','N/A','4x4 grid','Uniform','Solid','Normal','Balanced','Weave',16,10,'','' union all
select 13000624,624,'White','Black','N/A','5x5 grid','Uniform','Solid','Normal','Balanced','Weave',15,15,'','' union all
select 13000625,625,'Green','Black','N/A','3x3 grid','Uniform','Solid','Normal','Balanced','Weave',6,6,'Yes','' union all
select 13000626,626,'White','White','N/A','5x5 grid','Uniform','Solid','Normal','Off-center','Loop',14,14,'','' union all
select 13000627,627,'White','White','N/A','4x4 grid','Uniform','Solid','Normal','Balanced','Weave',11,11,'','' union all
select 13000628,628,'White','White','N/A','3x3 grid','Uniform','Solid','Normal','Off-center','Loop',9,6,'','' union all
select 13000629,629,'White','White','Red','4x4 grid','Smaller near center','Solid','Normal','Balanced','Weave',16,9,'','' union all
select 13000630,630,'Yellow','Black','N/A','4x4 grid','Bigger near center','Solid','Normal','Balanced','Loop',16,10,'','' union all
select 13000631,631,'White','Black','N/A','4x4 grid','Uniform','Solid','Normal','Off-center','Loop',8,8,'','' union all
select 13000632,632,'White','White','N/A','3x3 grid','Uniform','Solid','Normal','Balanced','Weave',9,6,'','' union all
select 13000633,633,'White','White','N/A','Recursive grid','Smaller near center','Solid','Normal','Balanced','Weave',28,16,'','' union all
select 13000634,634,'White','White','N/A','4x4 grid','Uniform','Solid','Normal','Balanced','Loop',9,9,'','' union all
select 13000635,635,'White','White','N/A','4x4 grid','Uniform','Solid','Normal','Balanced','Weave',9,9,'','' union all
select 13000636,636,'White','Black','N/A','5x5 grid','Uniform','Solid','Normal','Balanced','Loop',16,16,'','' union all
select 13000637,637,'White','Black','N/A','Recursive grid','Uniform','Bullseye','Normal','Balanced','Loop',16,16,'','' union all
select 13000638,638,'White','White','Red','5x5 grid','Uniform','Solid','Normal','Balanced','Weave',25,16,'Yes','' union all
select 13000639,639,'Red','White','N/A','4x4 grid','Uniform','Solid','Normal','Balanced','Weave',9,9,'','' union all
select 13000640,640,'Yellow','White','N/A','5x5 grid','Uniform','Bullseye','Normal','Balanced','Loop',13,13,'','' union all
select 13000641,641,'White','White','N/A','Recursive grid','Uniform','Bullseye','Normal','Balanced','Loop',24,24,'','' union all
select 13000642,642,'White','Black','Green','5x5 grid','Uniform','Solid','smol boi','Balanced','Loop',25,12,'','' union all
select 13000643,643,'White','Black','N/A','3x3 grid','Uniform','Bullseye','Normal','Balanced','Loop',9,6,'','' union all
select 13000644,644,'Red','White','Red','Tiled 3-2','Uniform','Solid','Normal','Balanced','Loop',8,4,'','' union all
select 13000645,645,'White','White','N/A','Recursive grid','Smaller near center','Solid','Normal','Balanced','Weave',7,4,'','' union all
select 13000646,646,'Yellow','White','N/A','Recursive grid','Uniform','Solid','Normal','Balanced','Loop',41,26,'','' union all
select 13000647,647,'White','Black','N/A','Recursive grid','Uniform','Solid','Normal','Balanced','Weave',31,24,'','' union all
select 13000648,648,'White','Black','N/A','5x5 grid','Uniform','Solid','Normal','Off-center','Weave',25,14,'','' union all
select 13000649,649,'White','Yellow','N/A','5x5 grid','Uniform','Solid','Normal','Balanced','Weave',25,14,'','' union all
select 13000650,650,'White','Black','N/A','3x3 grid','Uniform','Solid','Normal','Balanced','Weave',9,4,'','' union all
select 13000651,651,'White','White','N/A','5x5 grid','Uniform','Solid','Normal','Balanced','Weave',19,19,'','' union all
select 13000652,652,'White','White','N/A','5x5 grid','Uniform','Solid','Normal','Off-center','Loop',14,14,'','' union all
select 13000653,653,'White','Black','N/A','Tiled 3-4','Uniform','Solid','Normal','Balanced','Weave',9,9,'','' union all
select 13000654,654,'White','Black','N/A','Tiled 5-4','Uniform','Bullseye','Normal','Balanced','Weave',17,17,'','' union all
select 13000655,655,'White','Black','N/A','5x5 grid','Bigger near center','Solid','Normal','Balanced','Loop',15,15,'','' union all
select 13000656,656,'White','Black','N/A','Tiled 2-3','Uniform','Solid','Normal','Balanced','Loop',7,3,'','' union all
select 13000657,657,'White','White','N/A','5x5 grid','Uniform','Solid','Normal','Balanced','Loop',14,14,'','' union all
select 13000658,658,'White','Black','N/A','5x5 grid','Uniform','Solid','smol boi','Balanced','Weave',25,19,'','' union all
select 13000659,659,'Beige','Black','N/A','5x5 grid','Uniform','Bullseye','Normal','Off-center','Loop',17,17,'','' union all
select 13000660,660,'White','White','N/A','4x4 grid','Uniform','Solid','Normal','Balanced','Loop',11,11,'','' union all
select 13000661,661,'White','White','N/A','3x3 grid','Uniform','Solid','Normal','Balanced','Loop',5,5,'','' union all
select 13000662,662,'White','Black','N/A','4x4 grid','Smaller near center','Solid','Normal','Off-center','Weave',11,11,'','' union all
select 13000663,663,'White','White','Red','3x3 grid','Uniform','Solid','Normal','Balanced','Loop',9,6,'','' union all
select 13000664,664,'White','White','N/A','3x3 grid','Uniform','Solid','Normal','Balanced','Weave',6,6,'','' union all
select 13000665,665,'White','White','Red','Tiled 3-4','Smaller near center','Bullseye','Normal','Balanced','Loop',14,8,'','' union all
select 13000666,666,'White','Black','N/A','Recursive grid','Uniform','Solid','Normal','Balanced','Loop',26,13,'','' union all
select 13000667,667,'White','White','N/A','Recursive grid','Uniform','Solid','Normal','Off-center','Weave',5,5,'','' union all
select 13000668,668,'White','Black','N/A','3x3 grid','Uniform','Solid','Normal','Balanced','Loop',5,5,'','' union all
select 13000669,669,'Yellow','Black','N/A','5x5 grid','Bigger near center','Solid','Normal','Balanced','Loop',13,13,'','' union all
select 13000670,670,'White','Black','N/A','4x4 grid','Smaller near center','Solid','Normal','Balanced','Loop',16,8,'','' union all
select 13000671,671,'White','White','N/A','3x3 grid','Uniform','Solid','Normal','Balanced','Weave',6,6,'','' union all
select 13000672,672,'White','White','Red','5x5 grid','Bigger near center','Solid','Normal','Balanced','Loop',25,14,'','' union all
select 13000673,673,'White','White','N/A','4x4 grid','Uniform','Solid','Normal','Balanced','Weave',16,10,'Yes','' union all
select 13000674,674,'White','Black','N/A','4x4 grid','Uniform','Solid','Normal','Balanced','Loop',12,12,'','' union all
select 13000675,675,'Beige','White','N/A','4x4 grid','Uniform','Solid','Normal','Balanced','Loop',16,8,'','' union all
select 13000676,676,'White','Black','N/A','Recursive grid','Uniform','Solid','Normal','Balanced','Loop',25,17,'','' union all
select 13000677,677,'Yellow','White','N/A','4x4 grid','Uniform','Solid','Normal','Balanced','Loop',16,8,'','' union all
select 13000678,678,'White','White','N/A','3x3 grid','Smaller near center','Solid','Normal','Balanced','Weave',6,6,'','' union all
select 13000679,679,'White','White','N/A','Recursive grid','Uniform','Solid','Normal','Off-center','Loop',15,10,'','' union all
select 13000680,680,'White','Black','N/A','3x3 grid','Uniform','Solid','Normal','Balanced','Loop',6,6,'','' union all
select 13000681,681,'Yellow','White','N/A','3x3 grid','Uniform','Solid','Normal','Balanced','Weave',6,6,'','' union all
select 13000682,682,'White','White','N/A','3x3 grid','Uniform','Solid','Normal','Balanced','Weave',9,4,'','' union all
select 13000683,683,'White','White','Red','5x5 grid','Uniform','Solid','smol boi','Balanced','Loop',25,12,'','' union all
select 13000684,684,'White','White','N/A','3x3 grid','Bigger near center','Solid','Normal','Balanced','Weave',4,4,'','' union all
select 13000685,685,'White','White','N/A','3x3 grid','Smaller near center','Solid','Normal','Balanced','Loop',4,4,'','' union all
select 13000686,686,'White','Yellow','Red','Recursive grid','Uniform','Solid','Normal','Off-center','Weave',15,7,'','' union all
select 13000687,687,'White','White','N/A','3x3 grid','Bigger near center','Solid','Normal','Balanced','Weave',9,6,'Yes','' union all
select 13000688,688,'Beige','Black','N/A','5x5 grid','Uniform','Solid','Normal','Balanced','Loop',25,14,'','' union all
select 13000689,689,'White','Black','N/A','3x3 grid','Smaller near center','Solid','Normal','Balanced','Loop',9,6,'','' union all
select 13000690,690,'Yellow','Black','N/A','Tiled 3-2','Uniform','Solid','Normal','Balanced','Weave',8,4,'','' union all
select 13000691,691,'White','White','Red','5x5 grid','Bigger near center','Solid','Normal','Balanced','Loop',25,15,'Yes','' union all
select 13000692,692,'Beige','Black','N/A','4x4 grid','Smaller near center','Solid','Normal','Balanced','Loop',16,8,'','' union all
select 13000693,693,'White','White','N/A','4x4 grid','Uniform','Solid','Normal','Balanced','Loop',8,8,'','' union all
select 13000694,694,'White','Black','N/A','3x3 grid','Smaller near center','Solid','Normal','Balanced','Loop',6,6,'','' union all
select 13000695,695,'Beige','White','N/A','3x3 grid','Uniform','Solid','smol boi','Balanced','Weave',4,4,'','' union all
select 13000696,696,'White','Black','N/A','Tiled 5-4','Bigger near center','Solid','Normal','Balanced','Loop',13,13,'','' union all
select 13000697,697,'White','White','N/A','5x5 grid','Uniform','Solid','Normal','Balanced','Loop',25,18,'','' union all
select 13000698,698,'White','White','N/A','Tiled 4-5','Smaller near center','Bullseye','Normal','Balanced','Weave',22,15,'','' union all
select 13000699,699,'White','Black','N/A','4x4 grid','Bigger near center','Solid','Normal','Balanced','Loop',10,10,'','' union all
select 13000700,700,'White','White','Red','Recursive grid','Uniform','Bullseye','Normal','Balanced','Loop',23,12,'','' union all
select 13000701,701,'White','White','N/A','5x5 grid','Uniform','Solid','Normal','Balanced','Loop',25,13,'','' union all
select 13000702,702,'White','Black','Red','5x5 grid','Smaller near center','Solid','Normal','Off-center','Weave',25,13,'','' union all
select 13000703,703,'Yellow','White','N/A','Recursive grid','Uniform','Solid','Normal','Balanced','Loop',16,16,'','' union all
select 13000704,704,'White','Black','N/A','Recursive grid','Uniform','Solid','Normal','Off-center','Loop',25,25,'','' union all
select 13000705,705,'White','Black','N/A','4x4 grid','Uniform','Bullseye','Normal','Off-center','Loop',8,8,'Yes','' union all
select 13000706,706,'White','Black','N/A','3x3 grid','Uniform','Solid','Normal','Off-center','Weave',7,7,'','' union all
select 13000707,707,'White','Black','N/A','5x5 grid','Uniform','Solid','Normal','Off-center','Weave',25,12,'','' union all
select 13000708,708,'Red','Black','N/A','Tiled 3-2','Uniform','Solid','Normal','Off-center','Weave',5,5,'','' union all
select 13000709,709,'White','Black','N/A','5x5 grid','Uniform','Solid','Normal','Balanced','Weave',15,15,'','' union all
select 13000710,710,'White','Black','N/A','5x5 grid','Uniform','Solid','Normal','Balanced','Loop',15,15,'','' union all
select 13000711,711,'Yellow','Yellow','N/A','Recursive grid','Uniform','Solid','Normal','Balanced','Weave',25,25,'','' union all
select 13000712,712,'White','White','N/A','3x3 grid','Uniform','Solid','Normal','Balanced','Weave',6,6,'','' union all
select 13000713,713,'Yellow','White','Red','Tiled 3-4','Uniform','Solid','Normal','Balanced','Loop',14,8,'','' union all
select 13000714,714,'White','Black','N/A','Tiled 5-4','Uniform','Solid','smol boi','Balanced','Weave',12,12,'','' union all
select 13000715,715,'White','Yellow','N/A','Tiled 4-5','Uniform','Solid','Normal','Balanced','Loop',14,14,'','' union all
select 13000716,716,'Yellow','White','N/A','5x5 grid','Uniform','Solid','Normal','Balanced','Loop',18,18,'','' union all
select 13000717,717,'Yellow','White','N/A','Recursive grid','Uniform','Solid','Normal','Balanced','Weave',20,12,'','' union all
select 13000718,718,'Yellow','Black','N/A','3x3 grid','Uniform','Solid','Normal','Balanced','Loop',9,5,'','' union all
select 13000719,719,'White','White','N/A','Tiled 5-4','Uniform','Solid','Normal','Balanced','Loop',17,17,'','' union all
select 13000720,720,'White','White','N/A','Recursive grid','Uniform','Solid','Normal','Balanced','Weave',13,13,'','' union all
select 13000721,721,'White','White','N/A','5x5 grid','Uniform','Solid','Normal','Off-center','Weave',16,16,'','' union all
select 13000722,722,'White','White','N/A','5x5 grid','Uniform','Solid','Normal','Balanced','Loop',25,18,'','' union all
select 13000723,723,'White','White','N/A','5x5 grid','Uniform','Solid','Normal','Balanced','Loop',25,17,'','' union all
select 13000724,724,'White','White','N/A','3x3 grid','Smaller near center','Solid','Normal','Balanced','Loop',6,6,'','' union all
select 13000725,725,'Beige','Black','N/A','3x3 grid','Bigger near center','Bullseye','Normal','Balanced','Loop',9,4,'','' union all
select 13000726,726,'White','White','N/A','3x3 grid','Uniform','Bullseye','Normal','Balanced','Weave',9,6,'','' union all
select 13000727,727,'Red','Black','N/A','4x4 grid','Uniform','Bullseye','Normal','Balanced','Loop',12,12,'','' union all
select 13000728,728,'White','White','N/A','Recursive grid','Uniform','Solid','smol boi','Off-center','Weave',28,22,'','' union all
select 13000729,729,'Black','Black','N/A','3x3 grid','Uniform','Solid','Normal','Off-center','Weave',4,4,'','Dark mode' union all
select 13000730,730,'White','White','N/A','Tiled 3-2','Smaller near center','Solid','Normal','Off-center','Weave',8,5,'','' union all
select 13000731,731,'White','Black','N/A','5x5 grid','Uniform','Solid','Normal','Balanced','Weave',25,16,'','' union all
select 13000732,732,'White','Black','Red','5x5 grid','Uniform','Solid','Normal','Off-center','Weave',25,13,'','' union all
select 13000733,733,'White','Black','N/A','5x5 grid','Uniform','Solid','Normal','Balanced','Weave',25,19,'','' union all
select 13000734,734,'White','Black','N/A','4x4 grid','Bigger near center','Solid','Normal','Balanced','Weave',16,12,'','' union all
select 13000735,735,'White','White','N/A','3x3 grid','Uniform','Solid','Normal','Balanced','Weave',6,6,'','' union all
select 13000736,736,'White','White','Red','3x3 grid','Uniform','Bullseye','Normal','Balanced','Weave',9,5,'','' union all
select 13000737,737,'Red','Black','N/A','3x3 grid','Uniform','Solid','Normal','Balanced','Loop',4,4,'','' union all
select 13000738,738,'White','White','Red','Tiled 4-3','Uniform','Solid','Normal','Balanced','Loop',14,7,'','' union all
select 13000739,739,'White','White','N/A','3x3 grid','Bigger near center','Solid','Normal','Off-center','Loop',9,5,'','' union all
select 13000740,740,'Yellow','Black','Red','4x4 grid','Smaller near center','Solid','smol boi','Balanced','Loop',16,12,'','' union all
select 13000741,741,'Yellow','White','N/A','Tiled 4-5','Smaller near center','Solid','Normal','Balanced','Loop',17,17,'','' union all
select 13000742,742,'White','White','N/A','Tiled 3-2','Bigger near center','Solid','Normal','Balanced','Loop',8,5,'','' union all
select 13000743,743,'White','Black','N/A','Recursive grid','Uniform','Solid','Normal','Balanced','Weave',18,9,'Yes','' union all
select 13000744,744,'White','White','N/A','3x3 grid','Bigger near center','Solid','Normal','Balanced','Weave',4,4,'','' union all
select 13000745,745,'Beige','White','N/A','3x3 grid','Uniform','Solid','Normal','Balanced','Weave',5,5,'','' union all
select 13000746,746,'White','Black','Red','5x5 grid','Smaller near center','Solid','Normal','Off-center','Weave',25,18,'','' union all
select 13000747,747,'White','Black','Red','4x4 grid','Uniform','Solid','smol boi','Balanced','Loop',16,12,'','' union all
select 13000748,748,'White','White','N/A','Tiled 2-3','Uniform','Solid','Normal','Balanced','Loop',7,4,'','' union all
select 13000749,749,'White','White','N/A','Tiled 4-3','Uniform','Solid','Normal','Balanced','Loop',7,7,'','' union all
select 13000750,750,'White','White','N/A','Tiled 2-3','Uniform','Solid','Normal','Balanced','Loop',3,3,'','' union all
select 13000751,751,'White','White','N/A','Tiled 4-3','Uniform','Solid','Normal','Balanced','Loop',8,8,'','' union all
select 13000752,752,'White','Black','N/A','3x3 grid','Smaller near center','Solid','Normal','Balanced','Loop',6,6,'','' union all
select 13000753,753,'White','White','N/A','4x4 grid','Bigger near center','Solid','Normal','Off-center','Weave',8,8,'','' union all
select 13000754,754,'White','Black','N/A','5x5 grid','Uniform','Solid','Normal','Balanced','Weave',13,13,'','' union all
select 13000755,755,'White','White','N/A','4x4 grid','Smaller near center','Solid','Normal','Balanced','Loop',16,8,'','' union all
select 13000756,756,'White','White','N/A','4x4 grid','Uniform','Solid','Normal','Balanced','Weave',8,8,'','' union all
select 13000757,757,'White','White','N/A','Recursive grid','Smaller near center','Solid','Normal','Balanced','Weave',10,10,'','' union all
select 13000758,758,'White','White','N/A','Recursive grid','Smaller near center','Solid','Normal','Balanced','Weave',5,5,'','' union all
select 13000759,759,'White','White','Red','3x3 grid','Uniform','Solid','Normal','Balanced','Loop',9,7,'','' union all
select 13000760,760,'White','White','N/A','3x3 grid','Bigger near center','Solid','Normal','Balanced','Loop',6,6,'','' union all
select 13000761,761,'White','Black','N/A','5x5 grid','Uniform','Solid','Normal','Balanced','Loop',18,18,'','' union all
select 13000762,762,'White','Black','Red','Recursive grid','Uniform','Solid','Normal','Balanced','Loop',44,22,'','' union all
select 13000763,763,'White','Black','N/A','5x5 grid','Uniform','Solid','Normal','Balanced','Loop',17,17,'','' union all
select 13000764,764,'White','Black','N/A','3x3 grid','Bigger near center','Solid','Normal','Balanced','Weave',9,4,'','' union all
select 13000765,765,'Beige','White','N/A','Recursive grid','Uniform','Solid','Normal','Off-center','Loop',12,6,'','' union all
select 13000766,766,'Yellow','Black','N/A','Recursive grid','Uniform','Solid','smol boi','Balanced','Loop',14,14,'','' union all
select 13000767,767,'White','White','N/A','Tiled 4-3','Uniform','Solid','Normal','Balanced','Loop',14,10,'','' union all
select 13000768,768,'Beige','Black','N/A','Recursive grid','Bigger near center','Solid','Normal','Balanced','Weave',11,11,'','' union all
select 13000769,769,'White','White','N/A','5x5 grid','Uniform','Solid','Normal','Balanced','Weave',17,17,'','' union all
select 13000770,770,'White','Yellow','Red','4x4 grid','Uniform','Bullseye','Normal','Balanced','Loop',16,11,'','' union all
select 13000771,771,'White','Yellow','N/A','5x5 grid','Uniform','Solid','Normal','Balanced','Loop',12,12,'','' union all
select 13000772,772,'Red','Black','N/A','3x3 grid','Smaller near center','Solid','Normal','Balanced','Weave',5,5,'','' union all
select 13000773,773,'White','White','N/A','5x5 grid','Uniform','Solid','Normal','Balanced','Weave',16,16,'','' union all
select 13000774,774,'White','Black','N/A','3x3 grid','Uniform','Solid','Normal','Off-center','Weave',4,4,'','' union all
select 13000775,775,'White','White','N/A','5x5 grid','Uniform','Solid','smol boi','Off-center','Weave',19,19,'','' union all
select 13000776,776,'White','White','N/A','4x4 grid','Uniform','Solid','Normal','Balanced','Weave',12,12,'','' union all
select 13000777,777,'White','White','N/A','Recursive grid','Bigger near center','Bullseye','Normal','Balanced','Weave',17,17,'','' union all
select 13000778,778,'White','White','Red','Tiled 5-4','Uniform','Solid','Normal','Off-center','Loop',23,18,'','' union all
select 13000779,779,'White','White','N/A','3x3 grid','Uniform','Solid','Normal','Balanced','Weave',5,5,'','' union all
select 13000780,780,'White','White','N/A','5x5 grid','Uniform','Solid','Normal','Balanced','Loop',19,19,'','' union all
select 13000781,781,'White','Black','N/A','3x3 grid','Uniform','Solid','Normal','Balanced','Loop',9,5,'','' union all
select 13000782,782,'White','White','N/A','4x4 grid','Bigger near center','Solid','Normal','Balanced','Weave',8,8,'','' union all
select 13000783,783,'Beige','Black','Red','4x4 grid','Bigger near center','Solid','Normal','Balanced','Loop',16,8,'','' union all
select 13000784,784,'White','Black','N/A','5x5 grid','Uniform','Solid','Normal','Balanced','Loop',19,19,'','' union all
select 13000785,785,'White','White','N/A','5x5 grid','Uniform','Solid','Normal','Balanced','Loop',17,17,'','' union all
select 13000786,786,'White','White','N/A','4x4 grid','Smaller near center','Solid','Normal','Balanced','Weave',12,12,'','' union all
select 13000787,787,'Yellow','Black','N/A','3x3 grid','Uniform','Solid','Normal','Balanced','Loop',5,5,'Yes','' union all
select 13000788,788,'White','Black','N/A','4x4 grid','Uniform','Solid','Normal','Balanced','Weave',16,12,'','' union all
select 13000789,789,'White','Black','Red','Recursive grid','Uniform','Solid','Normal','Off-center','Loop',42,28,'Yes','' union all
select 13000790,790,'White','Black','N/A','5x5 grid','Uniform','Solid','Normal','Off-center','Loop',13,13,'','' union all
select 13000791,791,'Red','Black','Red','Recursive grid','Uniform','Solid','Normal','Balanced','Loop',18,14,'','' union all
select 13000792,792,'White','Black','N/A','3x3 grid','Uniform','Solid','Normal','Balanced','Loop',6,6,'Yes','' union all
select 13000793,793,'White','White','N/A','4x4 grid','Uniform','Solid','Normal','Balanced','Loop',16,11,'Yes','' union all
select 13000794,794,'White','White','N/A','Tiled 4-3','Uniform','Solid','Normal','Balanced','Weave',14,7,'','' union all
select 13000795,795,'White','Black','N/A','Tiled 3-4','Bigger near center','Solid','Normal','Balanced','Weave',7,7,'Yes','' union all
select 13000796,796,'White','Yellow','N/A','3x3 grid','Uniform','Solid','Normal','Balanced','Loop',6,6,'','' union all
select 13000797,797,'White','Yellow','N/A','Recursive grid','Uniform','Solid','Normal','Balanced','Loop',6,6,'','' union all
select 13000798,798,'White','White','N/A','3x3 grid','Uniform','Solid','Normal','Balanced','Loop',9,7,'','' union all
select 13000799,799,'White','White','N/A','5x5 grid','Uniform','Solid','Normal','Balanced','Loop',15,15,'','' union all
select 13000800,800,'White','Black','N/A','3x3 grid','Smaller near center','Solid','Normal','Balanced','Loop',6,6,'','' union all
select 13000801,801,'Beige','Black','N/A','5x5 grid','Uniform','Solid','Normal','Balanced','Loop',14,14,'Yes','' union all
select 13000802,802,'White','Black','N/A','3x3 grid','Uniform','Solid','Normal','Balanced','Loop',7,7,'','' union all
select 13000803,803,'Red','Black','N/A','5x5 grid','Bigger near center','Solid','Normal','Balanced','Weave',25,15,'','' union all
select 13000804,804,'Yellow','White','N/A','3x3 grid','Uniform','Solid','Normal','Balanced','Weave',9,6,'','' union all
select 13000805,805,'White','Black','N/A','Tiled 3-4','Bigger near center','Solid','Normal','Balanced','Loop',8,8,'','' union all
select 13000806,806,'White','White','N/A','5x5 grid','Uniform','Solid','Normal','Balanced','Weave',14,14,'','' union all
select 13000807,807,'White','White','N/A','Tiled 3-4','Uniform','Solid','Normal','Balanced','Weave',14,8,'','' union all
select 13000808,808,'Red','White','N/A','5x5 grid','Smaller near center','Solid','Normal','Balanced','Loop',18,18,'','' union all
select 13000809,809,'Red','Black','N/A','Tiled 3-4','Uniform','Solid','Normal','Balanced','Loop',10,10,'','' union all
select 13000810,810,'White','White','N/A','Tiled 4-3','Bigger near center','Solid','Normal','Balanced','Loop',14,9,'Yes','' union all
select 13000811,811,'White','White','N/A','Tiled 5-4','Uniform','Solid','Normal','Off-center','Loop',14,14,'','' union all
select 13000812,812,'White','Black','Red','5x5 grid','Uniform','Solid','Normal','Balanced','Loop',25,18,'','' union all
select 13000813,813,'White','Black','N/A','Tiled 4-5','Uniform','Solid','smol boi','Balanced','Weave',14,14,'','' union all
select 13000814,814,'White','White','N/A','Tiled 3-4','Uniform','Solid','Normal','Balanced','Weave',10,10,'Yes','' union all
select 13000815,815,'White','White','N/A','3x3 grid','Bigger near center','Solid','Normal','Balanced','Loop',9,6,'','' union all
select 13000816,816,'Beige','Black','N/A','4x4 grid','Uniform','Solid','Normal','Balanced','Weave',8,8,'','' union all
select 13000817,817,'White','Black','N/A','3x3 grid','Uniform','Solid','Normal','Balanced','Weave',6,6,'','' union all
select 13000818,818,'Red','White','N/A','Recursive grid','Bigger near center','Solid','Normal','Balanced','Loop',17,17,'','' union all
select 13000819,819,'White','Black','N/A','Tiled 4-3','Uniform','Solid','Normal','Balanced','Weave',14,7,'','' union all
select 13000820,820,'Red','Black','N/A','3x3 grid','Uniform','Solid','Normal','Balanced','Weave',6,6,'','' union all
select 13000821,821,'White','White','N/A','4x4 grid','Uniform','Solid','Normal','Balanced','Loop',8,8,'','' union all
select 13000822,822,'Red','White','Red','3x3 grid','Uniform','Solid','smol boi','Balanced','Weave',9,6,'','' union all
select 13000823,823,'White','White','N/A','5x5 grid','Uniform','Solid','Normal','Balanced','Weave',25,12,'','' union all
select 13000824,824,'Yellow','White','N/A','5x5 grid','Uniform','Solid','Normal','Off-center','Weave',13,13,'','' union all
select 13000825,825,'White','White','N/A','3x3 grid','Uniform','Solid','Normal','Off-center','Loop',9,4,'','' union all
select 13000826,826,'White','White','N/A','4x4 grid','Uniform','Solid','Normal','Off-center','Loop',8,8,'','' union all
select 13000827,827,'White','White','N/A','Recursive grid','Smaller near center','Solid','Normal','Off-center','Weave',23,13,'Yes','' union all
select 13000828,828,'White','Black','Blue','Tiled 2-3','Uniform','Solid','Normal','Balanced','Weave',7,5,'','' union all
select 13000829,829,'White','White','N/A','5x5 grid','Uniform','Solid','smol boi','Balanced','Loop',25,18,'','' union all
select 13000830,830,'White','White','N/A','3x3 grid','Uniform','Solid','Normal','Balanced','Loop',9,4,'','' union all
select 13000831,831,'White','Black','N/A','Tiled 3-4','Bigger near center','Solid','Normal','Balanced','Loop',8,8,'','' union all
select 13000832,832,'White','White','N/A','3x3 grid','Smaller near center','Solid','Normal','Balanced','Weave',6,6,'','' union all
select 13000833,833,'White','White','N/A','3x3 grid','Uniform','Solid','Normal','Balanced','Loop',9,5,'','' union all
select 13000834,834,'White','White','N/A','5x5 grid','Smaller near center','Solid','Normal','Balanced','Loop',25,17,'','' union all
select 13000835,835,'White','Black','N/A','3x3 grid','Uniform','Solid','Normal','Balanced','Weave',9,5,'','' union all
select 13000836,836,'White','White','N/A','3x3 grid','Uniform','Solid','Normal','Balanced','Weave',5,5,'','' union all
select 13000837,837,'Yellow','White','N/A','3x3 grid','Uniform','Solid','Normal','Balanced','Weave',4,4,'','' union all
select 13000838,838,'White','White','N/A','4x4 grid','Uniform','Solid','Normal','Off-center','Loop',12,12,'','' union all
select 13000839,839,'White','Yellow','N/A','4x4 grid','Uniform','Solid','Normal','Balanced','Weave',8,8,'','' union all
select 13000840,840,'Beige','Yellow','N/A','Tiled 4-5','Uniform','Solid','Normal','Off-center','Weave',16,16,'','' union all
select 13000841,841,'White','Black','N/A','Tiled 4-3','Uniform','Solid','Normal','Balanced','Loop',8,8,'','' union all
select 13000842,842,'White','Black','N/A','3x3 grid','Uniform','Bullseye','Normal','Balanced','Loop',5,5,'','' union all
select 13000843,843,'White','Black','N/A','Tiled 3-4','Uniform','Solid','Normal','Balanced','Weave',14,9,'','' union all
select 13000844,844,'White','White','Red','5x5 grid','Bigger near center','Solid','Normal','Balanced','Loop',25,14,'','' union all
select 13000845,845,'White','White','N/A','Tiled 4-3','Uniform','Solid','Normal','Balanced','Loop',14,10,'','' union all
select 13000846,846,'Black','Black','N/A','3x3 grid','Uniform','Solid','Normal','Off-center','Weave',4,4,'','Dark mode' union all
select 13000847,847,'White','White','N/A','4x4 grid','Bigger near center','Solid','Normal','Balanced','Weave',8,8,'','' union all
select 13000848,848,'White','Black','N/A','Tiled 3-2','Uniform','Solid','Normal','Balanced','Weave',8,6,'Yes','' union all
select 13000849,849,'White','Black','N/A','4x4 grid','Bigger near center','Solid','Normal','Off-center','Weave',11,11,'Yes','' union all
select 13000850,850,'White','White','N/A','5x5 grid','Uniform','Solid','Normal','Balanced','Loop',25,18,'','' union all
select 13000851,851,'White','Black','N/A','4x4 grid','Uniform','Solid','Normal','Off-center','Weave',16,10,'','' union all
select 13000852,852,'White','Black','Red','4x4 grid','Uniform','Solid','Normal','Balanced','Weave',16,9,'','' union all
select 13000853,853,'White','Black','N/A','5x5 grid','Bigger near center','Solid','Normal','Off-center','Loop',25,16,'','' union all
select 13000854,854,'White','White','N/A','5x5 grid','Bigger near center','Solid','Normal','Balanced','Loop',25,13,'','' union all
select 13000855,855,'White','Black','N/A','Recursive grid','Uniform','Solid','Normal','Balanced','Weave',13,13,'','' union all
select 13000856,856,'White','Black','Red','5x5 grid','Smaller near center','Solid','Normal','Balanced','Loop',25,12,'','' union all
select 13000857,857,'White','White','N/A','Tiled 5-4','Uniform','Solid','Normal','Off-center','Weave',23,17,'','' union all
select 13000858,858,'White','Black','N/A','3x3 grid','Uniform','Solid','Normal','Off-center','Weave',9,6,'','' union all
select 13000859,859,'White','Red','N/A','4x4 grid','Uniform','Solid','Normal','Balanced','Weave',8,8,'','' union all
select 13000860,860,'White','White','N/A','5x5 grid','Uniform','Solid','Normal','Off-center','Weave',14,14,'','' union all
select 13000861,861,'White','White','Red','Recursive grid','Uniform','Solid','Normal','Balanced','Loop',20,14,'','' union all
select 13000862,862,'White','Black','N/A','3x3 grid','Uniform','Bullseye','Normal','Off-center','Weave',9,5,'','' union all
select 13000863,863,'White','Black','N/A','4x4 grid','Uniform','Solid','Normal','Balanced','Loop',12,12,'','' union all
select 13000864,864,'White','Black','Red','4x4 grid','Uniform','Solid','Normal','Balanced','Weave',16,9,'','' union all
select 13000865,865,'White','Black','N/A','5x5 grid','Uniform','Bullseye','Normal','Off-center','Loop',25,17,'','' union all
select 13000866,866,'Yellow','Yellow','N/A','4x4 grid','Uniform','Bullseye','Normal','Balanced','Weave',8,8,'','' union all
select 13000867,867,'White','White','N/A','3x3 grid','Bigger near center','Solid','Normal','Balanced','Loop',6,6,'','' union all
select 13000868,868,'White','White','N/A','4x4 grid','Bigger near center','Bullseye','Normal','Balanced','Loop',8,8,'','' union all
select 13000869,869,'Red','Black','Red','3x3 grid','Uniform','Solid','Normal','Balanced','Weave',9,4,'','' union all
select 13000870,870,'White','White','N/A','3x3 grid','Uniform','Solid','Normal','Balanced','Weave',9,7,'','' union all
select 13000871,871,'White','Black','N/A','3x3 grid','Uniform','Solid','Normal','Balanced','Weave',9,5,'','' union all
select 13000872,872,'White','White','N/A','4x4 grid','Smaller near center','Solid','Normal','Balanced','Weave',16,11,'','' union all
select 13000873,873,'White','White','Red','4x4 grid','Uniform','Solid','Normal','Balanced','Weave',16,11,'','' union all
select 13000874,874,'White','White','N/A','Tiled 4-3','Bigger near center','Solid','Normal','Balanced','Loop',7,7,'','' union all
select 13000875,875,'White','White','N/A','4x4 grid','Uniform','Solid','smol boi','Off-center','Weave',12,12,'','' union all
select 13000876,876,'White','Black','N/A','4x4 grid','Smaller near center','Bullseye','Normal','Off-center','Weave',16,11,'','' union all
select 13000877,877,'White','White','N/A','Recursive grid','Uniform','Solid','Normal','Balanced','Weave',31,17,'','' union all
select 13000878,878,'White','Black','Red','5x5 grid','Uniform','Solid','Normal','Balanced','Weave',25,12,'','' union all
select 13000879,879,'White','Yellow','N/A','Recursive grid','Uniform','Solid','Normal','Off-center','Weave',10,10,'Yes','' union all
select 13000880,880,'White','White','N/A','3x3 grid','Smaller near center','Solid','Normal','Balanced','Loop',6,6,'','' union all
select 13000881,881,'White','White','N/A','3x3 grid','Smaller near center','Solid','Normal','Off-center','Weave',5,5,'','' union all
select 13000882,882,'Beige','White','Red','Tiled 5-4','Uniform','Solid','Normal','Balanced','Weave',23,15,'','' union all
select 13000883,883,'White','White','N/A','Recursive grid','Uniform','Solid','Normal','Balanced','Weave',15,15,'','' union all
select 13000884,884,'White','White','N/A','Tiled 5-4','Smaller near center','Solid','Normal','Balanced','Loop',17,17,'','' union all
select 13000885,885,'White','White','N/A','Tiled 4-5','Smaller near center','Solid','Normal','Off-center','Weave',22,13,'','' union all
select 13000886,886,'White','White','N/A','5x5 grid','Uniform','Solid','Normal','Balanced','Weave',14,14,'','' union all
select 13000887,887,'White','Black','N/A','4x4 grid','Bigger near center','Solid','Normal','Balanced','Weave',16,8,'','' union all
select 13000888,888,'White','White','Red','5x5 grid','Uniform','Solid','Normal','Balanced','Weave',25,14,'','' union all
select 13000889,889,'White','Yellow','Blue','3x3 grid','Uniform','Solid','Normal','Balanced','Loop',9,6,'','' union all
select 13000890,890,'White','White','N/A','3x3 grid','Uniform','Solid','Normal','Balanced','Loop',9,5,'','' union all
select 13000891,891,'White','White','N/A','3x3 grid','Uniform','Solid','Normal','Balanced','Loop',4,4,'','' union all
select 13000892,892,'White','Black','N/A','Recursive grid','Bigger near center','Bullseye','Normal','Balanced','Loop',10,10,'','' union all
select 13000893,893,'White','Yellow','N/A','5x5 grid','Uniform','Solid','Normal','Off-center','Loop',25,14,'','' union all
select 13000894,894,'White','White','N/A','5x5 grid','Uniform','Solid','Normal','Balanced','Weave',17,17,'','' union all
select 13000895,895,'White','Black','N/A','4x4 grid','Uniform','Solid','Normal','Balanced','Loop',16,8,'','' union all
select 13000896,896,'White','White','Red','4x4 grid','Bigger near center','Solid','Normal','Balanced','Loop',16,9,'','' union all
select 13000897,897,'White','White','Red','4x4 grid','Bigger near center','Solid','Normal','Balanced','Loop',16,12,'','' union all
select 13000898,898,'White','Black','N/A','3x3 grid','Bigger near center','Bullseye','Normal','Balanced','Loop',5,5,'','' union all
select 13000899,899,'White','White','N/A','3x3 grid','Uniform','Solid','Normal','Balanced','Weave',9,4,'','' union all
select 13000900,900,'White','White','N/A','Tiled 3-2','Uniform','Solid','Normal','Balanced','Weave',8,5,'','' union all
select 13000901,901,'White','Black','Red','3x3 grid','Uniform','Solid','Normal','Balanced','Weave',9,5,'','' union all
select 13000902,902,'Black','Black','N/A','4x4 grid','Smaller near center','Solid','Normal','Balanced','Loop',10,10,'','Dark mode' union all
select 13000903,903,'White','White','N/A','3x3 grid','Uniform','Solid','Normal','Balanced','Loop',4,4,'','' union all
select 13000904,904,'White','White','N/A','Tiled 4-5','Uniform','Solid','Normal','Balanced','Weave',15,15,'Yes','' union all
select 13000905,905,'White','White','Red','4x4 grid','Uniform','Solid','Normal','Balanced','Loop',16,10,'','' union all
select 13000906,906,'White','White','N/A','5x5 grid','Uniform','Solid','Normal','Balanced','Loop',15,15,'','' union all
select 13000907,907,'White','Black','N/A','Tiled 5-6','Uniform','Solid','Normal','Off-center','Weave',33,24,'','' union all
select 13000908,908,'White','Black','N/A','3x3 grid','Uniform','Solid','Normal','Balanced','Weave',5,5,'','' union all
select 13000909,909,'White','White','N/A','3x3 grid','Uniform','Solid','Normal','Balanced','Loop',6,6,'','' union all
select 13000910,910,'White','Black','N/A','4x4 grid','Smaller near center','Solid','Normal','Off-center','Weave',16,12,'','' union all
select 13000911,911,'Red','Black','N/A','3x3 grid','Uniform','Bullseye','Normal','Off-center','Loop',5,5,'','' union all
select 13000912,912,'Beige','White','N/A','3x3 grid','Uniform','Solid','Normal','Balanced','Loop',5,5,'','' union all
select 13000913,913,'White','White','Red','Tiled 2-3','Smaller near center','Solid','Normal','Off-center','Weave',7,4,'','' union all
select 13000914,914,'White','Black','N/A','4x4 grid','Uniform','Solid','Normal','Off-center','Weave',16,8,'','' union all
select 13000915,915,'White','Black','N/A','Tiled 5-4','Uniform','Bullseye','Normal','Balanced','Weave',16,16,'','' union all
select 13000916,916,'White','White','N/A','4x4 grid','Smaller near center','Solid','smol boi','Off-center','Weave',10,10,'','' union all
select 13000917,917,'White','White','N/A','Tiled 5-4','Uniform','Solid','Normal','Balanced','Weave',13,13,'','' union all
select 13000918,918,'Blue','Blue','Blue','4x4 grid','Uniform','Solid','Normal','Off-center','Loop',16,11,'','' union all
select 13000919,919,'White','White','N/A','5x5 grid','Uniform','Solid','Normal','Off-center','Weave',15,15,'','' union all
select 13000920,920,'White','Black','N/A','5x5 grid','Uniform','Solid','Normal','Balanced','Loop',13,13,'','' union all
select 13000921,921,'White','White','N/A','Tiled 3-4','Uniform','Solid','Normal','Balanced','Weave',14,8,'','' union all
select 13000922,922,'White','White','N/A','Tiled 3-2','Uniform','Solid','Normal','Balanced','Loop',8,5,'','' union all
select 13000923,923,'White','Black','N/A','Recursive grid','Uniform','Solid','Normal','Balanced','Weave',20,15,'','' union all
select 13000924,924,'Red','White','N/A','4x4 grid','Uniform','Solid','Normal','Balanced','Loop',16,10,'','' union all
select 13000925,925,'Red','Black','N/A','5x5 grid','Uniform','Solid','Normal','Balanced','Loop',16,16,'','' union all
select 13000926,926,'White','White','N/A','Tiled 5-4','Uniform','Solid','Normal','Balanced','Loop',23,12,'','' union all
select 13000927,927,'White','White','N/A','5x5 grid','Uniform','Solid','Normal','Balanced','Weave',15,15,'','' union all
select 13000928,928,'Red','Black','N/A','3x3 grid','Uniform','Solid','Normal','Balanced','Weave',5,5,'','' union all
select 13000929,929,'White','White','N/A','Recursive grid','Uniform','Solid','Normal','Off-center','Loop',4,4,'','' union all
select 13000930,930,'White','Black','Red','3x3 grid','Smaller near center','Solid','Normal','Balanced','Weave',9,4,'','' union all
select 13000931,931,'White','Black','N/A','Tiled 4-3','Uniform','Solid','Normal','Balanced','Loop',14,7,'','' union all
select 13000932,932,'White','Black','N/A','4x4 grid','Uniform','Solid','Normal','Off-center','Weave',16,8,'','' union all
select 13000933,933,'Beige','White','N/A','Recursive grid','Uniform','Solid','Normal','Off-center','Loop',16,16,'','' union all
select 13000934,934,'White','White','N/A','4x4 grid','Smaller near center','Solid','Normal','Balanced','Weave',9,9,'','' union all
select 13000935,935,'White','Black','N/A','3x3 grid','Uniform','Solid','Normal','Balanced','Loop',9,6,'','' union all
select 13000936,936,'White','White','N/A','3x3 grid','Uniform','Solid','Normal','Balanced','Weave',6,6,'','' union all
select 13000937,937,'Red','White','N/A','Tiled 4-5','Smaller near center','Solid','Normal','Balanced','Weave',22,15,'','' union all
select 13000938,938,'White','White','N/A','4x4 grid','Uniform','Solid','Normal','Balanced','Weave',10,10,'','' union all
select 13000939,939,'White','White','N/A','3x3 grid','Uniform','Solid','Normal','Balanced','Weave',9,6,'','' union all
select 13000940,940,'White','Black','N/A','Tiled 3-4','Smaller near center','Solid','Normal','Balanced','Weave',14,8,'','' union all
select 13000941,941,'Yellow','Black','N/A','Recursive grid','Uniform','Solid','Normal','Off-center','Weave',22,22,'','' union all
select 13000942,942,'White','White','N/A','4x4 grid','Uniform','Solid','Normal','Balanced','Loop',16,11,'','' union all
select 13000943,943,'Beige','Black','N/A','3x3 grid','Bigger near center','Solid','Normal','Balanced','Weave',9,6,'','' union all
select 13000944,944,'White','White','N/A','3x3 grid','Uniform','Bullseye','Normal','Balanced','Weave',9,6,'','' union all
select 13000945,945,'White','Black','Blue','4x4 grid','Uniform','Solid','smol boi','Off-center','Weave',16,8,'Yes','' union all
select 13000946,946,'White','White','N/A','5x5 grid','Uniform','Solid','Normal','Balanced','Weave',25,17,'','' union all
select 13000947,947,'White','White','N/A','3x3 grid','Bigger near center','Solid','Normal','Off-center','Loop',4,4,'','' union all
select 13000948,948,'White','White','N/A','4x4 grid','Uniform','Solid','Normal','Balanced','Loop',9,9,'','' union all
select 13000949,949,'White','White','N/A','Recursive grid','Uniform','Solid','Normal','Balanced','Weave',3,3,'','' union all
select 13000950,950,'White','Black','N/A','5x5 grid','Uniform','Solid','Normal','Balanced','Weave',19,19,'','' union all
select 13000951,951,'White','White','N/A','3x3 grid','Uniform','Solid','Normal','Balanced','Loop',4,4,'','' union all
select 13000952,952,'Beige','Yellow','N/A','5x5 grid','Uniform','Solid','Normal','Balanced','Weave',13,13,'Yes','' union all
select 13000953,953,'White','White','N/A','3x3 grid','Uniform','Solid','Normal','Off-center','Weave',6,6,'','' union all
select 13000954,954,'White','White','N/A','4x4 grid','Smaller near center','Solid','Normal','Balanced','Loop',9,9,'','' union all
select 13000955,955,'White','White','N/A','Recursive grid','Uniform','Solid','Normal','Balanced','Weave',37,22,'','' union all
select 13000956,956,'Red','Yellow','N/A','4x4 grid','Uniform','Solid','Normal','Off-center','Loop',11,11,'','' union all
select 13000957,957,'White','Red','N/A','3x3 grid','Smaller near center','Solid','Normal','Balanced','Weave',6,6,'','' union all
select 13000958,958,'Red','Yellow','N/A','4x4 grid','Uniform','Solid','Normal','Off-center','Weave',16,9,'','' union all
select 13000959,959,'White','Black','Red','5x5 grid','Uniform','Bullseye','Normal','Balanced','Loop',25,13,'','' union all
select 13000960,960,'White','Black','N/A','Recursive grid','Uniform','Solid','Normal','Balanced','Loop',23,14,'','' union all
select 13000961,961,'White','Red','N/A','Recursive grid','Uniform','Solid','Normal','Balanced','Weave',23,12,'','' union all
select 13000962,962,'Yellow','Black','N/A','5x5 grid','Uniform','Solid','Normal','Balanced','Weave',15,15,'','' union all
select 13000963,963,'White','Red','N/A','5x5 grid','Uniform','Solid','Normal','Off-center','Loop',17,17,'','' union all
select 13000964,964,'White','Yellow','N/A','5x5 grid','Uniform','Solid','Normal','Balanced','Loop',25,16,'','' union all
select 13000965,965,'White','White','N/A','Recursive grid','Uniform','Solid','Normal','Balanced','Loop',12,8,'','' union all
select 13000966,966,'White','White','Red','4x4 grid','Bigger near center','Solid','Normal','Off-center','Weave',16,8,'Yes','' union all
select 13000967,967,'White','White','N/A','Recursive grid','Uniform','Solid','Normal','Balanced','Weave',7,7,'','' union all
select 13000968,968,'White','White','N/A','5x5 grid','Uniform','Solid','Normal','Balanced','Loop',25,18,'','' union all
select 13000969,969,'White','Black','N/A','Recursive grid','Uniform','Solid','Normal','Off-center','Weave',7,4,'','' union all
select 13000970,970,'White','Black','N/A','5x5 grid','Uniform','Solid','Normal','Balanced','Loop',25,17,'','' union all
select 13000971,971,'White','Black','N/A','Tiled 4-3','Uniform','Solid','Normal','Off-center','Weave',14,9,'Yes','' union all
select 13000972,972,'White','Black','N/A','3x3 grid','Uniform','Bullseye','Normal','Balanced','Weave',6,6,'','' union all
select 13000973,973,'White','Black','Red','Tiled 5-4','Uniform','Solid','smol boi','Balanced','Weave',23,15,'','' union all
select 13000974,974,'Beige','Black','N/A','Recursive grid','Bigger near center','Solid','Normal','Off-center','Loop',16,16,'','' union all
select 13000975,975,'White','White','N/A','5x5 grid','Uniform','Solid','Normal','Balanced','Loop',19,19,'','' union all
select 13000976,976,'Yellow','White','N/A','4x4 grid','Uniform','Bullseye','Normal','Balanced','Loop',16,11,'','' union all
select 13000977,977,'White','Black','Red','Tiled 4-3','Uniform','Solid','Normal','Balanced','Loop',14,9,'','' union all
select 13000978,978,'White','White','Red','5x5 grid','Uniform','Solid','Normal','Balanced','Loop',25,17,'','' union all
select 13000979,979,'White','White','N/A','5x5 grid','Bigger near center','Solid','smol boi','Off-center','Weave',16,16,'','' union all
select 13000980,980,'White','Black','N/A','4x4 grid','Uniform','Solid','Normal','Balanced','Loop',16,11,'','' union all
select 13000981,981,'Yellow','White','N/A','4x4 grid','Uniform','Solid','Normal','Off-center','Weave',11,11,'','' union all
select 13000982,982,'White','Black','N/A','Tiled 5-4','Bigger near center','Solid','Normal','Off-center','Loop',15,15,'','' union all
select 13000983,983,'White','White','N/A','3x3 grid','Uniform','Solid','Normal','Balanced','Loop',5,5,'','' union all
select 13000984,984,'White','White','N/A','5x5 grid','Uniform','Solid','Normal','Balanced','Weave',25,16,'','' union all
select 13000985,985,'White','Black','N/A','5x5 grid','Uniform','Solid','Normal','Balanced','Loop',18,18,'','' union all
select 13000986,986,'White','White','Red','Recursive grid','Bigger near center','Solid','Normal','Balanced','Weave',15,9,'','' union all
select 13000987,987,'Yellow','Black','N/A','3x3 grid','Uniform','Solid','Normal','Balanced','Weave',5,5,'','' union all
select 13000988,988,'White','White','N/A','5x5 grid','Uniform','Solid','Normal','Balanced','Loop',25,12,'','' union all
select 13000989,989,'Black','Black','N/A','4x4 grid','Smaller near center','Solid','Normal','Balanced','Weave',11,11,'','Dark mode' union all
select 13000990,990,'White','Black','N/A','5x5 grid','Uniform','Solid','Normal','Balanced','Loop',18,18,'','' union all
select 13000991,991,'White','Black','N/A','5x5 grid','Uniform','Solid','Normal','Balanced','Loop',25,16,'','' union all
select 13000992,992,'White','Black','Red','4x4 grid','Uniform','Solid','Normal','Off-center','Weave',16,8,'','' union all
select 13000993,993,'White','Black','Red','Tiled 5-4','Uniform','Solid','Normal','Off-center','Weave',23,17,'','' union all
select 13000994,994,'White','Black','Blue','Tiled 2-3','Uniform','Solid','Normal','Off-center','Weave',7,5,'','' union all
select 13000995,995,'White','White','N/A','4x4 grid','Uniform','Solid','Normal','Balanced','Loop',16,11,'','' union all
select 13000996,996,'White','Yellow','N/A','Tiled 3-4','Bigger near center','Solid','Normal','Balanced','Weave',14,9,'','' union all
select 13000997,997,'Beige','White','N/A','Recursive grid','Bigger near center','Solid','Normal','Off-center','Loop',6,6,'','' union all
select 13000998,998,'White','White','Blue','5x5 grid','Uniform','Bullseye','Normal','Balanced','Weave',25,13,'','' union all
select 13000999,999,'White','Red','N/A','Recursive grid','Bigger near center','Solid','Normal','Balanced','Weave',41,22,'',''