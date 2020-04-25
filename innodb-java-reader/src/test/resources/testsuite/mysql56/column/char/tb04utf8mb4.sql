DROP TABLE IF EXISTS `tb04utf8mb4`;
CREATE TABLE `tb04utf8mb4`
(`id` int(11) NOT NULL ,
`a` varchar(32) NOT NULL,
`b` varchar(64) NOT NULL,
`c` varchar(254) NOT NULL,
`d` varchar(255) NOT NULL,
`e` varchar(256) NOT NULL,
`f` varchar(512) NOT NULL,
`g` varchar(768) NOT NULL,
`h` varchar(13950) NOT NULL, -- The maximum row size for the used table type, not counting BLOBs, is 65535
`i` char(1) NOT NULL,
`j` char(32) NOT NULL,
`k` char(255) NOT NULL,
PRIMARY KEY (`id`))
ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

delimiter ;;
drop procedure if EXISTS idata;
create procedure idata()
begin
declare i int;
set i=1;
while(i<=10)do
if(i % 2 = 0)then
insert into tb04utf8mb4 values
(
i,
concat(char(97+(i % 26)), REPEAT('阿', 31)),
concat(char(97+(i % 26)), REPEAT('里', 63)),
concat(char(97+(i % 26)), REPEAT('巴', 253)),
concat(char(97+(i % 26)), REPEAT('数', 254)),
concat(char(97+(i % 26)), REPEAT('据', 255)),
concat(char(97+(i % 26)), REPEAT('库', 511)),
concat(char(97+(i % 26)), REPEAT('事', 767)),
concat(char(97+(i % 26)), REPEAT('业', 13949)),
char(97+(i % 26)),
concat(char(97+(i % 26)), REPEAT('辰', 31)),
concat(char(97+(i % 26)), REPEAT('序', 254))
);
else
insert into tb04utf8mb4 values (
i,
concat(char(97+(i % 26)), REPEAT('a', 1)),
concat(char(97+(i % 26)), REPEAT('里', 10)),
concat(char(97+(i % 26)), REPEAT('b', 126)),
concat(char(97+(i % 26)), REPEAT('数', 200)),
concat(char(97+(i % 26)), REPEAT('j', 220)),
concat(char(97+(i % 26)), REPEAT('库', 400)),
concat(char(97+(i % 26)), REPEAT('s', 500)),
concat(char(97+(i % 26)), REPEAT('业', 10000)),
'',
concat(char(97+(i % 26)), REPEAT('辰', 10)),
concat(char(97+(i % 26)), REPEAT('x', 100))
);
end if;
set i=i+1;
end while;
end;;
delimiter ;
call idata();