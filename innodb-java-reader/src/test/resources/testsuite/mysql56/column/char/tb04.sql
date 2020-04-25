DROP TABLE IF EXISTS `tb04`;
CREATE TABLE `tb04`
(`id` int(11) NOT NULL ,
`a` varchar(32) NOT NULL,
`b` varchar(64) NOT NULL,
`c` varchar(254) NOT NULL,
`d` varchar(255) NOT NULL,
`e` varchar(256) NOT NULL,
`f` varchar(512) NOT NULL,
`g` varchar(16384) NOT NULL,
`h` varchar(47474) NOT NULL, -- max row length is 65535, for all columns length summary
`i` char(1) NOT NULL,
`j` char(32) NOT NULL,
`k` char(255) NOT NULL,
PRIMARY KEY (`id`))
ENGINE=InnoDB;

delimiter ;;
drop procedure if EXISTS idata;
create procedure idata()
begin
declare i int;
set i=1;
while(i<=10)do
if(i % 2 = 0)then
insert into tb04 values
(i,
concat(char(97+(i % 26)), REPEAT('a', 31)),
concat(char(97+(i % 26)), REPEAT('b', 63)),
concat(char(97+(i % 26)), REPEAT('c', 253)),
concat(char(97+(i % 26)), REPEAT('d', 254)),
concat(char(97+(i % 26)), REPEAT('e', 255)),
concat(char(97+(i % 26)), REPEAT('f', 511)),
concat(char(97+(i % 26)), REPEAT('g', 16383)),
concat(char(97+(i % 26)), REPEAT('h', 47473)),
char(97+(i % 26)),
concat(char(97+(i % 26)), REPEAT('j', 31)),
concat(char(97+(i % 26)), REPEAT('k', 254))
);
else
insert into tb04 values
(i,
concat(char(97+(i % 26)), REPEAT('a', 1)),
concat(char(97+(i % 26)), REPEAT('b', 10)),
concat(char(97+(i % 26)), REPEAT('c', 126)),
concat(char(97+(i % 26)), REPEAT('d', 127)),
concat(char(97+(i % 26)), REPEAT('e', 128)),
concat(char(97+(i % 26)), REPEAT('f', 400)),
concat(char(97+(i % 26)), REPEAT('g', 10000)),
concat(char(97+(i % 26)), REPEAT('h', 40000)),
'',
concat(char(97+(i % 26)), REPEAT('j', 8)),
concat(char(97+(i % 26)), REPEAT('k', 10))
);
end if;
set i=i+1;
end while;
end;;
delimiter ;
call idata();

-- below is test data for mysql8.0

DROP TABLE IF EXISTS `tb04`;
CREATE TABLE `tb04`
(`id` int(11) NOT NULL ,
`a` varchar(32) NOT NULL,
`b` varchar(64) NOT NULL,
`c` varchar(254) NOT NULL,
`d` varchar(255) NOT NULL,
`e` varchar(256) NOT NULL,
`f` varchar(512) NOT NULL,
`g` varchar(16384) NOT NULL,
`h` varchar(47474) NOT NULL, -- max row length is 65535, for all columns length summary
`i` char(1) NOT NULL,
`j` char(32) NOT NULL,
`k` char(255) NOT NULL,
PRIMARY KEY (`id`))
ENGINE=InnoDB CHARSET latin1;