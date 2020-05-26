DROP TABLE IF EXISTS `tb28`;
CREATE TABLE `tb28`(
`a` int(11) NOT NULL,
`b` varchar(10) NOT NULL,
`c` varchar(10) NOT NULL,
`d` varchar(10) DEFAULT '',
`e` varchar(10) NOT NULL,
UNIQUE INDEX `key_d` (`d`),
UNIQUE INDEX `key_e_d` (`e`, `d`),
KEY `key_e` (`e`),
KEY `key_a` (`a`),
UNIQUE KEY `key_b` (`b`),
KEY `key_c` (`c`)
)
ENGINE=InnoDB;

delimiter ;;
drop procedure if EXISTS idata;
create procedure idata()
begin
declare i int;
set i=1;
while(i<=40)do
insert into tb28 values(i, concat(REPEAT('b', 2), i), concat(REPEAT('c', 2), i), concat(REPEAT('D', 2), i), concat(REPEAT('E', 2), i));
set i=i+1;
end while;
end;;
delimiter ;
call idata();