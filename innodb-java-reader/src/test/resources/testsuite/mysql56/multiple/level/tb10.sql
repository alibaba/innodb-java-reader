DROP TABLE IF EXISTS `tb10`;
CREATE TABLE `tb10`
(`id` int(11) NOT NULL ,
`a` bigint(20) NOT NULL,
`b` varchar(64) NOT NULL,
`c` varchar(1024) NOT NULL,
PRIMARY KEY (`id`),
UNIQUE KEY (`a`),
INDEX (`b`)
)
ENGINE=InnoDB;

delimiter ;;
drop procedure if EXISTS idata;
create procedure idata()
begin
declare i int;
set i=1;
while(i<=1000)do
insert into tb10 values(i, i * 2, REPEAT(char(97+(i % 26)), 32), REPEAT(char(97+(i % 26)), 512));
set i=i+1;
end while;
end;;
delimiter ;
call idata();