DROP TABLE IF EXISTS `tb06`;
CREATE TABLE `tb06`
(`id` int(11) NOT NULL,
`a` bigint(20) NOT NULL,
`b` varchar(16380) NOT NULL,
PRIMARY KEY (`id`))
ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
-- in 5.7 varchar(16383) got error
-- ERROR 1074 (42000): Column length too big for column 'b' (max = 16383); use BLOB or TEXT instead

delimiter ;;
drop procedure if EXISTS idata;
create procedure idata()
begin
declare i int;
set i=1;
while(i<=50)do
insert into tb06 values (i, i * 2, REPEAT(char(97+(i % 26)), 8100));
set i=i+1;
end while;
end;;
delimiter ;
call idata();