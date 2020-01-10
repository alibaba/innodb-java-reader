DROP TABLE IF EXISTS `tb09`;
CREATE TABLE `tb09`
(`id` int(11) NOT NULL ,
`a` TINYBLOB NOT NULL,
`b` BLOB NOT NULL,
`c` MEDIUMBLOB NOT NULL,
`d` LONGBLOB NOT NULL,
PRIMARY KEY (`id`))
ENGINE=InnoDB;


delimiter ;;
drop procedure if EXISTS idata;
create procedure idata()
begin
declare i int;
set i=1;
while(i<=10)do
insert into tb09 values (i, concat(char(97+(i % 26)), REPEAT(0x0a, 200)), concat(char(97+(i % 26)), REPEAT(0x0b, 60000)), concat(char(97+(i % 26)), REPEAT(0x0c, 80000)), concat(char(97+(i % 26)), REPEAT(0x0d, 100000)));
set i=i+1;
end while;
end;;
delimiter ;
call idata();