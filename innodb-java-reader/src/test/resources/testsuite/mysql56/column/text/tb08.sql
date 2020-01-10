DROP TABLE IF EXISTS `tb08`;
CREATE TABLE `tb08`
(`id` int(11) NOT NULL ,
`a` TINYTEXT NOT NULL,
`b` TEXT NOT NULL,
`c` MEDIUMTEXT NOT NULL,
`d` LONGTEXT NOT NULL,
PRIMARY KEY (`id`))
ENGINE=InnoDB;


delimiter ;;
drop procedure if EXISTS idata;
create procedure idata()
begin
declare i int;
set i=1;
while(i<=10)do
insert into tb08 values (i, concat(char(97+(i % 26)), REPEAT('a', 200)), concat(char(97+(i % 26)), REPEAT('b', 60000)), concat(char(97+(i % 26)), REPEAT('c', 80000)), concat(char(97+(i % 26)), REPEAT('d', 100000)));
set i=i+1;
end while;
end;;
delimiter ;
call idata();