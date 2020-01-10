DROP TABLE IF EXISTS `tb07`;
CREATE TABLE `tb07`
(`id` int(11) NOT NULL ,
`a` varbinary(32) NOT NULL,
`b` varbinary(255) NOT NULL,
`c` varbinary(512) NOT NULL,
`d` binary(32) NOT NULL,
`e` binary(255) NOT NULL,
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
insert into tb07 values (i, concat(char(97+(i % 26)), REPEAT(0x0a, 8)), concat(char(97+(i % 26)), REPEAT(0x0b, 254)), concat(char(97+(i % 26)), REPEAT(0x0c, 400)), concat(char(97+(i % 26)), REPEAT(0x0a, 8)), concat(char(97+(i % 26)), REPEAT(0x0b, 254)));
else
insert into tb07 values (i, concat(char(97+(i % 26)), REPEAT(0x0a, 8)), concat(char(97+(i % 26)), REPEAT(0x0b, 10)), concat(char(97+(i % 26)), REPEAT(0x0c, 400)), concat(char(97+(i % 26)), REPEAT(0x0a, 8)), concat(char(97+(i % 26)), REPEAT(0x0b, 10)));
end if;
set i=i+1;
end while;
end;;
delimiter ;
call idata();