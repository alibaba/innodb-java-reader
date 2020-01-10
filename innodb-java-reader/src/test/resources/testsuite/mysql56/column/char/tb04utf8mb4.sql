DROP TABLE IF EXISTS `tb04utf8mb4`;
CREATE TABLE `tb04utf8mb4`
(`id` int(11) NOT NULL ,
`a` varchar(32) NOT NULL,
`b` varchar(64) NOT NULL,
`c` varchar(255) NOT NULL,
`d` varchar(256) NOT NULL,
`e` varchar(512) NOT NULL,
`f` char(32) NOT NULL,
`g` char(255) NOT NULL,
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
insert into tb04utf8mb4 values (i, concat(char(97+(i % 26)), REPEAT('东', 31)), concat(char(97+(i % 26)), REPEAT('西', 63)), concat(char(97+(i % 26)), REPEAT('南', 254)), concat(char(97+(i % 26)), REPEAT('北', 255)), concat(char(97+(i % 26)), REPEAT('中', 511)), concat(char(97+(i % 26)), REPEAT('左', 31)), concat(char(97+(i % 26)), REPEAT('右', 254)));
else
insert into tb04utf8mb4 values (i, concat(char(97+(i % 26)), REPEAT('东', 8)), concat(char(97+(i % 26)), REPEAT('西', 10)), concat(char(97+(i % 26)), REPEAT('南', 100)), concat(char(97+(i % 26)), REPEAT('北', 126)), concat(char(97+(i % 26)), REPEAT('中', 400)), concat(char(97+(i % 26)), REPEAT('左', 8)), concat(char(97+(i % 26)), REPEAT('右', 10)));
end if;
set i=i+1;
end while;
end;;
delimiter ;
call idata();