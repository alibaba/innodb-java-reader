DROP TABLE IF EXISTS `tb04`;
CREATE TABLE `tb04`
(`id` int(11) NOT NULL ,
`a` varchar(32) NOT NULL,
`b` varchar(64) NOT NULL,
`c` varchar(255) NOT NULL,
`d` varchar(256) NOT NULL,
`e` varchar(512) NOT NULL,
`f` char(32) NOT NULL,
`g` char(255) NOT NULL,
PRIMARY KEY (`id`))
ENGINE=InnoDB DEFAULT CHARSET=latin1;;

delimiter ;;
drop procedure if EXISTS idata;
create procedure idata()
begin
declare i int;
set i=1;
while(i<=10)do
if(i % 2 = 0)then
insert into tb04 values (i, concat(char(97+(i % 26)), REPEAT('a', 31)), concat(char(97+(i % 26)), REPEAT('b', 63)), concat(char(97+(i % 26)), REPEAT('c', 254)), concat(char(97+(i % 26)), REPEAT('d', 255)), concat(char(97+(i % 26)), REPEAT('e', 511)), concat(char(97+(i % 26)), REPEAT('f', 31)), concat(char(97+(i % 26)), REPEAT('g', 254)));
else
insert into tb04 values (i, concat(char(97+(i % 26)), REPEAT('a', 8)), concat(char(97+(i % 26)), REPEAT('b', 10)), concat(char(97+(i % 26)), REPEAT('c', 100)), concat(char(97+(i % 26)), REPEAT('d', 126)), concat(char(97+(i % 26)), REPEAT('e', 400)), concat(char(97+(i % 26)), REPEAT('f', 8)), concat(char(97+(i % 26)), REPEAT('g', 10)));
end if;
set i=i+1;
end while;
end;;
delimiter ;
call idata();