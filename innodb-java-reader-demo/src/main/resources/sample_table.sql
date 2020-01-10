DROP TABLE IF EXISTS `t`;
CREATE TABLE `t`
(`id` int(11) NOT NULL,
`a` bigint(20) NOT NULL,
`b` varchar(64) NOT NULL,
PRIMARY KEY (`id`))
ENGINE=InnoDB;

delimiter ;;
drop procedure if EXISTS idata;
create procedure idata()
  begin
    declare i int;
    set i=1;
    while(i<=5)do
      insert into t values(i, i * 2, REPEAT(char(97+((i - 1) % 26)), 8));
      set i=i+1;
    end while;
  end;;
delimiter ;
call idata();