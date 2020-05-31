DROP TABLE IF EXISTS `tb29`;
CREATE TABLE `tb29`
(`id` int(11) NOT NULL,
`a` bigint(20) NOT NULL,
`b` varchar(64) NOT NULL
)ENGINE=InnoDB;

delimiter ;;
drop procedure if EXISTS idata;
create procedure idata()
begin
declare i int;
set i=1;
while(i<=5000)do
insert into tb29 values(i, i * 2, REPEAT(char(97+(i % 26)), 16));
set i=i+1;
end while;
end;;
delimiter ;
call idata();

-- leave some "holes" in the front, middle and ending part of ibd file
--0,FILE_SPACE_HEADER,numPagesUsed=15,size=25,xdes.size=1
--1,IBUF_BITMAP
--2,INODE,inode.size=2
--3,INDEX,level=1,numOfRecs=11
--4,INDEX  logical removal page
--5,INDEX  logical removal page
--6,INDEX  logical removal page
--7,INDEX  logical removal page
--8,INDEX
--9,INDEX
--10,INDEX
--11,INDEX
--12,INDEX
--13,INDEX
--14,INDEX
--15,INDEX  logical removal page
--16,INDEX  logical removal page
--17,INDEX
--18,INDEX
--19,INDEX
--20,INDEX
--21,INDEX  logical removal page
--22,INDEX  logical removal page
--23,ALLOCATED
--24,ALLOCATED
delete from tb29 where id < 1000;
delete from tb29 where id > 2000 and id < 2200;
delete from tb29 where id > 3000 and id < 3800;
delete from tb29 where id > 4500;