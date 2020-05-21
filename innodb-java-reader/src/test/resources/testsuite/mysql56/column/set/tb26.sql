DROP TABLE IF EXISTS `tb26`;
CREATE TABLE `tb26`
(`id` int(11) NOT NULL AUTO_INCREMENT,
`a` SET('music','movie','swimming','足球') NOT NULL,
`b` set ('a','b','c','d','e','f','g','h','i','j','k','l', 'm','n','o','p','q','r','s','t','u','v','w','x', 'y','z') NOT NULL,
`c` set('1','2','3','4','5','6','7','8','9','10','11','12','13','14','15','16','17','18','19','20','21','22','23','24','25','26','27','28','29','30','31','32','33','34','35','36','37','38','39','40','41','42','43','44','45','46','47','48','49','50','51','52','53','54','55','56','57','58','59','60','61','62','63','64') NOT NULL,
PRIMARY KEY (`id`))
ENGINE=InnoDB DEFAULT CHARSET=utf8;

insert into tb26 values(null, 'music', 'a,e,i,o,u', '3');
insert into tb26 values(null, 'movie,swimming', 'o,p,q', '1,5,60');
insert into tb26 values(null, '足球,movie', 'z', '1,2,3,4,5,6,7,8,9,10,11,12,13,14,24,31,33,37,48,49,50,55,63,64');