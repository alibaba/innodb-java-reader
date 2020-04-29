DROP TABLE IF EXISTS `tb21`;
CREATE TABLE `tb21`(
`a` int(11) NOT NULL,
`b` varchar(10) NOT NULL,
`c` varchar(10) NOT NULL)
ENGINE=InnoDB;

insert into tb21(a, b, c) values(100, 'Jason', REPEAT('x', 8));
insert into tb21(a, b, c) values(200, 'Eric', REPEAT('y', 7));
insert into tb21(a, b, c) values(300, 'Tom', REPEAT('z', 6));