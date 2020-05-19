DROP TABLE IF EXISTS `tb21`;
CREATE TABLE `tb21`(
`a` int(11) NOT NULL,
`b` varchar(10) NOT NULL,
`c` varchar(10) NOT NULL,
KEY `key_b` (`b`),
KEY `key_a` (`a`)
)
ENGINE=InnoDB;

insert into tb21(a, b, c) values(600, 'Jason', REPEAT('a', 9));
insert into tb21(a, b, c) values(900, 'Eric', REPEAT('b', 8));
insert into tb21(a, b, c) values(1000, 'Tom', REPEAT('c', 7));
insert into tb21(a, b, c) values(500, 'Sarah', REPEAT('d', 6));
insert into tb21(a, b, c) values(400, 'jim', REPEAT('e', 5));
insert into tb21(a, b, c) values(100, 'tom', REPEAT('f', 4));
insert into tb21(a, b, c) values(200, 'jim', REPEAT('g', 3));
insert into tb21(a, b, c) values(800, 'Lucy', REPEAT('h', 2));
insert into tb21(a, b, c) values(700, 'smith', REPEAT('i', 1));
insert into tb21(a, b, c) values(300, 'jane', REPEAT('j', 8));