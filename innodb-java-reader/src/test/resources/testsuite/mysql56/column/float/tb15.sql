DROP TABLE IF EXISTS `tb15`;
CREATE TABLE `tb15`
(`id` int(11) unsigned NOT NULL AUTO_INCREMENT,
`c_float` FLOAT NOT NULL,
`c_real` FLOAT NOT NULL ,
`c_double` DOUBLE NOT NULL,
PRIMARY KEY (`id`))
ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

insert into tb15 values(null, 0, 0, 0);
insert into tb15 values(null, 0.56789, 0.12345, 0.987654321);
insert into tb15 values(null, 1, -1, -1);
insert into tb15 values(null, 222.22, 222.22, 3333.333);
insert into tb15 values(null, 12345678.1234, 12345678.1234, 1234567890.123456);
insert into tb15 values(null, -12345678.1234, -12345678.1234, -1234567890.123456);