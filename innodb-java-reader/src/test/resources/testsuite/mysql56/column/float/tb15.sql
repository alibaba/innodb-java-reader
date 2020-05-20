DROP TABLE IF EXISTS `tb15`;
CREATE TABLE `tb15`
(`id` int(11) unsigned NOT NULL AUTO_INCREMENT,
`c_float` FLOAT NOT NULL,
`c_float2` FLOAT(7,4) NOT NULL,
`c_real` FLOAT NOT NULL ,
`c_double` DOUBLE NOT NULL,
`c_double2` DOUBLE(15, 5) NOT NULL,
`c_double3` DOUBLE UNSIGNED NOT NULL,
PRIMARY KEY (`id`))
ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

insert into tb15 values(null, 0, 0, 0.0, 0, 0.0, 0);
insert into tb15 values(null, 0.56789, 999.0001, 0.12345, 0.987654321, 1234567890.12345, 1.0);
insert into tb15 values(null, 1, 0.0000, -1, -1, -1234567890.12345, 2.0);
insert into tb15 values(null, 222.22, 3.14, 222.22, 3333.333, 1234.56789, 3.0);
insert into tb15 values(null, 12345678.1234, 256.789, 12345678.1234, 1234567890.123456, -56.789, 4.0);
insert into tb15 values(null, -12345678.1234, 333.2222, -12345678.1234, -1234567890.123456, -0.87654, 5.0);