DROP TABLE IF EXISTS `tb17`;
CREATE TABLE `tb17`
(`id` int(11) NOT NULL AUTO_INCREMENT,
`a` int(11) NOT NULL,
`b` datetime(3) NOT NULL,
`c` datetime(6) NOT NULL,
`d` timestamp(6) NOT NULL,
`e` time(5) NOT NULL,
`f` datetime(0) NOT NULL,
PRIMARY KEY (`id`))
ENGINE=InnoDB;

SET @@session.time_zone = "+08:00";
insert into tb17 values(null, 100, '2019-10-02 10:59:59.123', '2000-01-01 00:01:03.100000', '2019-10-02 10:59:59.456389', '10:59:59.45638', '2019-10-02 10:59:59');
insert into tb17 values(null, 101, '1970-01-01 08:00:01.550', '2022-01-01 00:01:03.123450', '1970-01-01 08:00:01.000001', '08:00:01.00000', '1970-01-01 08:00:01');
insert into tb17 values(null, 102, '2008-11-23 09:23:00.808', '1999-12-31 00:01:03.123456', '2008-11-23 09:23:00.294000', '09:23:00.29400', '2008-11-23 09:23:00');