DROP TABLE IF EXISTS `tb03`;
CREATE TABLE `tb03`
(`id` int(11) NOT NULL AUTO_INCREMENT,
`a` int(11) NOT NULL,
`b` datetime NOT NULL,
`c` timestamp NOT NULL,
`d` time NOT NULL,
PRIMARY KEY (`id`))
ENGINE=InnoDB;

SET @@session.time_zone = "+08:00";
insert into tb03 values(null, 100, '2019-10-02 10:59:59', '2019-10-02 10:59:59', '10:59:59');
insert into tb03 values(null, 101, '1970-01-01 08:00:01', '1970-01-01 08:00:01', '08:00:01');
insert into tb03 values(null, 102, '2008-11-23 09:23:00', '2008-11-23 09:23:00', '09:23:00');