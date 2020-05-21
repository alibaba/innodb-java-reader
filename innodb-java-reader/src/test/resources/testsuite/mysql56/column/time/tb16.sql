DROP TABLE IF EXISTS `tb16`;
CREATE TABLE `tb16`
(`id` int(11) NOT NULL AUTO_INCREMENT,
`a` year NOT NULL,
`b` date NOT NULL,
PRIMARY KEY (`id`))
ENGINE=InnoDB;

insert into tb16 values(null, 0, '2100-11-11');
insert into tb16 values(null, 1, '2155-01-01');
insert into tb16 values(null, 1901, '1900-01-01');
insert into tb16 values(null, 1999, '1901-12-31');
insert into tb16 values(null, 1969, '1969-10-02');
insert into tb16 values(null, 2020, '2020-12-31');
insert into tb16 values(null, 2100, '0069-01-10');
insert into tb16 values(null, 2155, '0001-01-01');