DROP TABLE IF EXISTS `tb18`;
CREATE TABLE `tb18`
(`id` int(11) NOT NULL AUTO_INCREMENT,
`a` boolean NOT NULL,
`b` BOOL NOT NULL,
PRIMARY KEY (`id`))
ENGINE=InnoDB;

insert into tb18 values(null, TRUE, FALSE);
insert into tb18 values(null, FALSE, TRUE);