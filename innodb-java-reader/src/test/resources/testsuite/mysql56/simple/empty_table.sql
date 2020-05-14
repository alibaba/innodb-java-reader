DROP TABLE IF EXISTS `empty_table`;
CREATE TABLE `empty_table` (
`key` int(11) NOT NULL,
`value` varchar(288) DEFAULT NULL,
PRIMARY KEY  (`key`)
) ENGINE=InnoDB;