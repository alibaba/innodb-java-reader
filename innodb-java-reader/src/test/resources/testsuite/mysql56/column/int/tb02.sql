DROP TABLE IF EXISTS `tb02`;
CREATE TABLE `tb02`
(`id` int(11) unsigned NOT NULL AUTO_INCREMENT,
`c_utinyint` tinyint(11) unsigned NOT NULL ,
`c_tinyint` TINYINT(11) NOT NULL,
`c_usmallint` smallint(11) unsigned NOT NULL ,
`c_smallint` SMallInt(11) NOT NULL,
`c_umediumint` mediumint(11) unsigned NOT NULL ,
`c_mediumint` MEDIUMINT(11) NOT NULL,
`c_uint` INT(11) unsigned NOT NULL ,
`c_int` int(11) NOT NULL,
`c_ubigint` BIGINT(20) UNSIGNED NOT NULL,
`c_bigint` bigint(20) NOT NULL,
PRIMARY KEY (`id`))
ENGINE=InnoDB DEFAULT CHARSET=utf8 AUTO_INCREMENT = 100;

insert into tb02 values(null, 100, 100, 10000, 10000, 1000000, 1000000, 10000000, 10000000, 100000000000, 100000000000);
insert into tb02 values(null, 127, 127, 32767, 32767, 8388607, 8388607, 2147483647, 2147483647, 9223372036854775805, 9223372036854775807);
insert into tb02 values(null, 128, -128, 32768, -32768, 8388608, -8388608, 2147483648, -2147483648, 9223372036854775806, -9223372036854775807);
insert into tb02 values(null, 129, -127, 32769, -32767, 8388609, -8388607, 2147483649, -2147483647, 9223372036854775807, -9223372036854775806);
insert into tb02 values(null, 100, 100, 10000, 10000, 1000000, -1000000, 10000000, 10000000, 100000000000, 100000000000);

-- overflow value works in 5.6 but cannot set overflow values in 5.7, ERROR 1264 (22003): Out of range value for column 'c_tinyint' at row 1
