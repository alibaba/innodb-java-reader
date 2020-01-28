DROP TABLE IF EXISTS `tb20`;
CREATE TABLE `tb20`
(`id` int(11) NOT NULL ,
`a` varchar(9) CHARACTER SET gbk COLLATE gbk_bin NOT NULL,
PRIMARY KEY (`id`))
ENGINE=InnoDB DEFAULT CHARSET=latin1;

insert into tb20 values (1, 'abc$&');
insert into tb20 values (2, '你好这里是哪里');