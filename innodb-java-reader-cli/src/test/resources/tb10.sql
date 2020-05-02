--
-- Table structure for table `tb10`
--

DROP TABLE IF EXISTS `tb10`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;

CREATE TABLE `tb10`
(`id` int(11) NOT NULL ,
`a` bigint(20) NOT NULL,
`b` varchar(64) NOT NULL,
`c` varchar(1024) NOT NULL,
PRIMARY KEY (`id`))
ENGINE=InnoDB;