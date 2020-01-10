package com.alibaba.innodb.java.reader.schema;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.create.table.CreateTable;

import org.junit.Ignore;
import org.junit.Test;

/**
 * @author xu.zx
 */
public class JSqlParserTest {

  @Ignore
  @Test
  public void testParseSql() throws JSQLParserException {
    CreateTable stmt = (CreateTable) CCJSqlParserUtil.parse("CREATE TABLE `product001` (\n"
        + "  `id` bigint(20) NOT NULL COMMENT '主键',\n"
        + "  `user_id` bigint(20) unsigned DEFAULT '0' COMMENT '用户id,不再使用',\n"
        + "  `feed_id` int(11) DEFAULT '0' COMMENT 'feed id,不再使用',\n"
        + "  `outer_id` varchar(1024) NOT NULL COMMENT '用户定义的商品id',\n"
        + "  `feed_url_id` int(11) NOT NULL DEFAULT '0' COMMENT '抓取信息id',\n"
        + "  `name` varchar(200) NOT NULL COMMENT '商品名称',\n"
        + "  `loc` varchar(1024) NOT NULL COMMENT '商品详情页',\n"
        + "  `content` text COMMENT '商品属性',\n"
        + "  `content_hash` bigint(20) NOT NULL COMMENT 'content的hashcode',\n"
        + "  `version` bigint(20) NOT NULL COMMENT '版本',\n"
        + "  `name_hash` bigint(20) NOT NULL DEFAULT '0' COMMENT 'name hash',\n"
        + "  `deleted_state` int(11) NOT NULL DEFAULT '0' COMMENT '逻辑状态:0,有效 1，逻辑删除',\n"
        + "  PRIMARY KEY (`id`),\n"
        + "  KEY `ddd` (`user_id`) \n"
        + ") ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='商品信息'");
    System.out.println(stmt.toString());
    System.out.println(stmt.getCreateOptionsStrings());
    System.out.println(stmt.getTableOptionsStrings());
    System.out.println(stmt.getIndexes());
    System.out.println(stmt.getIndexes().get(0).getType());
    for (ColumnDefinition columnDefinition : stmt.getColumnDefinitions()) {
      System.out.println(columnDefinition.getColumnName());
      System.out.println(columnDefinition.getColDataType().getDataType());
      System.out.println(columnDefinition.getColDataType().getArgumentsStringList());
      System.out.println(columnDefinition.getColumnSpecStrings());
    }
  }

}
