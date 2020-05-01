package com.alibaba.innodb.java.reader.main;

import com.google.common.collect.ImmutableList;

import com.alibaba.innodb.java.reader.TableReader;
import com.alibaba.innodb.java.reader.TableReaderImpl;
import com.alibaba.innodb.java.reader.comparator.ComparisonOperator;
import com.alibaba.innodb.java.reader.page.index.GenericRecord;
import com.alibaba.innodb.java.reader.schema.Column;
import com.alibaba.innodb.java.reader.schema.TableDef;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * <pre>
 * CREATE TABLE `product001` (
 *   `id` bigint(20) NOT NULL COMMENT '主键',
 *   `user_id` bigint(20) DEFAULT '0' COMMENT '用户id,不再使用',
 *   `feed_id` int(11) DEFAULT '0' COMMENT 'feed id,不再使用',
 *   `outer_id` varchar(1024) NOT NULL COMMENT '用户定义的商品id',
 *   `feed_url_id` int(11) NOT NULL DEFAULT '0' COMMENT '抓取信息id',
 *   `name` varchar(200) NOT NULL COMMENT '商品名称',
 *   `loc` varchar(1024) NOT NULL COMMENT '商品详情页',
 *   `content` text COMMENT '商品属性',
 *   `content_hash` bigint(20) NOT NULL COMMENT 'content的hashcode',
 *   `version` bigint(20) NOT NULL COMMENT '版本',
 *   `name_hash` bigint(20) NOT NULL DEFAULT '0' COMMENT 'name hash',
 *   `deleted_state` int(11) NOT NULL DEFAULT '0' COMMENT '逻辑状态:0,有效 1，逻辑删除',
 *   PRIMARY KEY (`id`),
 *   KEY `iii` (`feed_id`)
 * ) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='商品信息'
 * </pre>
 *
 * @author xu.zx
 */
public class Realcase1TableReaderMainTest {

  public static void main(String[] args) {
    TableDef tableDef = new TableDef()
        .addColumn(new Column().setName("id").setType("bigint(20)").setNullable(false).setPrimaryKey(true))
        .addColumn(new Column().setName("user_id").setType("bigint(20)").setNullable(true))
        .addColumn(new Column().setName("feed_id").setType("int(11)").setNullable(true))
        .addColumn(new Column().setName("outer_id").setType("varchar(1024)").setNullable(false))
        .addColumn(new Column().setName("feed_url_id").setType("int(11)").setNullable(false))
        .addColumn(new Column().setName("name").setType("varchar(200)").setNullable(false))
        .addColumn(new Column().setName("loc").setType("varchar(1024)").setNullable(false))
        .addColumn(new Column().setName("content").setType("text").setNullable(false))
        .addColumn(new Column().setName("content_hash").setType(" bigint(20)").setNullable(false))
        .addColumn(new Column().setName("version").setType("bigint(20)").setNullable(false))
        .addColumn(new Column().setName("name_hash").setType("bigint(20)").setNullable(false))
        .addColumn(new Column().setName("deleted_state").setType("int(11)").setNullable(false));

    try (TableReader reader = new TableReaderImpl("/usr/local/mysql/data/test/product001.ibd", tableDef)) {
      reader.open();

      for (long i = 1; i <= 10; i++) {
        GenericRecord record = reader.queryByPrimaryKey(ImmutableList.of(i));
        Object[] values = record.getValues();
        System.out.println(Arrays.asList(values));
      }

      List<GenericRecord> records = reader.rangeQueryByPrimaryKey(
          ImmutableList.of(5000L), ComparisonOperator.GTE,
          ImmutableList.of(5100L), ComparisonOperator.LT);
      for (GenericRecord record : records) {
        System.out.println(Arrays.toString(record.getValues()));
      }

      Iterator<GenericRecord> iterator = reader.getQueryAllIterator();
      int count = 0;
      while (iterator.hasNext()) {
        GenericRecord record = iterator.next();
        if (((long) record.get("id") % 9 == 0)) {
          count++;
        }
      }
      System.out.println(count);
    }
  }

}
