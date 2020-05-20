package com.alibaba.innodb.java.reader;

import com.alibaba.innodb.java.reader.page.index.GenericRecord;

import java.util.Arrays;
import java.util.Iterator;

/**
 * Quick main test.
 *
 * @author xu.zx
 */
public class TableReaderMainTest {

  public static void main(String[] args) {
    String createTableSql = "CREATE TABLE `t`\n"
        + "(`id` int(11) NOT NULL ,\n"
        + "`a` bigint(20) NOT NULL,\n"
        + "`b` varchar(64) NOT NULL,\n"
        + "`c` varchar(1024) default 'THIS_IS_DEFAULT_VALUE',\n"
        + "PRIMARY KEY (`id`))\n"
        + "ENGINE=InnoDB;";
    String ibdFilePath = "path/t.ibd";
    try (TableReader reader = new TableReaderImpl(ibdFilePath, createTableSql)) {
      reader.open();

      // ~~~ query all records
      Iterator<GenericRecord> iterator = reader.getQueryAllIterator();
      int count = 0;
      while (iterator.hasNext()) {
        GenericRecord record = iterator.next();
        Object[] values = record.getValues();
        System.out.println(Arrays.asList(values));
        count++;
      }
      System.out.println(count);
    }
  }

}
