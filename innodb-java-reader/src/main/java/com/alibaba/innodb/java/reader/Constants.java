/*
 * Copyright (C) 1999-2019 Alibaba Group Holding Limited
 */
package com.alibaba.innodb.java.reader;

/**
 * Constants
 *
 * @author xu.zx
 */
public interface Constants {

  String DEFAULT_JAVA_CHARSET = "UTF-8";

  String DEFAULT_MYSQL_CHARSET = "utf8";

  /**
   * for mysql 5.6 and 5.7 the root page is usually page 3, but in mysql8 there introduces SDI page,
   * which may make root page number down from 4.
   * But the toolkit can be compatible with such case, if root page is SDI page then it will continue
   * search the file until the first index page found.
   */
  int ROOT_PAGE_NUMBER = 3;

  String CONST_UNSIGNED_UPPER = "UNSIGNED";

  String CONST_UNSIGNED_LOWER = "unsigned";

  int PRECISION_LIMIT = 5;

}
