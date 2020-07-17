/*
 * Copyright (C) 1999-2019 Alibaba Group Holding Limited
 */
package com.alibaba.innodb.java.reader;

import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 * Constants.
 *
 * @author xu.zx
 */
public interface Constants {

  String MAX_VAL = "max_val";

  String MIN_VAL = "min_val";

  String DEFAULT_JAVA_CHARSET = "UTF-8";

  String DEFAULT_MYSQL_CHARSET = "utf8";

  String DEFAULT_MYSQL_COLLATION = "utf8_general_ci";

  String COLUMN_ROW_ID = "_row_id_";

  List<Object> MAX_RECORD_1 = ImmutableList.of(MAX_VAL);

  List<Object> MAX_RECORD_2 = ImmutableList.of(MAX_VAL, MAX_VAL);

  List<Object> MAX_RECORD_3 = ImmutableList.of(MAX_VAL, MAX_VAL, MAX_VAL);

  List<Object> MAX_RECORD_4 = ImmutableList.of(MAX_VAL, MAX_VAL, MAX_VAL, MAX_VAL);

  List<Object> MAX_RECORD_5 = ImmutableList.of(MAX_VAL, MAX_VAL, MAX_VAL, MAX_VAL, MAX_VAL);

  List<Object> MIN_RECORD_1 = ImmutableList.of(MIN_VAL);

  List<Object> MIN_RECORD_2 = ImmutableList.of(MIN_VAL, MIN_VAL);

  List<Object> MIN_RECORD_3 = ImmutableList.of(MIN_VAL, MIN_VAL, MIN_VAL);

  List<Object> MIN_RECORD_4 = ImmutableList.of(MIN_VAL, MIN_VAL, MIN_VAL, MIN_VAL);

  List<Object> MIN_RECORD_5 = ImmutableList.of(MIN_VAL, MIN_VAL, MIN_VAL, MIN_VAL, MIN_VAL);

  /**
   * for mysql 5.6 and 5.7 the root page is usually page 3, but in mysql8 there introduces SDI page,
   * which may make root page number down from 4.
   * But the toolkit can be compatible with such case, if root page is SDI page then it will continue
   * search the file until the first index page found.
   */
  int ROOT_PAGE_NUMBER = 3;

  List<String> CONST_UNSIGNED = ImmutableList.of("UNSIGNED", "unsigned");

  int PRECISION_LIMIT = 5;

  String DEFAULT_DATA_FILE_SUFFIX = ".ibd";

  int INT_10000 = 10000;
  int INT_1000 = 1000;
  int INT_100 = 100;
  int INT_10 = 10;

  int MAX_PRECISION = 6;

  String COLUMN_ATTRIBUTE_ENUM_KEY = "_ENUM_";

  int MAX_ONE_BYTE_ENUM_COUNT = 255;

  String PRIMARY_KEY_NAME = "PRIMARY_KEY";

  String ZERO_DATETIME = "0000-00-00 00:00:00";

  byte[] BYTES_OF_INFIMUM = "infimum\0".getBytes();

  byte[] BYTES_OF_SUPREMUM = "supremum".getBytes();

}
