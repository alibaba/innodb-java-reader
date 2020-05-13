/*
 * Copyright (C) 1999-2019 Alibaba Group Holding Limited
 */
package com.alibaba.innodb.java.reader;

/**
 * SizeOf.
 *
 * @author xu.zx
 */
public final class SizeOf {

  public static final int SIZE_OF_BYTE = 1;
  public static final int SIZE_OF_SHORT = 2;
  public static final int SIZE_OF_MEDIUMINT = 3;
  public static final int SIZE_OF_INT = 4;
  public static final int SIZE_OF_LONG = 8;

  /**
   * 16K, UNIV_PAGE_SIZE
   */
  public static final int SIZE_OF_PAGE = 16384;

  public static final int SIZE_OF_FIL_HEADER = 38;
  public static final int SIZE_OF_FIL_TRAILER = 8;

  public static final int SIZE_OF_BODY = SIZE_OF_PAGE - SIZE_OF_FIL_HEADER - SIZE_OF_FIL_TRAILER;

  /**
   * RECORD_COMPACT_BITS_SIZE(3 bytes) + RECORD_NEXT_SIZE(2 bytes)
   */
  public static final int SIZE_OF_REC_HEADER = 5;

  /**
   * system record是一种特殊的记录，content是字符串infimum\0, supremum, 占用8个bytes
   */
  public static final int SIZE_OF_MUM_RECORD = 8;

  /**
   * system record bytes = 5(record header) + 8("supremum" length)
   */
  public static final int SIZE_OF_SYSTEM_RECORD = SIZE_OF_REC_HEADER + SIZE_OF_MUM_RECORD;

  /**
   * The size (in bytes) of the record pointers in each page directory slot.
   */
  public static final int SIZE_OF_PAGE_DIR_SLOT = 2;

  /**
   * The minimum number of records "owned" by each record with an entry in the page directory.
   */
  public static final int PAGE_DIR_SLOT_MIN_N_OWNED = 4;

  /**
   * The maximum number of records "owned" by each record with an entry in the page directory.
   */
  public static final int PAGE_DIR_SLOT_MAX_N_OWNED = 8;

  private SizeOf() {
  }

}
