/*
 * Copyright (C) 1999-2019 Alibaba Group Holding Limited
 */
package com.alibaba.innodb.java.reader.page;

import com.google.common.collect.Maps;

import com.alibaba.innodb.java.reader.util.IdAble;

import java.util.Map;

/**
 * PageType
 * <p/>
 * Please refer to i_ss.cc or fil0fil.h
 *
 * @author xu.zx
 */
public enum PageType implements IdAble<Integer> {

  /** Below are some commonly use page types */
  ALLOCATED(0, "FIL_PAGE_TYPE_ALLOCATED"),
  INDEX(17855, "FIL_PAGE_INDEX"),
  UNDO_LOG(2, "FIL_PAGE_UNDO_LOG"),
  INODE(3, "FIL_PAGE_INODE"),
  IBUF_FREE_LIST(4, "FIL_PAGE_IBUF_FREE_LIST"),
  IBUF_BITMAP(5, "FIL_PAGE_IBUF_BITMAP"),
  SYSTEM(6, "FIL_PAGE_TYPE_SYS"),
  TRX_SYSTEM(7, "FIL_PAGE_TYPE_TRX_SYS"),
  FILE_SPACE_HEADER(8, "FIL_PAGE_TYPE_FSP_HDR"),
  EXTENT_DESCRIPTOR(9, "FIL_PAGE_TYPE_XDES"),
  BLOB(10, "FIL_PAGE_TYPE_BLOB"),
  COMPRESSED_BLOB(11, "FIL_PAGE_TYPE_ZBLOB"),
  COMPRESSED_BLOB2(12, "FIL_PAGE_TYPE_ZBLOB2"),
  UNKNOWN(13, "I_S_PAGE_TYPE_UNKNOWN"),
  RTREE_INDEX(17854, "I_S_PAGE_TYPE_RTREE"),
  /**
   * Serialized Dictionary Information since mysql 8
   */
  SDI(17853, "FIL_PAGE_SDI"),
  /**
   * LOB related since mysql 8
   */
  LOB_INDEX(22, "FIL_PAGE_TYPE_LOB_INDEX"),
  LOB_DATA(23, "FIL_PAGE_TYPE_LOB_INDEX"),
  LOB_FIRST(24, "FIL_PAGE_TYPE_LOB_INDEX");
  //IBUF_INDEX("I_S_PAGE_TYPE_IBUF");

  private int type;

  private String value;

  PageType(int type, String value) {
    this.type = type;
    this.value = value;
  }

  public int type() {
    return type;
  }

  public String value() {
    return value;
  }

  @Override
  public Integer id() {
    return type;
  }

  // ---------- template method ---------- //

  private static Map<Integer, PageType> KVS = Maps.newHashMapWithExpectedSize(values().length);

  static {
    for (PageType recordType : values()) {
      KVS.put(recordType.type(), recordType);
    }
  }

  public static PageType parse(int type) {
    return KVS.get(type);
  }
}
