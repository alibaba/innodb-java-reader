/*
 * Copyright (C) 1999-2019 Alibaba Group Holding Limited
 */
package com.alibaba.innodb.java.reader.page.index;

import com.alibaba.innodb.java.reader.util.SliceInput;

import lombok.Data;

/**
 * As described in Page management in InnoDB space files,
 * the index root page’s FSEG header contains pointers to the file segments
 * used by this index.
 * All other index pages’ FSEG headers are unused and zero-filled.
 * <p/>
 * 20 bytes header
 *
 * @author xu.zx
 */
@Data
public class FsegHeader {

  private long leafPagesInodeSpace;

  private long leafPagesInodePageNumber;

  private int leafPagesInodeOffset;

  private long nonLeafPagesInodeSpace;

  private long nonLeafPagesInodePageNumber;

  private int nonLeafPagesInodeOffset;

  public static FsegHeader fromSlice(SliceInput input) {
    FsegHeader fsegHeader = new FsegHeader();
    fsegHeader.setLeafPagesInodeSpace(input.readUnsignedInt());
    fsegHeader.setLeafPagesInodePageNumber(input.readUnsignedInt());
    fsegHeader.setLeafPagesInodeOffset(input.readUnsignedShort());
    fsegHeader.setNonLeafPagesInodeSpace(input.readUnsignedInt());
    fsegHeader.setNonLeafPagesInodePageNumber(input.readUnsignedInt());
    fsegHeader.setNonLeafPagesInodeOffset(input.readUnsignedShort());
    return fsegHeader;
  }

}
