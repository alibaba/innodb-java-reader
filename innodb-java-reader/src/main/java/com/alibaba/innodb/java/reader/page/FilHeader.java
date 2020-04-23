/*
 * Copyright (C) 1999-2019 Alibaba Group Holding Limited
 */
package com.alibaba.innodb.java.reader.page;

import com.alibaba.innodb.java.reader.util.SliceInput;

import javax.annotation.Nullable;

import lombok.Data;

import static com.alibaba.innodb.java.reader.util.Utils.maybeUndefined;

/**
 * 38 bytes page header.
 *
 * @author xu.zx
 */
@Data
public class FilHeader {

  private long checksum;

  private long pageNumber;

  /**
   * Pointers to the logical previous and next page for this page type are stored in the header.
   * This allows doubly-linked lists of pages to be built, and this is used for INDEX pages to
   * link all pages at the same level, which allows for e.g. full index scans to be efficient.
   * Many page types do not use these fields.
   */
  @Nullable
  private Long prevPage;

  @Nullable
  private Long nextPage;

  private long lastModifiedLsn;

  private PageType pageType;

  private long flushLsn;

  private long spaceId;

  public static FilHeader fromSlice(SliceInput input) {
    FilHeader filHeader = new FilHeader();
    filHeader.setChecksum(input.readUnsignedInt());
    filHeader.setPageNumber(input.readUnsignedInt());
    filHeader.setPrevPage(maybeUndefined(input.readUnsignedInt()));
    filHeader.setNextPage(maybeUndefined(input.readUnsignedInt()));
    filHeader.setLastModifiedLsn(input.readLong());
    filHeader.setPageType(PageType.parse(input.readShort()));
    filHeader.setFlushLsn(input.readLong());
    filHeader.setSpaceId(input.readUnsignedInt());
    return filHeader;
  }

  public long getLow32Lsn() {
    return lastModifiedLsn & 0xffffffffL;
  }

}
