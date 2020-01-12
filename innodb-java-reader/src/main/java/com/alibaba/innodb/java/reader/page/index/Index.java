/*
 * Copyright (C) 1999-2019 Alibaba Group Holding Limited
 */
package com.alibaba.innodb.java.reader.page.index;

import com.alibaba.innodb.java.reader.page.AbstractPage;
import com.alibaba.innodb.java.reader.page.InnerPage;
import com.alibaba.innodb.java.reader.schema.Schema;

import lombok.Data;

import static com.alibaba.innodb.java.reader.SizeOf.SIZE_OF_FIL_TRAILER;
import static com.alibaba.innodb.java.reader.SizeOf.SIZE_OF_MUM_RECORD;
import static com.alibaba.innodb.java.reader.SizeOf.SIZE_OF_PAGE;
import static com.alibaba.innodb.java.reader.SizeOf.SIZE_OF_PAGE_DIR_SLOT;
import static com.google.common.base.Preconditions.checkState;

/**
 * Index page
 *
 * @author xu.zx
 */
@Data
public class Index extends AbstractPage {

  private IndexHeader indexHeader;

  private FsegHeader fsegHeader;

  private GenericRecord infimum;

  private GenericRecord supremum;

  private int[] dirSlots;

  public Index(InnerPage innerPage, Schema schema) {
    super(innerPage);

    // 36 bytes index header
    this.indexHeader = IndexHeader.fromSlice(sliceInput);
    checkState(this.indexHeader.getFormat() == PageFormat.COMPACT, "only compact page supported");

    // 20 bytes fseg header
    this.fsegHeader = FsegHeader.fromSlice(sliceInput);

    // 5 (record header) + 8 ("infimum0") = 13 bytes
    // end offset = 38(file header) + 56 + 13 = 107
    // infimum content page offset = 107 - 8 = 99
    RecordHeader infimumHeader = RecordHeader.fromSlice(sliceInput);
    this.infimum = new GenericRecord(infimumHeader, schema, innerPage.getPageNumber());
    this.infimum.setPrimaryKeyPosition(sliceInput.position());
    String infimumString = sliceInput.readUTF8String(SIZE_OF_MUM_RECORD);
    checkState("infimum\0".equals(infimumString));

    // 5 (record header) + 8 ("supremum") = 13 bytes
    // end offset = 107 + 13 = 120
    // supremum content page offset = 120 - 8 = 112
    RecordHeader supremumHeader = RecordHeader.fromSlice(sliceInput);
    this.supremum = new GenericRecord(supremumHeader, schema, innerPage.getPageNumber());
    this.supremum.setPrimaryKeyPosition(sliceInput.position());
    String supremumString = sliceInput.readUTF8String(SIZE_OF_MUM_RECORD);
    checkState("supremum".equals(supremumString));

    // read page directory
    // reverse order from trailer, 2 bytes of relative offset of one record
    int endOfSupremum = sliceInput.position();
    int dirSlotNum = this.indexHeader.getNumOfDirSlots();
    dirSlots = new int[dirSlotNum];
    sliceInput.setPosition(SIZE_OF_PAGE - SIZE_OF_FIL_TRAILER - dirSlotNum * SIZE_OF_PAGE_DIR_SLOT);
    for (int i = 0; i < dirSlotNum; i++) {
      dirSlots[dirSlotNum - i - 1] = sliceInput.readUnsignedShort();
    }
    sliceInput.setPosition(endOfSupremum);
  }

  public boolean isLeafPage() {
    return indexHeader.getPageLevel() == 0;
  }

  public boolean isRootPage() {
    return innerPage.getFilHeader().getPrevPage() == null && innerPage.getFilHeader().getNextPage() == null;
  }

  /**
   * Get used bytes in a page. For example, given a page like below, the used bytes is
   * <code>1000 + 376 + 8 - 240 = 1144</code>
   * <pre>
   *      0 +----------------------+ <- page offset 0
   *        |      FilHeader       |
   *     38 +----------------------+
   *    138 |        row 1         |
   *    400 |        row 2         |
   *    660 | row 3 (mark deleted) |  <- Deleted record will be counted as free space
   *    800 |        row 4         |
   *    920 |        row 5         |  <- Heap top position. The byte offset of the "end" of the currently used space
   *   1000 +----------------------+
   *        |                      |
   *        |                      |  <- Free space
   *  16000 +----------------------+
   *        |   Directory slots    |
   *  16376 +----------------------+
   *        |      FilTrailer      |
   *  16384 +----------------------+ <- page offset SIZE_OF_PAGE, usually 16KiB
   * </pre>
   *
   * @return used bytes
   */
  public int usedBytesInIndexPage() {
    return indexHeader.getHeapTopPosition() + SIZE_OF_FIL_TRAILER
        + this.indexHeader.getNumOfDirSlots() * SIZE_OF_PAGE_DIR_SLOT - indexHeader.getGarbageSpace();
  }

}
