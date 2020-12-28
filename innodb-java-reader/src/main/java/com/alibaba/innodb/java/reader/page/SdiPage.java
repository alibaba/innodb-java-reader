/*
 * Copyright (C) 1999-2019 Alibaba Group Holding Limited
 */
package com.alibaba.innodb.java.reader.page;

import com.alibaba.innodb.java.reader.exception.ReaderException;
import com.alibaba.innodb.java.reader.page.index.*;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import static com.alibaba.innodb.java.reader.Constants.BYTES_OF_INFIMUM;
import static com.alibaba.innodb.java.reader.Constants.BYTES_OF_SUPREMUM;
import static com.alibaba.innodb.java.reader.SizeOf.*;
import static com.alibaba.innodb.java.reader.SizeOf.SIZE_OF_PAGE_DIR_SLOT;
import static com.google.common.base.Preconditions.checkState;

/**
 * Since MySQL8.0, there is SDI, a.k.a Serialized Dictionary Information(SDI).
 *
 * @author xu.zx
 */
public class SdiPage extends AbstractPage {

  private IndexHeader indexHeader;

  private FsegHeader fsegHeader;

  private RecordHeader infimumHeader;

  private RecordHeader supremumHeader;

  private int[] dirSlots;

  private List<SdiRecord> sdiRecordList = new LinkedList<>();

  public SdiPage(InnerPage innerPage) {
    super(innerPage);
    this.indexHeader = IndexHeader.fromSlice(sliceInput);
    if (this.indexHeader.getFormat() != PageFormat.COMPACT) {
      throw new ReaderException("only support COMPACT format");
    }
    this.fsegHeader = FsegHeader.fromSlice(sliceInput);

    this.infimumHeader = RecordHeader.fromSlice(sliceInput);
    checkState(Arrays.equals(sliceInput.readByteArray(SIZE_OF_MUM_RECORD), BYTES_OF_INFIMUM));

    this.supremumHeader = RecordHeader.fromSlice(sliceInput);
    checkState(Arrays.equals(sliceInput.readByteArray(SIZE_OF_MUM_RECORD), BYTES_OF_SUPREMUM));

    int dirSlotNum = this.indexHeader.getNumOfDirSlots();
    dirSlots = new int[dirSlotNum];
    sliceInput.setPosition(SIZE_OF_PAGE - SIZE_OF_FIL_TRAILER - dirSlotNum * SIZE_OF_PAGE_DIR_SLOT);
    for (int i = 0; i < dirSlotNum; i++) {
      dirSlots[dirSlotNum - i - 1] = sliceInput.readUnsignedShort();
    }
    sliceInput.setPosition(dirSlots[0]);
    RecordHeader recordHeader = null;
    while (true) {
      sliceInput.decrPosition(SIZE_OF_REC_HEADER);
      recordHeader = RecordHeader.fromSlice(sliceInput);
      if (recordHeader.getRecordType() == RecordType.INFIMUM) {
        sliceInput.setPosition(sliceInput.position() + recordHeader.getNextRecOffset());
        continue;
      }
      if (recordHeader.getRecordType() == RecordType.SUPREMUM) {
        break;
      }
      int pos = sliceInput.position();
      SdiRecord record = new SdiRecord(sliceInput, recordHeader);
      this.sdiRecordList.add(record);
      sliceInput.setPosition(pos + recordHeader.getNextRecOffset());
    }
  }

}
