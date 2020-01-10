/*
 * Copyright (C) 1999-2019 Alibaba Group Holding Limited
 */
package com.alibaba.innodb.java.reader.page.index;

import com.alibaba.innodb.java.reader.page.RecordInfoFlag;
import com.alibaba.innodb.java.reader.util.SliceInput;

import lombok.Data;

/**
 * RecordHeader
 *
 * @author xu.zx
 */
@Data
public class RecordHeader {

  /**
   * fo Flags: A 4-bit bitmap to store boolean flags about this record.
   * Currently only two flags are defined: min_rec (1) meaning this record is the minimum record in a non-leaf level of the B+Tree,
   * and deleted (2) meaning the record is delete-marked (and will be actually deleted by a purge operation in the future).
   */
  private RecordInfoFlag infoFlag;

  /**
   * Number of Records Owned: The number of records “owned” by the current record in the page directory.
   * This field will be further discussed in a future post about the page directory.
   */
  private int numOfRecOwned;

  /**
   * Order: The order in which this record was inserted into the heap. Heap records (which include infimum and supremum)
   * are numbered from 0. Infimum is always order 0, supremum is always order 1.
   * User records inserted will be numbered from 2.
   */
  private short order;

  /**
   * The type of the record, where currently only 4 values are supported:
   * conventional (0), node pointer (1), infimum (2), and supremum (3).
   */
  private RecordType recordType;

  /**
   * Next Record Offset: A relative offset from the current record to the origin of the next record
   * within the page in ascending order by key.
   * <p/>
   * 直接定位到下一个record的数据部分，而不是header
   */
  private int nextRecOffset;

  public static RecordHeader fromSlice(SliceInput input) {
    RecordHeader record = new RecordHeader();

    // Fields packed in an 8-bit integer (LSB first):
    //  4 bits for n_owned
    //  4 bits for flags
    byte b1 = input.readByte();

    // For performance issue, we don't use EnumUtil find method
    record.setInfoFlag(RecordInfoFlag.parse((b1 & 0xf0) >> 4));
    record.setNumOfRecOwned(b1 & 0x0f);

    // Fields packed in a 16-bit integer (LSB first):
    // 3 bits for type
    // 13 bits for heap_number
    int b2 = input.readUnsignedShort();
    record.setRecordType(RecordType.parse((b2 & 0x07)));
    record.setOrder((short) ((b2 & 0xfff8) >> 3));

    record.setNextRecOffset(input.readShort());

    return record;
  }

}
