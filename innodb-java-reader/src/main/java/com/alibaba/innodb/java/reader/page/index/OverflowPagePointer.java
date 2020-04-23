/*
 * Copyright (C) 1999-2019 Alibaba Group Holding Limited
 */
package com.alibaba.innodb.java.reader.page.index;

import com.alibaba.innodb.java.reader.util.SliceInput;

import lombok.Data;

/**
 * https://mysqlserverteam.com/externally-stored-fields-in-innodb/
 * <p>
 * https://www.itread01.com/content/1546184536.html
 * <p>
 * When a BLOB field is stored externally, a BLOB reference is stored in the clustered index record.
 * The BLOB reference will be stored after the BLOB prefix, if any. This BLOB reference is 20 bytes,
 * and it contains the following information:
 * 當一個BLOB field被儲存在external pages時，會在index pages中儲存該BLOB field的BLOB reference(指示該BLOB的
 * size,external page位置等元資料資訊)。如果表的row format為COMPACT或者REDUNDANT，那麼BLOB reference儲存在
 * BLOB prefix之後。BLOB reference 佔用20 bytes空間，包含如下資訊：
 * <ul>
 * <li>The space identifier (4 bytes)    表空間資訊(下圖中的Space ID部分)</li>
 * <li>The page number where the first page of the BLOB is stored (4 bytes)
 * 儲存BLOB field的起始 page號(下圖中的Page Number部分)</li>
 * <li>The offset of the BLOB header within that page (4 bytes)
 * 起始page中 BLOB header 的位移量(下圖中的Offset)</li>
 * <li>The total size of the BLOB data (8 bytes)
 * 該BLOB 的 total size(除了上面三個以外的剩餘部分)</li>
 * </ul>
 * Even though 8 bytes are available to store the total size of the BLOB data, only the last 4 bytes
 * are actually used. This means that within InnoDB, the maximum size of a single BLOB field is
 * currently 4GB.
 * 儘管上面我們提到有8 bytes的空間可以用來儲存BLOB size資訊，但是實際上僅有 4 bytes空間可以真正用來儲存BLOB
 * size 資訊。這就意味著，innodb中可以儲存的最大 BLOB field size 是4 GB(4 bytes=32 bit ,2^32=4GB)
 *
 * @author xu.zx
 */
@Data
public class OverflowPagePointer {

  private long space;

  private long pageNumber;

  private long pageOffset;

  private long length;

  public static OverflowPagePointer fromSlice(SliceInput input) {
    OverflowPagePointer overflowPagePointer = new OverflowPagePointer();
    overflowPagePointer.setSpace(input.readUnsignedInt());
    overflowPagePointer.setPageNumber(input.readUnsignedInt());
    overflowPagePointer.setPageOffset(input.readUnsignedInt());
    input.skipBytes(4);
    overflowPagePointer.setLength(input.readUnsignedInt());
    return overflowPagePointer;
  }

}
