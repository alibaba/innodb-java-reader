/*
 * Copyright (C) 1999-2019 Alibaba Group Holding Limited
 */
package com.alibaba.innodb.java.reader.page;

import com.alibaba.innodb.java.reader.util.SliceInput;

import lombok.Data;

/**
 * 8 bytes page trailer
 *
 * @author xu.zx
 */
@Data
public class FilTrailer {

  private long checksum;

  private long low32lsn;

  public static FilTrailer fromSlice(SliceInput input) {
    FilTrailer filTrailer = new FilTrailer();
    filTrailer.setChecksum(input.readUnsignedInt());
    filTrailer.setLow32lsn(input.readUnsignedInt());
    return filTrailer;
  }

}
