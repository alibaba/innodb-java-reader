package com.alibaba.innodb.java.reader.page.index;

import com.alibaba.innodb.java.reader.util.SliceInput;
import com.alibaba.innodb.java.reader.util.ZlibUtil;
import lombok.Data;

import java.nio.charset.StandardCharsets;

import static com.alibaba.innodb.java.reader.SizeOf.SIZE_OF_REC_HEADER;

/**
 * @author jie xu
 * @description
 * @created 2020/12/21
 **/
@Data
public class SdiRecord {

  private int contentLength;

  private RecordHeader recordHeader;

  private int sdiType;

  private long sdiId;

  private long sdiTransId;

  private int unzipLength;

  private int zipLength;

  private String content;

  public SdiRecord(SliceInput sliceInput, RecordHeader recordHeader) {
    this.recordHeader = recordHeader;
    int position = sliceInput.position();
    this.contentLength = parseContentLength(sliceInput);
    sliceInput.setPosition(position);
    this.sdiType = sliceInput.readInt();
    this.sdiId = sliceInput.readLong();
    this.sdiTransId = sliceInput.read6BytesInt();
    //skip rollBackRef
    sliceInput.setPosition(sliceInput.position() + 7);
    this.unzipLength = sliceInput.readInt();
    this.zipLength = sliceInput.readInt();
    if (this.contentLength != zipLength && this.contentLength != unzipLength) {
      throw new RuntimeException("sdi record parse error!");
    }
    byte[] buffer = new byte[this.contentLength];
    sliceInput.readBytes(buffer);
    if (this.contentLength == zipLength) {
      buffer = ZlibUtil.decompress(buffer);
    }
    this.content = new String(buffer, 0, buffer.length, StandardCharsets.US_ASCII);
    sliceInput.setPosition(position);
  }

  private int parseContentLength(SliceInput sliceInput) {
    int resu = 0;
    sliceInput.decrPosition(SIZE_OF_REC_HEADER + 1);
    int first = sliceInput.readUnsignedByte();
    if ((first & 0x80) != 0) {
      resu = (first & 0x3f) << 8;
      if ((first & 0x40) != 0x00) {
        //todo big data length, read from overflowPage (BLOB page?)
        throw new RuntimeException("unsupported record length");
      } else {
        sliceInput.decrPosition(2);
        int second = sliceInput.readUnsignedByte();
        sliceInput.readUnsignedByte();
        resu += second;
      }
    } else {
      resu = first;
    }
    return resu;
  }

}
