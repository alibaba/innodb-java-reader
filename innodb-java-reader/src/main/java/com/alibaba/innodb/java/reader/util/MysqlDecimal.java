/**
 * Apache License Version 2.0.
 *
 * Copy and refine from https://github.com/rolandhe/hiriver
 */
package com.alibaba.innodb.java.reader.util;

import java.math.BigDecimal;

/**
 * 用于描述、解析mysql binlog格式的decimal类型数据。该逻辑来自mysql源码中的 decimal.c, decimal2bin and bin2decimal. <br>
 * <p>
 * decimal 是用来描述十进制大实数值的，包括整数和小数部分，它有 precision和scale两个属性，precision表示该数值最大位数（包括小数，但不包括小数点），
 * scale表示小数位数，比如，1234.23， 它的precision=6，scale=2，因此它的整数部分的长度是4.
 * decimal 在计算机内使用变长字节存储，其字节长度会超过8个，根据decimal的数值增大而变长。decimal是十进制的，在它被转化为二进制的过程中
 * 也是使用十进制计算的。
 * decimal分为小数部分和整数部分，根据precision和scale可以计算出其二进制字节数据，具体参见构造方法。在转化过程中无论是整数部分还是小数部分
 * 都是先按照十进制的位数进行分段，每9位分为一段，每段使用一个int描述，整数部分从低位开始分段，小数部分从高位开始分段，最后一段（简称尾段）可能不足9位。
 * 9位段占满4字节，而尾段根据其位数的不同而占的字节数不同，具体参见{@link #DIG2BYTES}, 位数即数组下标。比如：<br>
 * 713234568987.0345678992, 被划分成 713 234568987 034567899 2， 713 是整数部分的尾段，共3位，占2个字节， 2是小数部分的尾段，
 * 234568987 是整数部分中9位段，占4个字节，注意都是<b>大尾端</b>，034567899是小数部分的9位段，也占4个字节，整个二进制按照 713，234568987，
 * 034567899，2顺序从低位向高位摆放。
 * 符号表示：
 * 如果是正数，二进制数组的第0个元素^0x80
 * 如果是负数，绝对值转化为二进制后挨个取反，再第0个元素^0x80
 * </p>
 *
 * Created by hexiufeng on 2017/5/11.
 *
 * @author hexiufeng
 */
public class MysqlDecimal {

  /**
   * 负号
   */
  private static final char NEGATIVE_SIGN = '-';

  /**
   * 小数点
   */
  private static final char DECIMAL_POINT = '.';

  /**
   * 字符'0'所对应的Ascii码值，用于转化0-9数值到'0'-'9'字符
   */
  private static final int ZERO_ASCII = 48;

  /**
   * int 数据类型所占字节数
   */
  private static final int SIZE_OF_INT32 = 4;

  /**
   * decimal 按位分段基数
   */
  private static final int DIG_PER_DEC1 = 9;

  /**
   * 分段后，不足一段的部分转化为binary时所应该占的字节数据, 不足一段的长度作为下标，<br>
   * 比如：1234872356870.12, 整数部分分为1234 872356870, 1234就是不足一段的部分,长度<br>
   * 是4，那么它所占的字节数是DIG2BYTES[4]=2
   */
  private static final int[] DIG2BYTES = {0, 1, 1, 2, 2, 3, 3, 4, 4, 4};

  /**
   * decimal使用内部使用int[] 来存储，每个int即调拨一个段，每个段为1-9位，POWERS10用于描述不同
   * 的位数所能表达的上限值，注意是开区间
   */
  private static final int[] POWERS10 = {1, 10, 100, 1000, 10000, 100000, 1000000, 10000000, 100000000, 1000000000};

  /**
   * decimal整数部分的十进制长度
   */
  private final int intLength;

  /**
   * decimal小数部分的十进制长度
   */
  private final int fracLength;

  /**
   * decimal 整数部分的分段数, 每一段的长度是 {@link DIG_PER_DEC1}
   */
  private final int intDecSize;

  /**
   * decimal 小数部分的分段数, 每一段的长度是 {@link DIG_PER_DEC1}
   */
  private final int fracDecSize;

  /**
   * decimal 整数部分不足一段十进制长度
   */
  private final int intNoneDecLength;

  /**
   * decimal 小数数部分不足一段十进制长度
   */
  private final int fracNoneDecLength;

  /**
   * 指定精度和小数位数的decimal转化成binary后的长度
   */
  private final int binSize;

  /**
   * 整数部分用数组表示
   */
  private final int[] intArray;

  /**
   * 小数部分用数组表示
   */
  private final int[] fracArray;

  /**
   * 符号，false 表示正数
   */
  private boolean sign;

  /**
   * 根据指定的精度和小数位数生成decimal的描述信息
   *
   * @param precision 精度
   * @param scale     小数位数
   */
  public MysqlDecimal(int precision, int scale) {
    intLength = precision - scale;
    fracLength = scale;
    intDecSize = intLength / DIG_PER_DEC1;
    fracDecSize = fracLength / DIG_PER_DEC1;
    intNoneDecLength = intLength % DIG_PER_DEC1;
    fracNoneDecLength = fracLength % DIG_PER_DEC1;
    binSize = intDecSize * SIZE_OF_INT32 + fracDecSize * SIZE_OF_INT32 + DIG2BYTES[intNoneDecLength]
        + DIG2BYTES[fracNoneDecLength];
    intArray = new int[(intLength + DIG_PER_DEC1 - 1) / DIG_PER_DEC1];
    fracArray = new int[(fracLength + DIG_PER_DEC1 - 1) / DIG_PER_DEC1];
  }

  /**
   * 由二进制数值解析成decimal的内部存储
   *
   * @param buf 二进制值
   */
  public void parse(byte[] buf) {
    Position pos = Position.factory();
    sign = (buf[0] & 0x80) == 0;
    buf[0] ^= 0x80;

    if (sign) {
      for (int i = 0; i < buf.length; i++) {
        // 哈哈，取反实现
        buf[i] ^= 0xff;
      }
    }
    parseIntSection(buf, pos);

    parseFracSection(buf, pos);

  }

  /**
   * 解析小数部分
   */
  private void parseFracSection(byte[] buf, Position pos) {
    for (int i = 0; i < fracDecSize; i++) {
      fracArray[i] = readInt(buf, pos, SIZE_OF_INT32);
      checkIntValue(fracArray[i], DIG_PER_DEC1);
    }
    if (fracNoneDecLength > 0) {
      fracArray[fracArray.length - 1] = readInt(buf, pos, DIG2BYTES[fracNoneDecLength]);
      checkIntValue(fracArray[fracArray.length - 1], fracNoneDecLength);
    }
  }

  /**
   * 解析整数部分
   */
  private void parseIntSection(byte[] buf, Position pos) {
    int startIndex = 0;
    if (intNoneDecLength > 0) {
      intArray[0] = readInt(buf, pos, DIG2BYTES[intNoneDecLength]);
      checkIntValue(intArray[0], intNoneDecLength);
      startIndex++;
    }
    for (int i = 0; i < intDecSize; i++) {
      intArray[startIndex + i] = readInt(buf, pos, SIZE_OF_INT32);
      checkIntValue(intArray[startIndex + i], DIG_PER_DEC1);
    }
  }

  /**
   * 转化成BigDecimal
   */
  public BigDecimal toDecimal() {
    SimpleStringBuilder sb = new SimpleStringBuilder(calDecimalStringLength());
    if (sign) {
      sb.append(NEGATIVE_SIGN);
    }
    convertIntSection2CharArray(sb);
    convertFracSection2CharArray(sb);
    return new BigDecimal(sb.toCharArray());
  }

  /**
   * 转化小数部分为字符数组
   */
  private void convertFracSection2CharArray(SimpleStringBuilder sb) {
    if (fracLength > 0) {
      sb.append(DECIMAL_POINT);
    }
    for (int i = 0; i < fracDecSize; i++) {
      convertInt2Char(fracArray[i], DIG_PER_DEC1, sb);
    }
    if (fracNoneDecLength > 0) {
      convertInt2Char(fracArray[fracArray.length - 1], fracNoneDecLength, sb);
    }
  }

  /**
   * 转化整数部分为字符数组
   */
  private void convertIntSection2CharArray(SimpleStringBuilder sb) {
    int startIndex = 0;
    if (intNoneDecLength > 0) {
      convertInt2Char(intArray[0], intNoneDecLength, sb);
      startIndex++;
    }
    for (int i = 0; i < intDecSize; i++) {
      convertInt2Char(intArray[startIndex + i], DIG_PER_DEC1, sb);
    }
  }

  public int getBinSize() {
    return this.binSize;
  }

  /**
   * 计算decimal转化成字符数组的长度
   */
  private int calDecimalStringLength() {
    int len = intLength;
    // 符号位
    if (sign) {
      len++;
    }
    // 如果有小数，需要小数点
    if (fracLength > 0) {
      len += fracLength + 1;
    }
    return len;
  }

  /**
   * 把一个整数转化成指定长度的char数组，如果整数的总位数小于指定的长度，在高位用0补齐
   */
  private void convertInt2Char(int value, int digitLen, final SimpleStringBuilder sb) {
    for (int i = digitLen - 1; i >= 0; i--) {
      int digit = value / POWERS10[i];
      // 使用算术运算完成字符数字向字符的转化，提高性能
      digit += ZERO_ASCII;
      sb.append((char) digit);
      value = value % POWERS10[i];
    }
  }

  /**
   * 根据指定的字节数读取int值，
   *
   * @param buf      大尾端描述
   * @param byteSize 必须是 1、2、3、4中的一个
   */
  private int readInt(final byte[] buf, final Position pos, final int byteSize) {
    switch (byteSize) {
      case 1:
        return read1Int(buf, pos);
      case 2:
        return read2BEInt(buf, pos);
      case 3:
        return read3BEInt(buf, pos);
      case 4:
        return read4BEInt(buf, pos);
      default:
        throw new RuntimeException("read int value error: int must be 1,2,3,4 bytes.");
    }
  }

  /**
   * 检查用于描述decimal的int[]中的每个数值是有效的
   */
  private void checkIntValue(int value, int digitLen) {
    if (value >= POWERS10[digitLen]) {
      throw new RuntimeException("invalid decimal int value:" + value);
    }
  }

  /**
   * 简单的用于拼接字符的容器，指定固定的长度，顺序拼接，
   * 一次初始化内部存储，不动态扩容，也不复制.
   *
   * 直接使用char而不是string，减少转化，提高性能
   */
  private static class SimpleStringBuilder {
    private final char[] buf;
    private int position;

    SimpleStringBuilder(int capcity) {
      buf = new char[capcity];
    }

    void append(char c) {
      buf[position++] = c;
    }

    char[] toCharArray() {
      return buf;
    }
  }

  private int read1Int(byte[] bytes, Position position) {
    return bytes[position.getAndForwordPos()];
  }

  private int read2BEInt(byte[] bytes, Position position) {
    return (bytes[position.getAndForwordPos()] & 0xFF) << 8 | (bytes[position.getAndForwordPos()] & 0xFF);
  }

  private int read3BEInt(byte[] bytes, Position position) {
    return (bytes[position.getAndForwordPos()] & 0xFF) << 16
        | (bytes[position.getAndForwordPos()] & 0xFF) << 8
        | (bytes[position.getAndForwordPos()] & 0xFF);
  }

  private int read4BEInt(byte[] bytes, Position position) {
    return (bytes[position.getAndForwordPos()] & 0xFF) << 24
        | (bytes[position.getAndForwordPos()] & 0xFF) << 16
        | (bytes[position.getAndForwordPos()] & 0xFF) << 8
        | (bytes[position.getAndForwordPos()] & 0xFF);
  }

}
