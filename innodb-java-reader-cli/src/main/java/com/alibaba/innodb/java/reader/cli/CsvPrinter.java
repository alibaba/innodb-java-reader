/*
 * Copyright 2020 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.innodb.java.reader.cli;

import java.io.IOException;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.QuoteMode;

/**
 * CSV printer.
 * 
 * @author xu.zx
 * @author Adam Jurcik
 */
public class CsvPrinter {

  public static final CSVFormat FIELD_FORMAT_QUOTED;

  static {
    // delimiter is not used
    FIELD_FORMAT_QUOTED = CSVFormat.newFormat(',')
        .withNullString("null")
        .withQuote('"')
        .withQuoteMode(QuoteMode.ALL_NON_NULL);
  }
  
  /**
   * Use {@link StringBuilder} to build string out of an array.
   * <p>
   * Sometimes by reusing StringBuilder, we can avoid creating many StringBuilder and good to garbage collection.
   *
   * @param a         array
   * @param b         reusable StringBuilder
   * @param delimiter delimiter
   * @param quote     whether to quote value
   * @param newLine   if this is a new line, if true, write slash n at the end
   * @return array string
   */
  public static String arrayToString(Object[] a, StringBuilder b, String delimiter, boolean quote, boolean newLine) {
    if (a == null) {
      return "null";
    }
    // clean StringBuilder
    b.delete(0, b.length());
    for (int i = 0; i < a.length; i++) {
      if (quote) {
        try {
          FIELD_FORMAT_QUOTED.print(a[i], b, true);
        } catch (IOException e) {
          // should not happen as StringBuilder does not throw IOException
          throw new IllegalStateException(e);
        }
      } else {
        b.append(a[i]);
      }
      b.append(delimiter);
    }
    if (b.length() > 0) {
      b.deleteCharAt(b.length() - 1);
    }
    if (newLine) {
      b.append("\n");
    }
    return b.toString();
  }

}
