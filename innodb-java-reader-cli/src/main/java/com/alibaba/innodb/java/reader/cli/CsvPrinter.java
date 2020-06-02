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

import com.google.common.collect.Maps;
import java.io.IOException;
import java.util.Map;
import org.apache.commons.csv.CSVFormat;

/**
 * CSV printer.
 *
 * @author xu.zx
 * @author Adam Jurcik
 */
public class CsvPrinter {

  /**
   * Field delimiter.
   */
  private String delimiter;

  /**
   * Quote mode.
   */
  private QuoteMode quoteMode;

  /**
   * Null value string.
   */
  private String nullStr;

  /**
   * Field value formatting facility.
   */
  private CSVFormat valueFormat;

  /**
   * Row printing target.
   */
  private StringBuilder row = new StringBuilder();

  /**
   * App quote modes mapping to CSV lib quote modes.
   */
  private static final Map<QuoteMode, org.apache.commons.csv.QuoteMode> QUOTE_MODES
      = Maps.newHashMapWithExpectedSize(QuoteMode.values().length);

  static {
    QUOTE_MODES.put(QuoteMode.ALL, org.apache.commons.csv.QuoteMode.ALL);
    QUOTE_MODES.put(QuoteMode.NON_NULL, org.apache.commons.csv.QuoteMode.ALL_NON_NULL);
    QUOTE_MODES.put(QuoteMode.NON_NUMERIC, org.apache.commons.csv.QuoteMode.NON_NUMERIC);
  }

  /**
   * Creates a CSV printer.
   *
   * @param quoteMode field value quotation mode
   * @param nullStr   null string
   */
  public CsvPrinter(String delimiter, QuoteMode quoteMode, String nullStr) {
    this.delimiter = delimiter;
    this.quoteMode = quoteMode;
    this.nullStr = nullStr;

    // delimiter is not used as the lib enables to use only one character
    valueFormat = CSVFormat.newFormat(',')
        .withNullString(nullStr)
        .withQuote('"');

    if (quoteMode != QuoteMode.NONE) {
      valueFormat = valueFormat.withQuoteMode(QUOTE_MODES.get(quoteMode));
    }
  }

  /**
   * Prints array of table fields to CSV.
   *
   * @param a         array
   * @param newLine   if this is a new line, if true, write slash n at the end
   * @return array string
   */
  public String arrayToString(Object[] a, boolean newLine) {
    if (a == null) {
      return "null";
    }
    // clean StringBuilder
    row.setLength(0);
    for (Object value : a) {
      if (quoteMode != QuoteMode.NONE) {
        try {
          valueFormat.print(value, row, true);
        } catch (IOException e) {
          // should not happen as StringBuilder does not throw IOException
          throw new IllegalStateException(e);
        }
      } else if (value == null) {
          row.append(nullStr);
      } else {
          row.append(value);
      }
      row.append(delimiter);
    }
    if (row.length() > 0) {
      row.deleteCharAt(row.length() - 1);
    }
    if (newLine) {
      row.append("\n");
    }
    return row.toString();
  }

}
