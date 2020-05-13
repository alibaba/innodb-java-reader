/*
 * Copyright (C) 1999-2019 Alibaba Group Holding Limited
 */
package com.alibaba.innodb.java.reader.schema;

import com.google.common.collect.ImmutableList;

import com.alibaba.innodb.java.reader.CharsetMapping;
import com.alibaba.innodb.java.reader.exception.SqlParseException;
import com.alibaba.innodb.java.reader.util.Symbol;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.extern.slf4j.Slf4j;

import static com.alibaba.innodb.java.reader.Constants.DEFAULT_JAVA_CHARSET;
import static com.alibaba.innodb.java.reader.Constants.DEFAULT_MYSQL_CHARSET;
import static com.alibaba.innodb.java.reader.column.ColumnType.CHAR;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

/**
 * Table definition, like the result of the sql command: <code>SHOW CREATE TABLE LIKE 'TTT'</code>.
 *
 * @author xu.zx
 */
@EqualsAndHashCode
@Slf4j
public class TableDef {

  /**
   * Full qualified name, like <tt>db.t1</tt>. If not set, it is the same
   * as {@link #name}.
   */
  private String fullQualifiedName;

  private String name;

  private List<String> columnNames;

  private List<Column> columnList;

  private Map<String, Field> nameToFieldMap;

  private KeyMeta primaryKeyMeta;

  private List<KeyMeta> secondaryKeyMetaList;

  private int nullableColumnNum = 0;

  private int variableLengthColumnNum = 0;

  private List<Column> nullableColumnList;

  private List<Column> variableLengthColumnList;

  /**
   * Column ordinal.
   */
  private int ordinal = 0;

  /**
   * Default charset for decoding string in Java. Derived from table DDL default charset
   * according to {@link CharsetMapping}.
   */
  private String defaultJavaCharset = DEFAULT_JAVA_CHARSET;

  /**
   * Table DDL default charset, for example can be latin, utf8, utf8mb4.
   */
  private String defaultCharset = DEFAULT_MYSQL_CHARSET;

  /**
   * //TODO make sure this is the right way to implement
   * For example, if table charset set to utf8, then it will consume up to 3 bytes for one character.
   * if it is utf8mb4, then it must be set to 4.
   */
  private int maxBytesPerChar = 1;

  public TableDef() {
    this.columnList = new ArrayList<>();
    this.columnNames = new ArrayList<>();
    this.nameToFieldMap = new HashMap<>();
    this.nullableColumnList = new ArrayList<>();
    this.variableLengthColumnList = new ArrayList<>();
  }

  public void validate() {
    checkState(CollectionUtils.isNotEmpty(columnList), "No column is specified");
  }

  public boolean containsVariableLengthColumn() {
    return variableLengthColumnNum > 0;
  }

  public boolean containsNullColumn() {
    return nullableColumnNum > 0;
  }

  public List<Column> getColumnList() {
    return columnList;
  }

  public List<String> getColumnNames() {
    return columnNames;
  }

  public int getColumnNum() {
    return columnNames.size();
  }

  public int getNullableColumnNum() {
    return nullableColumnNum;
  }

  public int getVariableLengthColumnNum() {
    return variableLengthColumnNum;
  }

  public TableDef addColumn(Column column) {
    checkNotNull(column, "Column should not be null");
    checkArgument(StringUtils.isNotEmpty(column.getName()), "Column name is empty");
    checkArgument(StringUtils.isNotEmpty(column.getType()), "Column type is empty");
    checkArgument(!nameToFieldMap.containsKey(column.getName()), "Duplicate column name");
    if (column.isNullable()) {
      nullableColumnList.add(column);
      nullableColumnNum++;
    }
    if (column.isVariableLength()) {
      variableLengthColumnList.add(column);
      variableLengthColumnNum++;
    } else if (CHAR.equals(column.getType()) && maxBytesPerChar > 1) {
      // 多字符集则设置为varchar的读取方式
      column.setVarLenChar(true);
      variableLengthColumnList.add(column);
      variableLengthColumnNum++;
    }
    column.setOrdinal(ordinal++);
    column.setTableDef(this);
    columnList.add(column);
    columnNames.add(column.getName());
    nameToFieldMap.put(column.getName(), new Field(column.getOrdinal(), column.getName(), column));
    if (column.isPrimaryKey()) {
      checkState(primaryKeyMeta == null, "Primary key is already defined");
      primaryKeyMeta = createKeyMetaInfo(KeyMeta.Type.PRIMARY_KEY.literal(),
          KeyMeta.Type.PRIMARY_KEY.literal(),
          ImmutableList.of(column.getName()));
    }
    return this;
  }

  public Field getField(String columnName) {
    return nameToFieldMap.get(columnName);
  }

  public List<Column> getPrimaryKeyColumns() {
    return primaryKeyMeta == null ? ImmutableList.of() : primaryKeyMeta.getKeyColumns();
  }

  public List<String> getPrimaryKeyColumnNames() {
    return primaryKeyMeta == null ? ImmutableList.of() : primaryKeyMeta.getKeyColumnNames();
  }

  public int getPrimaryKeyColumnNum() {
    return primaryKeyMeta == null ? 0 : primaryKeyMeta.getNumOfColumns();
  }

  public List<Column> getPrimaryKeyVarLenColumns() {
    return primaryKeyMeta == null ? ImmutableList.of() : primaryKeyMeta.getKeyVarLenColumns();
  }

  public List<String> getPrimaryKeyVarLenColumnNames() {
    return primaryKeyMeta == null ? ImmutableList.of() : primaryKeyMeta.getKeyVarLenColumnNames();
  }

  public int getPrimaryKeyVarLenColumnNum() {
    return primaryKeyMeta == null ? 0 : primaryKeyMeta.getKeyVarLenColumns().size();
  }

  public boolean isNoPrimaryKey() {
    return primaryKeyMeta == null || CollectionUtils.isEmpty(primaryKeyMeta.getKeyColumns());
  }

  public boolean isColumnPrimaryKey(Column column) {
    return primaryKeyMeta != null && primaryKeyMeta.containsColumn(column.getName());
  }

  public List<KeyMeta> getSecondaryKeyMetaList() {
    return secondaryKeyMetaList;
  }

  public TableDef setPrimaryKeyColumns(List<String> pkColumnNames) {
    checkState(primaryKeyMeta == null, "Primary key is already defined in column");
    primaryKeyMeta = createKeyMetaInfo(KeyMeta.Type.PRIMARY_KEY.literal(),
        KeyMeta.Type.PRIMARY_KEY.literal(), pkColumnNames);
    return this;
  }

  public TableDef addSecondaryKeyColumns(String type, String keyName, List<String> columnNames) {
    if (secondaryKeyMetaList == null) {
      secondaryKeyMetaList = new ArrayList<>();
    }
    KeyMeta secondaryKeyMeta = createKeyMetaInfo(type, keyName, columnNames);
    secondaryKeyMetaList.add(secondaryKeyMeta);
    return this;
  }

  /**
   * Create key metadata.
   *
   * @param type           key type like key, index, unique key or primary key
   * @param keyName        key name
   *                       for secondary key, the value is arbitrary
   * @param keyColumnNames key column names
   * @return key metadata
   */
  public KeyMeta createKeyMetaInfo(String type, String keyName, List<String> keyColumnNames) {
    checkArgument(CollectionUtils.isNotEmpty(keyColumnNames),
        "Key column names is empty for key = " + keyName);

    ImmutableList.Builder<Column> cols = ImmutableList.builder();
    ImmutableList.Builder<String> colNames = ImmutableList.builder();
    ImmutableList.Builder<Column> varLenCols = ImmutableList.builder();
    ImmutableList.Builder<String> varLenColNames = ImmutableList.builder();
    for (String colName : keyColumnNames) {
      String columnName = colName.replace(Symbol.BACKTICK, Symbol.EMPTY)
          .replace(Symbol.DOUBLE_QUOTE, Symbol.EMPTY);
      if (containsColumn(columnName)) {
        Column col = getField(columnName).getColumn();
        cols.add(col);
        colNames.add(columnName);

        if (isVarLen(col)) {
          varLenCols.add(col);
          varLenColNames.add(columnName);
        }
      } else {
        throw new SqlParseException("Column " + columnName + " is not defined for key " + keyName);
      }
    }

    return KeyMeta.builder()
        .keyColumns(cols.build())
        .keyColumnNames(colNames.build())
        .keyVarLenColumns(varLenCols.build())
        .keyVarLenColumnNames(varLenColNames.build())
        .numOfColumns(keyColumnNames.size())
        .type(KeyMeta.Type.parse(type))
        .name(keyName != null ? keyName.replace(Symbol.BACKTICK, Symbol.EMPTY)
            .replace(Symbol.DOUBLE_QUOTE, Symbol.EMPTY) : null)
        .build();
  }

  private boolean isVarLen(Column pk) {
    return pk.isVariableLength()
        || (CHAR.equals(pk.getType()) && maxBytesPerChar > 1);
  }

  public List<Column> getVariableLengthColumnList() {
    return variableLengthColumnList;
  }

  public List<Column> getNullableColumnList() {
    return nullableColumnList;
  }

  public String getDefaultJavaCharset() {
    return defaultJavaCharset;
  }

  public String getDefaultCharset() {
    return defaultCharset;
  }

  public TableDef setDefaultCharset(String defaultCharset) {
    checkArgument(CollectionUtils.isEmpty(columnList), "Default charset should be set before adding columns");
    this.defaultCharset = defaultCharset;
    this.defaultJavaCharset = CharsetMapping.getJavaCharsetForMysqlCharset(defaultCharset);
    this.maxBytesPerChar = CharsetMapping.getMaxByteLengthForMysqlCharset(defaultCharset);
    return this;
  }

  public String getName() {
    return name;
  }

  public String getFullyQualifiedName() {
    return fullQualifiedName;
  }

  public TableDef setName(String name) {
    this.name = name
        .replace(Symbol.BACKTICK, Symbol.EMPTY)
        .replace(Symbol.DOUBLE_QUOTE, Symbol.EMPTY);
    // if full qualified name is null, make it the same as name
    if (fullQualifiedName == null) {
      setFullyQualifieName(this.name);
    }
    return this;
  }

  public TableDef setFullyQualifieName(String fullQualifiedName) {
    this.fullQualifiedName = fullQualifiedName
        .replace(Symbol.BACKTICK, Symbol.EMPTY)
        .replace(Symbol.DOUBLE_QUOTE, Symbol.EMPTY);
    // if name is null, find the table name from full qualified name and assign to name
    if (name == null) {
      if (this.fullQualifiedName.contains(Symbol.DOT)) {
        int start = this.fullQualifiedName.lastIndexOf(Symbol.DOT);
        name = this.fullQualifiedName.substring(start + 1);
      } else {
        name = this.fullQualifiedName;
      }
    }
    return this;
  }

  public int getMaxBytesPerChar() {
    return maxBytesPerChar;
  }

  public boolean containsColumn(String columnName) {
    return nameToFieldMap.containsKey(columnName);
  }

  /**
   * Create a bitmap with primary columns ordinal set as true.
   *
   * @return bitmap
   */
  public BitSet createBitmapWithPkIncluded() {
    BitSet result = new BitSet(getColumnNum());
    if (primaryKeyMeta != null) {
      for (Column pk : getPrimaryKeyColumns()) {
        result.set(pk.getOrdinal());
      }
    }
    return result;
  }

  @Data
  public class Field {
    private int ordinal;
    private String name;
    private Column column;

    public Field(int ordinal, String name, Column column) {
      this.ordinal = ordinal;
      this.name = name;
      this.column = column;
    }
  }

  @Override
  public String toString() {
    return toString(true);
  }

  public String toString(boolean multiLine) {
    StringBuilder sb = new StringBuilder();
    sb.append("CREATE TABLE ");
    sb.append(name == null ? "<undefined>" : name);
    sb.append("(");
    for (int i = 0; i < columnList.size(); i++) {
      Column column = columnList.get(i);
      if (multiLine) {
        sb.append("\n");
      }
      sb.append(column.getName());
      sb.append(" ");
      sb.append(column.getFullType());
      if (!column.isNullable()) {
        sb.append(" NOT NULL");
      }
      if (column.isPrimaryKey()) {
        sb.append(" PRIMARY KEY");
      }
      if (i != columnList.size() - 1) {
        sb.append(",");
      }
    }
    if (primaryKeyMeta != null) {
      sb.append(",");
      if (multiLine) {
        sb.append("\n");
      }
      sb.append("PRIMARY KEY");
      sb.append(" (");
      sb.append(primaryKeyMeta.getKeyColumnNames().stream()
          .collect(Collectors.joining(",")));
      sb.append(")");
    }
    if (secondaryKeyMetaList != null) {
      for (KeyMeta keyMeta : secondaryKeyMetaList) {
        sb.append(",");
        if (multiLine) {
          sb.append("\n");
        }
        sb.append(keyMeta.getType().literal())
            .append(" ");
        if (StringUtils.isNotEmpty(keyMeta.getName())) {
          sb.append(keyMeta.getName())
              .append(" ");
        }
        sb.append("(");
        sb.append(keyMeta.getKeyColumnNames().stream()
            .collect(Collectors.joining(",")));
        sb.append(")");
      }
    }

    if (multiLine) {
      sb.append("\n");
    }
    sb.append(")");
    sb.append("ENGINE = InnoDB");
    if (StringUtils.isNotEmpty(defaultCharset)) {
      sb.append(" DEFAULT CHARSET = ").append(defaultCharset);
    }
    sb.append(";");
    return sb.toString();
  }

}
