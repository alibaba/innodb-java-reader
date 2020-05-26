/*
 * Copyright (C) 1999-2019 Alibaba Group Holding Limited
 */
package com.alibaba.innodb.java.reader.schema;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import com.alibaba.innodb.java.reader.CharsetMapping;
import com.alibaba.innodb.java.reader.CollationMapping;
import com.alibaba.innodb.java.reader.column.ColumnType;
import com.alibaba.innodb.java.reader.exception.ReaderException;
import com.alibaba.innodb.java.reader.exception.SqlParseException;
import com.alibaba.innodb.java.reader.util.Pair;
import com.alibaba.innodb.java.reader.util.Symbol;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.extern.slf4j.Slf4j;

import static com.alibaba.innodb.java.reader.Constants.COLUMN_ROW_ID;
import static com.alibaba.innodb.java.reader.Constants.DEFAULT_JAVA_CHARSET;
import static com.alibaba.innodb.java.reader.Constants.DEFAULT_MYSQL_CHARSET;
import static com.alibaba.innodb.java.reader.Constants.DEFAULT_MYSQL_COLLATION;
import static com.alibaba.innodb.java.reader.column.ColumnType.CHAR;
import static com.alibaba.innodb.java.reader.schema.KeyMeta.Type.FOREIGN_KEY;
import static com.alibaba.innodb.java.reader.schema.KeyMeta.Type.FULLTEXT_KEY;
import static com.alibaba.innodb.java.reader.schema.KeyMeta.Type.PRIMARY_KEY;
import static com.alibaba.innodb.java.reader.schema.KeyMeta.Type.UNIQUE_INDEX;
import static com.alibaba.innodb.java.reader.schema.KeyMeta.Type.UNIQUE_KEY;
import static com.alibaba.innodb.java.reader.schema.KeyMeta.Type.isValidSk;
import static com.alibaba.innodb.java.reader.util.Utils.sanitize;
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
   * Table DDL collation, for example can be utf8_general_ci, utf8_bin (case sensitive).
   */
  private String collation = DEFAULT_MYSQL_COLLATION;

  /**
   * Per {@link #collation}, determine if string type columns are case sensitive or not.
   */
  private boolean collationCaseSensitive = false;

  /**
   * //TODO make sure this is the right way to implement
   * For example, if table charset set to utf8, then it will consume up to 3 bytes for one character.
   * if it is utf8mb4, then it must be set to 4.
   */
  private int maxBytesPerChar = 1;

  /**
   * When build new TableDef for secondary key, this indicates that the table
   * is derived from sk, and will only by used internally.
   */
  private boolean derivedFromSk;

  public TableDef() {
    this.columnList = new ArrayList<>();
    this.columnNames = new ArrayList<>();
    this.nameToFieldMap = new HashMap<>();
    this.nullableColumnList = new ArrayList<>();
    this.variableLengthColumnList = new ArrayList<>();
  }

  public void prepare() {
    checkState(CollectionUtils.isNotEmpty(columnList), "No column is specified");
    makeFirstUniqueKeyAsPrimaryKeyIfPossible();
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

  public KeyMeta getPrimaryKeyMeta() {
    return primaryKeyMeta;
  }

  public List<KeyMeta> getSecondaryKeyMetaList() {
    if (CollectionUtils.isEmpty(secondaryKeyMetaList)) {
      return ImmutableList.of();
    }
    return secondaryKeyMetaList;
  }

  public Map<String, KeyMeta> getSecondaryKeyMetaMap() {
    if (CollectionUtils.isEmpty(secondaryKeyMetaList)) {
      return ImmutableMap.of();
    }
    return secondaryKeyMetaList.stream()
        .collect(Collectors.toMap(KeyMeta::getName, Function.identity()));
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
    checkArgument(KeyMeta.Type.isValid(type),
        "Key type is invalid " + type);
    if (isValidSk(type)) {
      checkArgument(CollectionUtils.isNotEmpty(keyColumnNames),
          "Key column names is empty for key = " + keyName);
    }

    ImmutableList.Builder<Column> cols = ImmutableList.builder();
    ImmutableList.Builder<String> colNames = ImmutableList.builder();
    ImmutableList.Builder<Column> varLenCols = ImmutableList.builder();
    ImmutableList.Builder<String> varLenColNames = ImmutableList.builder();
    ImmutableList.Builder<Integer> varLen = ImmutableList.builder();
    for (String colName : keyColumnNames) {
      String columnName = sanitize(colName);
      String wrappedString = null;
      if (columnName.contains(Symbol.LEFT_PARENTHESES) && columnName.contains(Symbol.RIGHT_PARENTHESES)) {
        wrappedString = StringUtils
            .substringBetween(columnName, Symbol.LEFT_PARENTHESES, Symbol.RIGHT_PARENTHESES);
        checkState(StringUtils.isNotEmpty(wrappedString),
            "String " + colName + " cannot be empty between ( and ), for example varchar(255)");
        columnName = columnName.substring(0, columnName.indexOf(Symbol.LEFT_PARENTHESES));
      }
      if (containsColumn(columnName)) {
        Column col = getField(columnName).getColumn();
        cols.add(col);
        colNames.add(columnName);

        if (isVarLen(col)) {
          varLenCols.add(col);
          varLenColNames.add(columnName);
          if (StringUtils.isNotEmpty(wrappedString)) {
            varLen.add(Integer.parseInt(wrappedString));
          } else {
            varLen.add(0);
          }
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
        .keyVarLen(varLen.build())
        .numOfColumns(keyColumnNames.size())
        .type(KeyMeta.Type.parse(type))
        .name(keyName != null ? sanitize(keyName) : null)
        .build()
        .validate();
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
    this.collation = CollationMapping.getDefaultCollation(defaultCharset);
    this.collationCaseSensitive = CollationMapping.isCollationCaseSensitive(this.collation);
    return this;
  }

  /**
   * Table charset must be set before collation.
   *
   * @param collation collation
   * @return table definition
   */
  public TableDef setCollation(String collation) {
    checkArgument(CollectionUtils.isEmpty(columnList), "Collation should be set before adding columns");
    this.collationCaseSensitive = CollationMapping.isCollationCaseSensitive(collation);
    this.collation = collation;
    return this;
  }

  public String getCollation() {
    return collation;
  }

  public String getName() {
    return name;
  }

  public String getFullyQualifiedName() {
    return fullQualifiedName;
  }

  public TableDef setName(String name) {
    this.name = sanitize(name);
    // if full qualified name is null, make it the same as name
    if (fullQualifiedName == null) {
      setFullyQualifiedName(this.name);
    }
    return this;
  }

  public TableDef setFullyQualifiedName(String fullQualifiedName) {
    this.fullQualifiedName = sanitize(fullQualifiedName);
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

  /**
   * Build a virtual secondary key table definition.
   * This will be used in secondary key querying.
   *
   * @param skName    secondary key name
   * @param skOrdinal secondary key ordinal, starts from 0
   * @return table definition
   */
  public Pair<KeyMeta, TableDef> buildSkTableDef(String skName, Optional<Integer> skOrdinal) {
    checkArgument(CollectionUtils.isNotEmpty(secondaryKeyMetaList),
        "Secondary key is empty");
    KeyMeta keyMeta;
    if (skOrdinal.isPresent()) {
      checkArgument(skOrdinal.get() < secondaryKeyMetaList.size(),
          "Secondary key ordinal " + skOrdinal + " is out of range ");
      keyMeta = secondaryKeyMetaList.get(skOrdinal.get());
    } else {
      Map<String, KeyMeta> skNameMap = getSecondaryKeyMetaMap();
      keyMeta = skNameMap.get(skName);
    }
    if (keyMeta == null) {
      throw new ReaderException("Secondary key does not exist with name " + skName + ", ordinal " + skOrdinal);
    }
    if (keyMeta.getType() == null || FULLTEXT_KEY == keyMeta.getType()
        || FOREIGN_KEY == keyMeta.getType() || PRIMARY_KEY == keyMeta.getType()) {
      throw new IllegalStateException("Secondary key type does not support " + keyMeta.getType());
    }
    if (CollectionUtils.isEmpty(keyMeta.getKeyColumns())) {
      throw new AssertionError("Secondary key columns should not be empty " + skOrdinal);
    }
    TableDef tableDef = new TableDef().setDefaultCharset(defaultCharset);
    for (Column keyColumn : keyMeta.getKeyColumns()) {
      tableDef.addColumn(keyColumn.copy());
    }
    if (isNoPrimaryKey()) {
      tableDef.addColumn(createRowIdColumn());
    } else {
      for (Column primaryKeyColumn : getPrimaryKeyColumns()) {
        tableDef.addColumn(primaryKeyColumn.copy());
      }
    }
    tableDef.setPrimaryKeyColumns(keyMeta.getKeyColumnNames());
    tableDef.setDerivedFromSk(true);
    return Pair.of(keyMeta, tableDef);
  }

  public boolean isDerivedFromSk() {
    return derivedFromSk;
  }

  public void setDerivedFromSk(boolean derivedFromSk) {
    this.derivedFromSk = derivedFromSk;
  }

  public boolean isCollationCaseSensitive() {
    return collationCaseSensitive;
  }

  public TableDef copy() {
    TableDef result = new TableDef();
    result.setDefaultCharset(defaultCharset);
    result.setCollation(collation);
    result.primaryKeyMeta = primaryKeyMeta;
    result.secondaryKeyMetaList = secondaryKeyMetaList;
    result.setName(name);
    result.setFullyQualifiedName(fullQualifiedName);
    for (Column column : columnList) {
      result.addColumn(column);
    }
    return result;
  }

  public static Column createRowIdColumn() {
    return new Column().setName(COLUMN_ROW_ID).setType(ColumnType.ROW_ID).setNullable(false);
  }

  /**
   * If no primary key provided, make first non-null unique key as primary key.
   */
  private void makeFirstUniqueKeyAsPrimaryKeyIfPossible() {
    if (primaryKeyMeta == null) {
      if (CollectionUtils.isNotEmpty(secondaryKeyMetaList)) {
        Predicate<KeyMeta> predicate = k -> {
          if (k.getType() == UNIQUE_KEY || k.getType() == UNIQUE_INDEX) {
            for (Column keyColumn : k.getKeyColumns()) {
              if (keyColumn.isNullable()) {
                return false;
              }
            }
            return true;
          }
          return false;
        };
        Optional<KeyMeta> pkMeta = secondaryKeyMetaList.stream().filter(predicate).findFirst();
        if (pkMeta.isPresent()) {
          log.debug("Make key `{}` as primary key", pkMeta.get().getName());
          setPrimaryKeyColumns(pkMeta.get().getKeyColumnNames());
          secondaryKeyMetaList.remove(pkMeta.get());
        }
      }
    }
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
      String charsetAndCollate = column.getCharsetCollationString();
      if (StringUtils.isNotEmpty(charsetAndCollate)) {
        sb.append(" ").append(charsetAndCollate);
      }
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
