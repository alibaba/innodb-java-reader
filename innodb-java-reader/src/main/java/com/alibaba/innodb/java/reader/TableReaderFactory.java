/*
 * Copyright (C) 1999-2019 Alibaba Group Holding Limited
 */
package com.alibaba.innodb.java.reader;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import com.alibaba.innodb.java.reader.exception.ReaderException;
import com.alibaba.innodb.java.reader.schema.TableDef;
import com.alibaba.innodb.java.reader.schema.provider.TableDefProvider;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import lombok.extern.slf4j.Slf4j;

import static com.alibaba.innodb.java.reader.Constants.DEFAULT_DATA_FILE_SUFFIX;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * TableReader factory.
 *
 * @author xu.zx
 */
@Slf4j
public class TableReaderFactory {

  /**
   * Table definition provider list.
   */
  private List<TableDefProvider> tableDefProviderList;

  /**
   * Table identity to table definition map.
   */
  private Map<String, TableDef> identityTableDefMap;

  /**
   * ibd data file base path.
   */
  private String dataFileBasePath;

  /**
   * Exact file to read.
   */
  private String dataFilePath;

  private String dataFileSuffix;

  private TableReaderFactory(List<TableDefProvider> tableDefProviderList,
                             String dataFileBasePath,
                             String dataFilePath,
                             String dataFileSuffix) {
    checkNotNull(tableDefProviderList);
    this.tableDefProviderList = tableDefProviderList;
    if (StringUtils.isEmpty(dataFileBasePath)) {
      checkNotNull(dataFilePath, "If no dataFileBasePath used, dataFilePath should be defined at least");
      this.dataFilePath = dataFilePath;
    } else {
      this.dataFileBasePath = dataFileBasePath;
    }
    this.dataFileSuffix = dataFileSuffix;
    log.debug("Create TableReaderFactory with {} providers {}, dataFileBasePath is {}, dataFileSuffix is {}",
        tableDefProviderList.size(), tableDefProviderList, dataFileBasePath, dataFileSuffix);
  }

  private void load() {
    ImmutableMap.Builder<String, TableDef> builder = new ImmutableMap.Builder<>();
    for (TableDefProvider tableDefProvider : tableDefProviderList) {
      Map<String, TableDef> map = tableDefProvider.load();
      if (log.isDebugEnabled()) {
        log.debug("Add table {}", map.keySet());
      }
      builder.putAll(map);
    }
    identityTableDefMap = builder.build();
  }

  /**
   * Create table reader based on table identity.
   * <p>
   * Data file path will concat {@link #dataFileBasePath}, table def name and file suffix.
   *
   * @param identity identity can be table name or full qualified name.
   * @return table reader
   */
  public TableReader createTableReader(String identity) {
    checkArgument(StringUtils.isNotEmpty(identity));
    if (!identityTableDefMap.containsKey(identity)) {
      throw new ReaderException("Table definition not found for table " + identity);
    }
    TableDef tableDef = identityTableDefMap.get(identity);
    String filePath = StringUtils.isEmpty(dataFileBasePath)
        ? dataFilePath : dataFileBasePath + tableDef.getName() + dataFileSuffix;
    return new TableReaderImpl(filePath,
        identityTableDefMap.get(identity));
  }

  public Map<String, TableDef> getIdentityTableDefMap() {
    return identityTableDefMap;
  }

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {

    private List<TableDefProvider> tableDefProviderList = new ArrayList<>();

    private String dataFileBasePath;

    private String dataFilePath;

    private String dataFileSuffix = DEFAULT_DATA_FILE_SUFFIX;

    public Builder withProvider(TableDefProvider tableDefProvider) {
      checkNotNull(tableDefProvider);
      if (!tableDefProviderList.contains(tableDefProvider)) {
        this.tableDefProviderList.add(tableDefProvider);
      }
      return this;
    }

    public Builder withDataFileBasePath(String dataFileBasePath) {
      checkArgument(StringUtils.isNotEmpty(dataFileBasePath));
      if (!dataFileBasePath.endsWith(File.separator)) {
        dataFileBasePath += File.separator;
      }
      this.dataFileBasePath = dataFileBasePath;
      return this;
    }

    public Builder withDataFilePath(String dataFilePath) {
      checkArgument(StringUtils.isNotEmpty(dataFilePath));
      this.dataFilePath = dataFilePath;
      return this;
    }

    public Builder withDataFileSuffix(String dataFileSuffix) {
      this.dataFileSuffix = dataFileSuffix;
      return this;
    }

    /**
     * Create TableReaderFactory, this will try loading table definition from providers.
     *
     * @return TableReaderFactory
     */
    public TableReaderFactory build() {
      checkArgument(CollectionUtils.isNotEmpty(tableDefProviderList),
          "TableDefProvider should be not empty");
      checkArgument(StringUtils.isNotEmpty(dataFileBasePath) || StringUtils.isNotEmpty(dataFilePath),
          "dataFileBasePath or dataFilePath are not specified");
      TableReaderFactory tableReaderFactory = new TableReaderFactory(
          ImmutableList.copyOf(tableDefProviderList), dataFileBasePath, dataFilePath, dataFileSuffix);
      tableReaderFactory.load();
      return tableReaderFactory;
    }
  }

}
