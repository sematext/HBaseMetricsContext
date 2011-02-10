/*
 *    Copyright (c) Sematext International
 *    All Rights Reserved
 *
 *    THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF Sematext International
 *    The copyright notice above does not evidence any
 *    actual or intended publication of such source code.
 */
package com.sematext.hadoop.metrics;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.metrics.ContextFactory;
import org.apache.hadoop.metrics.MetricsException;
import org.apache.hadoop.metrics.spi.AbstractMetricsContext;
import org.apache.hadoop.metrics.spi.OutputRecord;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class HBaseMetricsContext extends AbstractMetricsContext {

  private String TABLE_NAME_PROPERTY = "tableName";
  private String PERIOD_PROPERTY = "period";
  private HTable table;

  public HBaseMetricsContext() {
    super();
  }

  @Override
  public void init(String contextName, ContextFactory contextfactory) {
    super.init(contextName, contextfactory);
    String periodStr = getAttribute(PERIOD_PROPERTY);
    if (periodStr != null) {
      int period = 0;
      try {
        period = Integer.parseInt(periodStr);
      } catch (NumberFormatException nfe) {
      }
      if (period <= 0) {
        throw new MetricsException("Invalid period: " + periodStr);
      }
      setPeriod(period);
    }
  }

  private HTable getTable() throws MetricsException {
    try {
      if (table == null) {
        String tableName = getAttribute(TABLE_NAME_PROPERTY);
        if (tableName == null) {
          throw new MetricsException("tableName not set for metrics context");
        }
        table = new HTable(HBaseConfiguration.create(), tableName);
      }
    } catch (IOException e) {
      throw new MetricsException(e.getMessage());
    }
    return table;
  }

  @Override
  protected void emitRecord(String context, String record, OutputRecord outputrecord) throws IOException {
    HTable table = getTable();
    if (table == null) {
      return;
    }

    long timestamp = System.currentTimeMillis();
    byte[] family = Bytes.toBytes(context + "." + record);

    ByteArrayOutputStream row = new ByteArrayOutputStream();
    // Most recent records will be at the beginning of scans.
    row.write(Bytes.toBytes(Long.MAX_VALUE - timestamp));
    // Add the tags to disambiguate records
    for (String tagName : outputrecord.getTagNames()) {
      row.write(Bytes.toBytes(tagName));
      row.write(Bytes.toBytes(outputrecord.getTag(tagName).toString()));
    }

    Put put = new Put(row.toByteArray(), timestamp);
    for (String tagName : outputrecord.getTagNames()) {
      put.add(family, Bytes.toBytes(tagName), Bytes.toBytes(outputrecord.getTag(tagName).toString()));
    }
    for (String metricName : outputrecord.getMetricNames()) {
      put.add(family, Bytes.toBytes(metricName), Bytes.toBytes(outputrecord.getMetric(metricName).toString()));
    }
    table.put(put);
  }

  @Override
  public void close() {
    if (table != null) {
      try {
        table.close();
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
  }
}
