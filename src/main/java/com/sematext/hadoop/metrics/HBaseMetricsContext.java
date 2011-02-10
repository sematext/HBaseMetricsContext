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
import org.apache.hadoop.metrics.spi.AbstractMetricsContext;
import org.apache.hadoop.metrics.spi.OutputRecord;
import org.apache.log4j.Logger;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class HBaseMetricsContext extends AbstractMetricsContext {
  private static final Logger log = Logger.getLogger(HBaseMetricsContext.class);

  private String TABLE_NAME_PROPERTY = "tableName";
  private HTable table;

  public HBaseMetricsContext() {
    super();
  }

  synchronized public void init() throws IOException {
    if (table == null) {
      String tableName = getAttribute(TABLE_NAME_PROPERTY);
      this.table = new HTable(HBaseConfiguration.create(), tableName);
    }
  }

  @Override
  protected void emitRecord(String context, String record, OutputRecord outputrecord) throws IOException {
    Put put = null;
    try {
      init();

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
      put = new Put(row.toByteArray(), timestamp);

      for (String tagName : outputrecord.getTagNames()) {
        put.add(family, Bytes.toBytes(tagName), Bytes.toBytes(outputrecord.getTag(tagName).toString()));
      }
      for (String metricName : outputrecord.getMetricNames()) {
        put.add(family, Bytes.toBytes(metricName), Bytes.toBytes(outputrecord.getMetric(metricName).toString()));
      }
      table.put(put);
    } catch (IOException e) {
      if (log.isDebugEnabled()) {
        log.debug(put.toString(), e);
      }
      throw e;
    }
  }
}
