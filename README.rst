HBaseMetricsContext
===================

This is an example of extending the Hadoop metrics framework. It stores Hadoop metrics in HBase. Since HBase itself uses the Hadoop metrics framework, you can use it to store its own metrics inside itself. Useful? Maybe. This is just an example after all.

Build the project using:

::

  mvn package

Put the resulting Jar file in the HBase lib directory.

You will need to create a table with the relevant column families. We assume the column families are a composite of:

:: 

  columnFamily = contextName + "." + recordName

In the HBase shell create your table: 

::

  create 'metrics', 'hbase.master', 'hbase.regionserver'

Edit your hadoop-metrics.properties file to include:

::

  hbase.class=com.sematext.hadoop.metrics.HBaseMetricsContext
  hbase.tableName=metrics
  hbase.period=10

Restart HBase and it will start inserting to the metrics table every 10 seconds.