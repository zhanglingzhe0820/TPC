package com.yahoo.ycsb;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.yahoo.ycsb.ByteArrayByteIterator;
import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.Status;
import com.yahoo.ycsb.measurements.Measurements;
import java.io.IOException;
import java.util.ConcurrentModificationException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.BufferedMutatorParams;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.security.UserGroupInformation;

public class HBaseClient10 extends DB {
  private Configuration config = HBaseConfiguration.create();

  private static AtomicInteger threadCount = new AtomicInteger(0);

  private boolean debug = true;

  private String tableName = "";

  private static Connection connection = null;

  private static final Object CONNECTION_LOCK = new Object();

  private Table currentTable = null;

  private BufferedMutator bufferedMutator = null;

  private String columnFamily = "";

  private byte[] columnFamilyBytes;

  private Durability durability = Durability.USE_DEFAULT;

  private boolean usePageFilter = true;

  private boolean clientSideBuffering = false;

  private long writeBufferSize = 12582912L;

  public void init() throws DBException {
    if ("true"
        .equals(getProperties().getProperty("clientbuffering", "false")))
      this.clientSideBuffering = true;
    if (getProperties().containsKey("writebuffersize"))
      this
          .writeBufferSize = Long.parseLong(getProperties().getProperty("writebuffersize"));
    if (getProperties().getProperty("durability") != null)
      this
          .durability = Durability.valueOf(getProperties().getProperty("durability"));
    if ("kerberos".equalsIgnoreCase(this.config.get("hbase.security.authentication"))) {
      this.config.set("hadoop.security.authentication", "Kerberos");
      UserGroupInformation.setConfiguration(this.config);
    }
    if (getProperties().getProperty("principal") != null &&
        getProperties().getProperty("keytab") != null)
      try {
        UserGroupInformation.loginUserFromKeytab(getProperties().getProperty("principal"),
            getProperties().getProperty("keytab"));
      } catch (IOException e) {
        System.err.println("Keytab file is not readable or not found");
        throw new DBException(e);
      }
    try {
      threadCount.getAndIncrement();
      synchronized (CONNECTION_LOCK) {
        if (connection == null)
          connection = ConnectionFactory.createConnection(this.config);
      }
    } catch (IOException e) {
      throw new DBException(e);
    }
    if (getProperties().getProperty("debug") != null &&
        getProperties().getProperty("debug").compareTo("true") == 0)
      this.debug = true;
    if ("false"
        .equals(getProperties().getProperty("hbase.usepagefilter", "true")))
      this.usePageFilter = false;
    this.columnFamily = getProperties().getProperty("columnfamily");
    if (this.columnFamily == null) {
      System.err.println("Error, must specify a columnfamily for HBase table");
      throw new DBException("No columnfamily specified");
    }
    this.columnFamilyBytes = Bytes.toBytes(this.columnFamily);
    String table = getProperties().getProperty("table", "usertable");
    try {
      TableName tName = TableName.valueOf(table);
      synchronized (CONNECTION_LOCK) {
        connection.getTable(tName).getTableDescriptor();
      }
    } catch (IOException e) {
      throw new DBException(e);
    }
  }

  public void cleanup() throws DBException {
    Measurements measurements = Measurements.getMeasurements();
    try {
      long st = System.nanoTime();
      if (this.bufferedMutator != null)
        this.bufferedMutator.close();
      if (this.currentTable != null)
        this.currentTable.close();
      long en = System.nanoTime();
      String type = this.clientSideBuffering ? "UPDATE" : "CLEANUP";
      measurements.measure(type, (int)((en - st) / 1000L));
      threadCount.decrementAndGet();
      if (threadCount.get() <= 0)
        synchronized (CONNECTION_LOCK) {
          if (connection != null) {
            connection.close();
            connection = null;
          }
        }
    } catch (IOException e) {
      throw new DBException(e);
    }
  }

  public void getHTable(String table) throws IOException {
    TableName tName = TableName.valueOf(table);
    synchronized (CONNECTION_LOCK) {
      this.currentTable = connection.getTable(tName);
      if (this.clientSideBuffering) {
        BufferedMutatorParams p = new BufferedMutatorParams(tName);
        p.writeBufferSize(this.writeBufferSize);
        this.bufferedMutator = connection.getBufferedMutator(p);
      }
    }
  }

  public Status read(String table, String key, Set<String> fields, HashMap<String, ByteIterator> result) {
    if (!this.tableName.equals(table)) {
      this.currentTable = null;
      try {
        getHTable(table);
        this.tableName = table;
      } catch (IOException e) {
        System.err.println("Error accessing HBase table: " + e);
        return Status.ERROR;
      }
    }
    Result r = null;
    try {
      if (this.debug) {
        System.out
            .println("Doing read from HBase columnfamily " + this.columnFamily);
        System.out.println("Doing read for key: " + key);
      }
      Get g = new Get(Bytes.toBytes(key));
      if (fields == null) {
        g.addFamily(this.columnFamilyBytes);
      } else {
        for (String field : fields)
          g.addColumn(this.columnFamilyBytes, Bytes.toBytes(field));
      }
      r = this.currentTable.get(g);
    } catch (IOException e) {
      if (this.debug)
        System.err.println("Error doing get: " + e);
      return Status.ERROR;
    } catch (ConcurrentModificationException e) {
      return Status.ERROR;
    }
    if (r.isEmpty())
      return Status.NOT_FOUND;
    while (r.advance()) {
      Cell c = r.current();
      result.put(Bytes.toString(CellUtil.cloneQualifier(c)), new ByteArrayByteIterator(
          CellUtil.cloneValue(c)));
      if (this.debug)
        System.out.println("Result for field: " +
            Bytes.toString(CellUtil.cloneQualifier(c)) + " is: " +
            Bytes.toString(CellUtil.cloneValue(c)));
    }
    return Status.OK;
  }

  public Status scan(String table, String startkey, int recordcount, Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
    if (!this.tableName.equals(table)) {
      this.currentTable = null;
      try {
        getHTable(table);
        this.tableName = table;
      } catch (IOException e) {
        System.err.println("Error accessing HBase table: " + e);
        return Status.ERROR;
      }
    }
    Scan s = new Scan(Bytes.toBytes(startkey));
    s.setCaching(recordcount);
    if (this.usePageFilter)
      s.setFilter((Filter)new PageFilter(recordcount));
    if (fields == null) {
      s.addFamily(this.columnFamilyBytes);
    } else {
      for (String field : fields)
        s.addColumn(this.columnFamilyBytes, Bytes.toBytes(field));
    }
    ResultScanner scanner = null;
    try {
      scanner = this.currentTable.getScanner(s);
      int numResults = 0;
      for (Result rr = scanner.next(); rr != null; rr = scanner.next()) {
        String key = Bytes.toString(rr.getRow());
        if (this.debug)
          System.out.println("Got scan result for key: " + key);
        HashMap<String, ByteIterator> rowResult = new HashMap<>();
        while (rr.advance()) {
          Cell cell = rr.current();
          rowResult.put(Bytes.toString(CellUtil.cloneQualifier(cell)), new ByteArrayByteIterator(
              CellUtil.cloneValue(cell)));
        }
        result.add(rowResult);
        numResults++;
        if (numResults >= recordcount)
          break;
      }
    } catch (IOException e) {
      if (this.debug)
        System.out.println("Error in getting/parsing scan result: " + e);
      return Status.ERROR;
    } finally {
      if (scanner != null)
        scanner.close();
    }
    return Status.OK;
  }

  public Status scan(String table, String filter, String clientFilter, String timestamp, Set<String> fields, long runStartTime, Vector<HashMap<String, ByteIterator>> result1, Vector<HashMap<String, ByteIterator>> result2) {
    long oldTimeStamp;
    if (!this.tableName.equals(table)) {
      this.currentTable = null;
      try {
        getHTable(table);
        this.tableName = table;
      } catch (IOException e) {
        System.err.println("Error accessing HBase table: " + e);
        return Status.ERROR;
      }
    }
    Status s1 = scanHelper(table, filter, clientFilter, Long.valueOf(timestamp).longValue(), fields, result1);
    if (runStartTime > 0L) {
      long time = Long.valueOf(timestamp).longValue() - runStartTime;
      oldTimeStamp = Long.valueOf(timestamp).longValue() - time;
    } else {
      oldTimeStamp = Long.valueOf(timestamp).longValue() - 1800000L;
    }
    long timestampVal = oldTimeStamp + (long)(Math.random() * (Long.valueOf(timestamp).longValue() - 10000L - oldTimeStamp));
    Status s2 = scanHelper(table, filter, clientFilter, timestampVal, fields, result2);
    if (s1.isOk() && s2.isOk())
      return Status.OK;
    return Status.ERROR;
  }

  private Status scanHelper(String table, String filter, String clientFilter, long timestamp, Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
    Scan s = new Scan();
    ResultScanner scanner = null;
    try {
      s.setTimeRange(timestamp, timestamp + 5000L);
      StringBuffer startKey = new StringBuffer();
      startKey.append(clientFilter);
      startKey.append(":");
      startKey.append(filter);
      startKey.append(":");
      startKey.append(timestamp);
      StringBuffer endKey = new StringBuffer();
      endKey.append(clientFilter);
      endKey.append(":");
      endKey.append(filter);
      endKey.append(":");
      endKey.append(timestamp + 5000L);
      s.setStartRow(startKey.toString().getBytes());
      s.setStopRow(endKey.toString().getBytes());
      if (fields == null) {
        s.addFamily(this.columnFamilyBytes);
      } else {
        for (String field : fields)
          s.addColumn(this.columnFamilyBytes, Bytes.toBytes(field));
      }
      scanner = this.currentTable.getScanner(s);
      int numResults = 0;
      for (Result rr = scanner.next(); rr != null; rr = scanner.next()) {
        String key = Bytes.toString(rr.getRow());
        if (this.debug)
          System.out.println("Got scan result for key: " + key);
        HashMap<String, ByteIterator> rowResult = new HashMap<>();
        while (rr.advance()) {
          Cell cell = rr.current();
          rowResult.put(Bytes.toString(CellUtil.cloneQualifier(cell)), new ByteArrayByteIterator(
              CellUtil.cloneValue(cell)));
        }
        result.add(rowResult);
        numResults++;
      }
    } catch (IOException e) {
      if (this.debug)
        System.out.println("Error in getting/parsing scan result: " + e);
      return Status.ERROR;
    } catch (Exception e) {
      if (this.debug)
        System.out.println("Error in getting/parsing scan result: " + e);
      return Status.ERROR;
    } finally {
      if (scanner != null)
        scanner.close();
    }
    return Status.OK;
  }

  public Status update(String table, String key, HashMap<String, ByteIterator> values) {
    if (!this.tableName.equals(table)) {
      this.currentTable = null;
      try {
        getHTable(table);
        this.tableName = table;
      } catch (IOException e) {
        System.err.println("Error accessing HBase table: " + e);
        return Status.ERROR;
      }
    }
    if (this.debug)
      System.out.println("Setting up put for key: " + key);
    Put p = new Put(Bytes.toBytes(key));
    p.setDurability(this.durability);
    for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
      byte[] value = ((ByteIterator)entry.getValue()).toArray();
      if (this.debug)
        System.out.println("Adding field/value " + (String)entry.getKey() + "/" +
            Bytes.toStringBinary(value) + " to put request");
      p.addColumn(this.columnFamilyBytes, Bytes.toBytes(entry.getKey()), value);
    }
    try {
      if (this.clientSideBuffering) {
        Preconditions.checkNotNull(this.bufferedMutator);
        this.bufferedMutator.mutate((Mutation)p);
      } else {
        this.currentTable.put(p);
      }
    } catch (IOException e) {
      if (this.debug)
        System.err.println("Error doing put: " + e);
      return Status.ERROR;
    } catch (ConcurrentModificationException e) {
      return Status.ERROR;
    }
    return Status.OK;
  }

  public Status insert(String table, String key, HashMap<String, ByteIterator> values) {
    return update(table, key, values);
  }

  public Status delete(String table, String key) {
    if (!this.tableName.equals(table)) {
      this.currentTable = null;
      try {
        getHTable(table);
        this.tableName = table;
      } catch (IOException e) {
        System.err.println("Error accessing HBase table: " + e);
        return Status.ERROR;
      }
    }
    if (this.debug)
      System.out.println("Doing delete for key: " + key);
    Delete d = new Delete(Bytes.toBytes(key));
    d.setDurability(this.durability);
    try {
      if (this.clientSideBuffering) {
        Preconditions.checkNotNull(this.bufferedMutator);
        this.bufferedMutator.mutate((Mutation)d);
      } else {
        this.currentTable.delete(d);
      }
    } catch (IOException e) {
      if (this.debug)
        System.err.println("Error doing delete: " + e);
      return Status.ERROR;
    }
    return Status.OK;
  }

  @VisibleForTesting
  void setConfiguration(Configuration newConfig) {
    this.config = newConfig;
  }
}
