package com.yahoo.ycsb;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.yahoo.ycsb.measurements.Measurements;
import com.yahoo.ycsb.workloads.CoreWorkload;
import java.io.IOException;
import java.util.ArrayList;
import java.util.ConcurrentModificationException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;
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
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.session.pool.SessionPool;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

public class IoTDBClient extends DB {

  private boolean debug = true;

  private int storageGroupNum = 0;
  private static Map<String, Integer> hash = null;
  /**
   * A Cluster Connection instance that is shared by all running ycsb threads. Needs to be
   * initialized late so we pick up command-line configs if any. To ensure one instance only in a
   * multi-threaded context, guard access with a 'lock' object.
   *
   * @See #CONNECTION_LOCK.
   */
  private static SessionPool sessionPool = null;

  private static final Object CONNECTION_LOCK = new Object();
  private static AtomicInteger threadCount = new AtomicInteger(0);

  public static final String USERNAME_PROPERTY = "iotdb.username";
  public static final String PASSWORD_PROPERTY = "iotdb.password";
  public static final String STORAGE_GROUP_NUM_PROPERTY = "iotdb.storageGroupNum";
  public static final String USERNAME_PROPERTY_DEFAULT = "root";
  public static final String PASSWORD_PROPERTY_DEFAULT = "root";
  public static final String STORAGE_GROUP_PREFIX = "root.sg";
  public static final String STORAGE_GROUP_NUM_DEFAULT = "10";

  public void init() throws DBException {
    storageGroupNum = Integer.parseInt(
        getProperties().getProperty(STORAGE_GROUP_NUM_PROPERTY, STORAGE_GROUP_NUM_DEFAULT));
    if (CoreWorkload.prekeys.length % storageGroupNum != 0) {
      System.err.println("Error, storageGroupNum must can be divided by 160(length of perkeys)");
      throw new DBException("StorageGroupNum must can be divided by 160(length of perkeys)");
    }
    try {
      threadCount.getAndIncrement();
      synchronized (CONNECTION_LOCK) {
        if (sessionPool == null) {
          hash = new ConcurrentHashMap<>();
          sessionPool = new SessionPool("127.0.0.1", 6667, USERNAME_PROPERTY_DEFAULT,
              PASSWORD_PROPERTY_DEFAULT, 3);
          for (int i = 0; i < storageGroupNum; i++) {
            sessionPool.setStorageGroup(STORAGE_GROUP_PREFIX + i);
          }
        }
      }

    } catch (IoTDBConnectionException | StatementExecutionException e) {
      e.printStackTrace();
      throw new DBException(e);
    }

    if ((getProperties().getProperty("debug") != null)
        && (getProperties().getProperty("debug").compareTo("true") == 0)) {
      debug = true;
    }
  }

  public void cleanup() throws DBException {
    threadCount.decrementAndGet();
    if (threadCount.get() <= 0) {
      synchronized (CONNECTION_LOCK) {
        if (sessionPool != null) {
          List<String> storageGroups = new ArrayList<>();
          for (int i = 0; i < storageGroupNum; i++) {
            storageGroups.add((STORAGE_GROUP_PREFIX + i));
          }
          try {
            sessionPool.deleteStorageGroups(storageGroups);
          } catch (IoTDBConnectionException | StatementExecutionException e) {
            e.printStackTrace();
            throw new DBException(e);
          }
          sessionPool.close();
          sessionPool = null;
          hash.clear();
        }
      }
    }
  }

  public Status read(String table, String key, Set<String> fields,
      HashMap<String, ByteIterator> result) {
    return Status.OK;
  }

  public Status scan(String table, String startkey, int recordcount, Set<String> fields,
      Vector<HashMap<String, ByteIterator>> result) {
    return Status.OK;
  }

  public Status scan(String table, String filter, String clientFilter, String timestamp,
      Set<String> fields, long runStartTime, Vector<HashMap<String, ByteIterator>> result1,
      Vector<HashMap<String, ByteIterator>> result2) {
    return Status.OK;
  }

  public Status update(String table, String key, HashMap<String, ByteIterator> values) {
    String[] paras = key.split(":");
    String device = String.format("%s_%s", paras[0], paras[1]);
    int index = hash.computeIfAbsent(device, k -> Math.abs(device.hashCode()) % storageGroupNum);
    String deviceID = String.format("%s%d.%s", STORAGE_GROUP_PREFIX, index, device);
    long timestamp = Long.parseLong(paras[2]);
    List<String> measurements = new ArrayList<>();
    List<TSDataType> types = new ArrayList<>();
    List<Object> datas = new ArrayList<>();
    for (Entry<String, ByteIterator> entry : values.entrySet()) {
      measurements.add(entry.getKey());
      types.add(TSDataType.TEXT);
      datas.add((entry.getValue()).toString());
    }
    try {
      synchronized (CONNECTION_LOCK) {
        sessionPool.insertRecord(deviceID, timestamp, measurements, types, datas);
      }
    } catch (IoTDBConnectionException | StatementExecutionException e) {
      e.printStackTrace();
      return Status.ERROR;
    }

    return Status.OK;
  }

  public Status insert(String table, String key, HashMap<String, ByteIterator> values) {
    return update(table, key, values);
  }

  public Status delete(String table, String key) {
    String[] paras = key.split(":");
    String device = String.format("%s_%s", paras[0], paras[1]);
    int index = hash.get(device);
    String deviceID = String.format("%s%d.%s", STORAGE_GROUP_PREFIX, index, device);
    long timestamp = Long.parseLong(paras[2]);
    try {
      sessionPool.deleteData(deviceID, timestamp);
    } catch (IoTDBConnectionException | StatementExecutionException e) {
      e.printStackTrace();
      return Status.ERROR;
    }
    return Status.OK;
  }
}
