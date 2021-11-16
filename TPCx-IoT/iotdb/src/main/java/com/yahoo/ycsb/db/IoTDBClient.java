package com.yahoo.ycsb.db;

import com.yahoo.ycsb.ByteArrayByteIterator;
import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.Status;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.Vector;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.session.SessionDataSet;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.write.record.Tablet;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

public class IoTDBClient extends DB {

  private Session session = null;

  private static final Object CONNECTION_LOCK = new Object();
  private static Map<String, List<Pair<Long, Pair<String, byte[]>>>> cacheData;
  private static int cacheNum = 0;
  private static final int FETCH_SIZE = 256;

  private String db_host = "192.168.130.34";
  private int db_port = 6667;
  private int cache_threshold = 5000;
  private boolean debug = true;

  public void init() throws DBException {
    if ((getProperties().getProperty("debug") != null)
        && (getProperties().getProperty("debug").compareTo("true") == 0)) {
      debug = true;
    }
    if (getProperties().containsKey("iotdbinfo")) {
      String[] serverInfo = getProperties().getProperty("iotdbinfo").split(":");
      if (serverInfo.length != 3) {
        System.err.println("Parse IoTDB Server info failed,it should be ip:port:threshold");
      } else {
        this.db_host = serverInfo[0];
        this.db_port = Integer.parseInt(serverInfo[1]);
        this.cache_threshold = Integer.parseInt(serverInfo[2]);
        if (debug) {
          System.out
              .println(String.format("Parse IoTDB Server info succeed: %s:%d:%d", db_host, db_port,
                  cache_threshold));
        }
      }
    }
    try {
      session = new Session(db_host, db_port);
      session.open();
      if (debug) {
        System.out.println(String.format("start session(%s:%s) succeed", db_host, db_port));
      }
      synchronized (CONNECTION_LOCK) {
        if (cacheData == null) {
          cacheData = new HashMap<>();
        }
      }
    } catch (IoTDBConnectionException e) {
      System.err.println(
          String.format("start session(%s:%s) failed:%s", db_host, db_port, e.toString()));
      e.printStackTrace();
      throw new DBException(e);
    }
  }

  public void cleanup() throws DBException {
    synchronized (CONNECTION_LOCK) {
      try {
        if (cacheData.size() > 0) {
          Map<String, Tablet> tablets = generateTablets();
          session.insertTablets(tablets);
          cacheData.clear();
        }
        session.close();
        session = null;
        if (debug) {
          System.out.println(String.format("cleanup session(%s:%s) succeed", db_host, db_port));
        }
      } catch (IoTDBConnectionException | StatementExecutionException e) {
        System.err.println(
            String
                .format("cleanup session(%s:%s) failed:%s", db_host, db_port, e.toString()));
        e.printStackTrace();
        throw new DBException(e);
      }
    }
  }

  // TPCx-IoT will not use this method
  public Status read(String table, String key, Set<String> fields,
      HashMap<String, ByteIterator> result) {
    return Status.OK;
  }

  // TPCx-IoT will not use this method
  public Status scan(String table, String startkey, int recordcount, Set<String> fields,
      Vector<HashMap<String, ByteIterator>> result) {
    return Status.OK;
  }

  public Status scan(String table, String filter, String clientFilter, String timestamp,
      Set<String> fields, long runStartTime, Vector<HashMap<String, ByteIterator>> result1,
      Vector<HashMap<String, ByteIterator>> result2) {
    String deviceID = String.format("root.%s.%s", clientFilter, filter);
    long newTimeStamp = Long.parseLong(timestamp);
    Status s1 = scanHelper(deviceID, newTimeStamp - 500000000L, fields, result1);
    if (runStartTime > 0L) {
      long timestampVal = newTimeStamp - 1000000000L;
      Status s2 = scanHelper(deviceID, timestampVal, fields, result2);
      if (s1.isOk() && s2.isOk()) {
        return Status.OK;
      }
    }
    return Status.ERROR;
  }

  private Status scanHelper(String deviceID, long timestamp, Set<String> fields,
      Vector<HashMap<String, ByteIterator>> result) {
    long startTime = timestamp;
    try {
      synchronized (CONNECTION_LOCK) {
        long queryStartTime = System.currentTimeMillis();
        SessionDataSet dataSet = session
            .executeRawDataQuery(Collections.singletonList(deviceID), startTime, Long.MAX_VALUE);
        dataSet.setFetchSize(FETCH_SIZE);
        String device;
        String[] paths;
        while (dataSet.hasNext()) {
          RowRecord record = dataSet.next();
          HashMap<String, ByteIterator> rowResult = new HashMap<>();
          for (int i = 0; i < record.getFields().size(); i++) {
            device = dataSet.getColumnNames().get(i + 1);
            paths = device.split("\\.");
            rowResult.put(paths[paths.length - 1],
                new ByteArrayByteIterator(record.getFields().get(i).getBinaryV().getValues()));
          }
          result.add(rowResult);
        }
        System.out.println(String
            .format("scan %d results from server succeed for deviceID %s from %s to end by %d ms",
                result.size(), deviceID, transferLongToDate(startTime),
                System.currentTimeMillis() - queryStartTime));
        dataSet.closeOperationHandle();
      }
    } catch (IoTDBConnectionException | StatementExecutionException e) {
      e.printStackTrace();
      return Status.ERROR;
    }
    return Status.OK;
  }

  private String transferLongToDate(Long millSec) {
    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    Date date = new Date(millSec);
    return sdf.format(date);
  }

  // TPCx-IoT will not use this method
  public Status update(String table, String key, HashMap<String, ByteIterator> values) {
    return Status.OK;
  }

  // only support one sensor now
  public Status insert(String table, String key, HashMap<String, ByteIterator> values) {
    String[] paras = key.split(":");
    String deviceID = String.format("root.%s.%s", paras[0], paras[1]);
    long timestamp = Long.parseLong(paras[2]);

    try {
      Map<String, Tablet> tablets = null;
      synchronized (CONNECTION_LOCK) {
        cacheData.computeIfAbsent(deviceID, k -> new ArrayList<>())
            .add(new Pair<>(timestamp, new Pair<>(values.keySet().iterator().next(),
                values.values().iterator().next().toArray())));
        cacheNum++;
        if (cacheNum >= cache_threshold) {
          tablets = generateTablets();
          cacheNum = 0;
          cacheData.clear();
          if (debug) {
            System.out
                .println(String.format("write %d records to server succeed", cache_threshold));
          }
        }
      }
      if (tablets != null) {
        session.insertTablets(tablets);
      }
    } catch (IoTDBConnectionException | StatementExecutionException e) {
      e.printStackTrace();
      System.err.println(String
          .format("write %d records to server failed because %s", cache_threshold, e.toString()));
      return Status.ERROR;
    }
    return Status.OK;
  }

  private Map<String, Tablet> generateTablets() {
    List<MeasurementSchema> schemaList = Collections.singletonList(
        new MeasurementSchema(cacheData.values().iterator().next().get(0).right.left,
            TSDataType.TEXT));
    Map<String, Tablet> tabletsMap = new HashMap<>();
    for (Entry<String, List<Pair<Long, Pair<String, byte[]>>>> entry : cacheData
        .entrySet()) {
      List<Pair<Long, Pair<String, byte[]>>> TVList = entry.getValue();
      Tablet tablet = new Tablet(entry.getKey(), schemaList, TVList.size());
      tabletsMap.put(entry.getKey(), tablet);
      for (Pair<Long, Pair<String, byte[]>> pair : TVList) {
        int row1 = tablet.rowSize++;
        tablet.addTimestamp(row1, pair.left);
        tablet.addValue(schemaList.get(0).getMeasurementId(), row1,
            new Binary(pair.right.right));
      }
    }
    return tabletsMap;
  }

  // TPCx-IoT will not use this method
  public Status delete(String table, String key) {
    return Status.OK;
  }
}
