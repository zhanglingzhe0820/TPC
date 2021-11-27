package com.yahoo.ycsb;

import com.yahoo.ycsb.measurements.Measurements;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;
import java.util.logging.Logger;
import org.apache.htrace.core.TraceScope;
import org.apache.htrace.core.Tracer;

public class DBWrapper extends DB {

  private final DB db;

  private final Measurements measurements;

  private final Tracer tracer;

  private boolean reportLatencyForEachError = false;

  private HashSet<String> latencyTrackedErrors = new HashSet<>();

  private static final String REPORT_LATENCY_FOR_EACH_ERROR_PROPERTY = "reportlatencyforeacherror";

  private static final String REPORT_LATENCY_FOR_EACH_ERROR_PROPERTY_DEFAULT = "false";

  private static final String LATENCY_TRACKED_ERRORS_PROPERTY = "latencytrackederrors";

  private final String scopeStringCleanup;

  private final String scopeStringDelete;

  private final String scopeStringInit;

  private final String scopeStringInsert;

  private final String scopeStringRead;

  private final String scopeStringScan;

  private final String scopeStringUpdate;

  private boolean isQuerying = false;

  public DBWrapper(DB db, Tracer tracer) {
    this.db = db;
    this.measurements = Measurements.getMeasurements();
    this.tracer = tracer;
    String simple = db.getClass().getSimpleName();
    this.scopeStringCleanup = simple + "#cleanup";
    this.scopeStringDelete = simple + "#delete";
    this.scopeStringInit = simple + "#init";
    this.scopeStringInsert = simple + "#insert";
    this.scopeStringRead = simple + "#read";
    this.scopeStringScan = simple + "#scan";
    this.scopeStringUpdate = simple + "#update";
  }

  public void setProperties(Properties p) {
    this.db.setProperties(p);
  }

  public Properties getProperties() {
    return this.db.getProperties();
  }

  public void init() throws DBException {
    try (TraceScope span = this.tracer.newScope(this.scopeStringInit)) {
      this.db.init();
      this.reportLatencyForEachError = Boolean.parseBoolean(getProperties()
          .getProperty("reportlatencyforeacherror", "false"));
      if (!this.reportLatencyForEachError) {
        String latencyTrackedErrorsProperty = getProperties()
            .getProperty("latencytrackederrors", null);
        if (latencyTrackedErrorsProperty != null) {
          this.latencyTrackedErrors = new HashSet<>(Arrays.asList(latencyTrackedErrorsProperty
              .split(",")));
        }
      }
      System.err.println(
          "DBWrapper: report latency for each error is " + this.reportLatencyForEachError
              + " and specific error codes to track for latency are: " + this.latencyTrackedErrors

              .toString());
    }
  }

  public void cleanup() throws DBException {
    try (TraceScope span = this.tracer.newScope(this.scopeStringCleanup)) {
      long ist = this.measurements.getIntendedtartTimeNs();
      long st = System.nanoTime();
      this.db.cleanup();
      long en = System.nanoTime();
      measure("CLEANUP", Status.OK, ist, st, en);
    }
  }

  public Status read(String table, String key, Set<String> fields,
      HashMap<String, ByteIterator> result) {
    try (TraceScope span = this.tracer.newScope(this.scopeStringRead)) {
      long ist = this.measurements.getIntendedtartTimeNs();
      long st = System.nanoTime();
      Status res = this.db.read(table, key, fields, result);
      long en = System.nanoTime();
      measure("READ", res, ist, st, en);
      this.measurements.reportStatus("READ", res);
      return res;
    }
  }

  public Status scan(String table, String startkey, int recordcount, Set<String> fields,
      Vector<HashMap<String, ByteIterator>> result) {
    try (TraceScope span = this.tracer.newScope(this.scopeStringScan)) {
      long ist = this.measurements.getIntendedtartTimeNs();
      long st = System.nanoTime();
      Status res = this.db.scan(table, startkey, recordcount, fields, result);
      long en = System.nanoTime();
      measure("SCAN", res, ist, st, en);
      this.measurements.reportStatus("SCAN", res);
      HashMap<String, ArrayList<Double>> value = new HashMap<>();
      for (int i = 0; i < result.size(); i++) {
        String hashVal = ((ByteIterator) ((HashMap) result.get(i)).get("field0")).toString();
        String name = hashVal.split(":")[0];
        double val = Double.valueOf(hashVal.split(":")[2]).doubleValue();
        if (value.containsKey(name)) {
          ArrayList<Double> list = value.get(name);
          list.add(Double.valueOf(val));
        } else {
          ArrayList<Double> list = new ArrayList<>();
          list.add(Double.valueOf(val));
          value.put(name, list);
        }
        Iterator<String> it = value.keySet().iterator();
        Logger log = Logger.getGlobal();
        while (it.hasNext()) {
          String iotKey = it.next();
          List<Double> l = value.get(iotKey);
          double sum = 0.0D;
          for (int k = 0; k < l.size(); k++) {
            sum += ((Double) l.get(k)).doubleValue();
          }
          double avgVal = sum / l.size();
          System.out.println("Avg Value for " + iotKey + "=" + avgVal);
          it.remove();
        }
      }
      return res;
    }
  }

  public Status scan(String table, String key, String client, String timestamp, Set<String> fields,
      long runStartTime, Vector<HashMap<String, ByteIterator>> result1,
      Vector<HashMap<String, ByteIterator>> result2) {
    if (!isQuerying) {
      isQuerying = true;
      Thread thread = new Thread(() -> {
        try (TraceScope span = this.tracer.newScope(this.scopeStringScan)) {
          long ist = this.measurements.getIntendedtartTimeNs();
          long st = System.nanoTime();
          Status res = this.db
              .scan(table, key, client, timestamp, fields, runStartTime, result1, result2);
          long en = System.nanoTime();
          measure("SCAN", res, ist, st, en);
          this.measurements.reportStatus("SCAN", res);
          if (result1.size() <= 0 || result2.size() <= 0) {
            System.err.println(
                "Unable to get query results from database, please check the status of the table ");
          }
          isQuerying = false;
        }
      });
      thread.start();
    }
    return Status.OK;
  }

  private void measure(String op, Status result, long intendedStartTimeNanos, long startTimeNanos,
      long endTimeNanos) {
    String measurementName = op;
    if (result == null || !result.isOk()) {
      if (this.reportLatencyForEachError || this.latencyTrackedErrors
          .contains(result.getName())) {
        measurementName = op + "-" + result.getName();
      } else {
        measurementName = op + "-FAILED";
      }
    }
    this.measurements.measure(measurementName, (int) ((endTimeNanos - startTimeNanos) / 1000L));
    this.measurements
        .measureIntended(measurementName, (int) ((endTimeNanos - intendedStartTimeNanos) / 1000L));
  }

  public Status update(String table, String key, HashMap<String, ByteIterator> values) {
    try (TraceScope span = this.tracer.newScope(this.scopeStringUpdate)) {
      long ist = this.measurements.getIntendedtartTimeNs();
      long st = System.nanoTime();
      Status res = this.db.update(table, key, values);
      long en = System.nanoTime();
      measure("UPDATE", res, ist, st, en);
      this.measurements.reportStatus("UPDATE", res);
      return res;
    }
  }

  public Status insert(String table, String key, HashMap<String, ByteIterator> values) {
    try (TraceScope span = this.tracer.newScope(this.scopeStringInsert)) {
      long ist = this.measurements.getIntendedtartTimeNs();
      long st = System.nanoTime();
      Status res = this.db.insert(table, key, values);
      long en = System.nanoTime();
      measure("INSERT", res, ist, st, en);
      this.measurements.reportStatus("INSERT", res);
      return res;
    }
  }

  public Status delete(String table, String key) {
    try (TraceScope span = this.tracer.newScope(this.scopeStringDelete)) {
      long ist = this.measurements.getIntendedtartTimeNs();
      long st = System.nanoTime();
      Status res = this.db.delete(table, key);
      long en = System.nanoTime();
      measure("DELETE", res, ist, st, en);
      this.measurements.reportStatus("DELETE", res);
      return res;
    }
  }
}
