package com.yahoo.ycsb.workloads;

import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.RandomByteIterator;
import com.yahoo.ycsb.Status;
import com.yahoo.ycsb.StringByteIterator;
import com.yahoo.ycsb.Workload;
import com.yahoo.ycsb.WorkloadException;
import com.yahoo.ycsb.generator.AcknowledgedCounterGenerator;
import com.yahoo.ycsb.generator.ConstantIntegerGenerator;
import com.yahoo.ycsb.generator.CounterGenerator;
import com.yahoo.ycsb.generator.DiscreteGenerator;
import com.yahoo.ycsb.generator.ExponentialGenerator;
import com.yahoo.ycsb.generator.HistogramGenerator;
import com.yahoo.ycsb.generator.NumberGenerator;
import com.yahoo.ycsb.generator.UniformIntegerGenerator;
import com.yahoo.ycsb.generator.UniformLongGenerator;
import com.yahoo.ycsb.generator.UnixEpochTimestampGenerator;
import com.yahoo.ycsb.generator.ZipfianGenerator;
import com.yahoo.ycsb.measurements.Measurements;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.Vector;
import java.util.concurrent.TimeUnit;

public class CoreWorkload extends Workload {
  public static final String TABLENAME_PROPERTY = "table";

  public static final String TABLENAME_PROPERTY_DEFAULT = "usertable";

  protected String table;

  public static final String FIELD_COUNT_PROPERTY = "fieldcount";

  public static final String FIELD_COUNT_PROPERTY_DEFAULT = "1";

  protected String client;

  protected String runType;

  long runStartTime;

  public static final String DEFAULT_RUN_TYPE = "run";

  public static final String DEFAULT_CLIENT_NAME = "client1";

  protected int fieldcount;

  public static String[] prekeys = new String[] {
      "cent_9_Humidity", "side_8_Humidity", "side_7_Humidity", "ang_30_Humidity", "ang_45_Humidity", "ang_60_Humidity", "ang_90_Humidity", "bef_1195_Humidity", "aft_1120_Humidity", "mid_1125_Humidity",
      "cor_4_Humidity", "cor_1_Humidity", "cor_5_Humidity", "cent_9_Power", "side_8_Power", "side_7_Power", "ang_30_Power", "ang_45_Power", "ang_60_Power", "ang_90_Power",
      "bef_1195_Power", "aft_1120_Power", "mid_1125_Power", "cor_4_Power", "cor_1_Power", "cor_5_Power", "cent_9_Pressure", "side_8_Pressure", "side_7_Pressure", "ang_30_Pressure",
      "ang_45_Pressure", "ang_60_Pressure", "ang_90_Pressure", "bef_1195_Pressure", "aft_1120_Pressure", "mid_1125_Pressure", "cor_4_Pressure", "cor_1_Pressure", "cor_5_Pressure", "cent_9_Flow",
      "side_8_Flow", "side_7_Flow", "ang_30_Flow", "ang_45_Flow", "ang_60_Flow", "ang_90_Flow", "bef_1195_Flow", "aft_1120_Flow", "mid_1125_Flow", "cor_4_Flow",
      "cor_1_Flow", "cor_5_Flow", "cent_9_Level", "side_8_Level", "side_7_Level", "ang_30_Level", "ang_45_Level", "ang_60_Level", "ang_90_Level", "bef_1195_Level",
      "aft_1120_Level", "mid_1125_Level", "cor_4_Level", "cor_1_Level", "cor_5_Level", "cent_9_Temperature", "side_8_Temperature", "side_7_Temperature", "ang_30_Temperature", "ang_45_Temperature",
      "ang_60_Temperature", "ang_90_Temperature", "bef_1195_Temperature", "aft_1120_Temperature", "mid_1125_Temperature", "cor_4_Temperature", "cor_1_Temperature", "cor_5_Temperature", "cent_9_vibration", "side_8_vibration",
      "side_7_vibration", "ang_30_vibration", "ang_45_vibration", "ang_60_vibration", "ang_90_vibration", "bef_1195_vibration", "aft_1120_vibration", "mid_1125_vibration", "cor_4_vibration", "cor_1_vibration",
      "cor_5_vibration", "cent_9_tilt", "side_8_tilt", "side_7_tilt", "ang_30_tilt", "ang_45_tilt", "ang_60_tilt", "ang_90_tilt", "bef_1195_tilt", "aft_1120_tilt",
      "mid_1125_tilt", "cor_4_tilt", "cor_1_tilt", "cor_5_tilt", "cent_9_level", "side_8_level", "side_7_level", "ang_30_level", "ang_45_level", "ang_60_level",
      "ang_90_level", "bef_1195_level", "aft_1120_level", "mid_1125_level", "cor_4_level", "cor_1_level", "cor_5_level", "cent_9_level_vibrating", "side_8_level_vibrating", "side_7_level_vibrating",
      "ang_30_level_vibrating", "ang_45_level_vibrating", "ang_60_level_vibrating", "ang_90_level_vibrating", "bef_1195_level_vibrating", "aft_1120_level_vibrating", "mid_1125_level_vibrating", "cor_4_level_vibrating", "cor_1_level_vibrating", "cor_5_level_vibrating",
      "cent_9_level_rotating", "side_8_level_rotating", "side_7_level_rotating", "ang_30_level_rotating", "ang_45_level_rotating", "ang_60_level_rotating", "ang_90_level_rotating", "bef_1195_level_rotating", "aft_1120_level_rotating", "mid_1125_level_rotating",
      "cor_4_level_rotating", "cor_1_level_rotating", "cor_5_level_rotating", "cent_9_level_admittance", "side_8_level_admittance", "side_7_level_admittance", "ang_30_level_admittance", "ang_45_level_admittance", "ang_60_level_admittance", "ang_90_level_admittance",
      "bef_1195_level_admittance", "aft_1120_level_admittance", "mid_1125_level_admittance", "cor_4_level_admittance", "cor_1_level_admittance", "cor_5_level_admittance", "cent_9_Pneumatic_level", "side_8_Pneumatic_level", "side_7_Pneumatic_level", "ang_30_Pneumatic_level" };

//  public static Map<String,String>

  private List<String> fieldnames;

  public static final String FIELD_LENGTH_DISTRIBUTION_PROPERTY = "fieldlengthdistribution";

  public static final String CLIENT_NAME = "client";

  public static final String RUN_TYPE = "runtype";

  public static final String FIELD_LENGTH_DISTRIBUTION_PROPERTY_DEFAULT = "constant";

  public static final String FIELD_LENGTH_PROPERTY = "fieldlength";

  public static final String FIELD_LENGTH_PROPERTY_DEFAULT = "1000";

  public static final String FIELD_LENGTH_HISTOGRAM_FILE_PROPERTY = "fieldlengthhistogram";

  public static final String FIELD_LENGTH_HISTOGRAM_FILE_PROPERTY_DEFAULT = "hist.txt";

  protected NumberGenerator fieldlengthgenerator;

  public static final String READ_ALL_FIELDS_PROPERTY = "readallfields";

  public static final String READ_ALL_FIELDS_PROPERTY_DEFAULT = "true";

  protected boolean readallfields;

  public static final String WRITE_ALL_FIELDS_PROPERTY = "writeallfields";

  public static final String WRITE_ALL_FIELDS_PROPERTY_DEFAULT = "false";

  protected boolean writeallfields;

  public static final String DATA_INTEGRITY_PROPERTY = "dataintegrity";

  public static final String DATA_INTEGRITY_PROPERTY_DEFAULT = "true";

  private boolean dataintegrity;

  public static final String READ_PROPORTION_PROPERTY = "readproportion";

  public static final String READ_PROPORTION_PROPERTY_DEFAULT = "0.00";

  public static final String UPDATE_PROPORTION_PROPERTY = "updateproportion";

  public static final String UPDATE_PROPORTION_PROPERTY_DEFAULT = "0.05";

  public static final String INSERT_PROPORTION_PROPERTY = "insertproportion";

  public static final String INSERT_PROPORTION_PROPERTY_DEFAULT = "1.0";

  public static final String SCAN_PROPORTION_PROPERTY = "scanproportion";

  public static final String SCAN_PROPORTION_PROPERTY_DEFAULT = "0.00005";

  public static final String READMODIFYWRITE_PROPORTION_PROPERTY = "readmodifywriteproportion";

  public static final String READMODIFYWRITE_PROPORTION_PROPERTY_DEFAULT = "0.0";

  public static final String REQUEST_DISTRIBUTION_PROPERTY = "requestdistribution";

  public static final String REQUEST_DISTRIBUTION_PROPERTY_DEFAULT = "uniform";

  public static final String ZERO_PADDING_PROPERTY = "zeropadding";

  public static final String ZERO_PADDING_PROPERTY_DEFAULT = "1";

  public static final String MAX_SCAN_LENGTH_PROPERTY = "maxscanlength";

  public static final String MAX_SCAN_LENGTH_PROPERTY_DEFAULT = "100";

  public static final String SCAN_LENGTH_DISTRIBUTION_PROPERTY = "scanlengthdistribution";

  public static final String SCAN_LENGTH_DISTRIBUTION_PROPERTY_DEFAULT = "uniform";

  public static final String INSERT_ORDER_PROPERTY = "insertorder";

  public static final String INSERT_ORDER_PROPERTY_DEFAULT = "hashed";

  public static final String HOTSPOT_DATA_FRACTION = "hotspotdatafraction";

  public static final String HOTSPOT_DATA_FRACTION_DEFAULT = "0.2";

  public static final String HOTSPOT_OPN_FRACTION = "hotspotopnfraction";

  public static final String HOTSPOT_OPN_FRACTION_DEFAULT = "0.8";

  public static final String INSERTION_RETRY_LIMIT = "core_workload_insertion_retry_limit";

  public static final String INSERTION_RETRY_LIMIT_DEFAULT = "0";

  public static final String INSERTION_RETRY_INTERVAL = "core_workload_insertion_retry_interval";

  public static final String INSERTION_RETRY_INTERVAL_DEFAULT = "3";

  protected NumberGenerator keysequence;

  protected DiscreteGenerator operationchooser;

  protected NumberGenerator keychooser;

  protected NumberGenerator writeKeyChooser = (NumberGenerator)new UniformIntegerGenerator(0, this.prekeys.length - 1);

  protected NumberGenerator readKeyChooser = (NumberGenerator)new UniformIntegerGenerator(0, this.prekeys.length - 1);

  protected NumberGenerator fieldchooser;

  protected AcknowledgedCounterGenerator transactioninsertkeysequence;

  protected NumberGenerator scanlength;

  protected boolean orderedinserts;

  protected long recordcount;

  protected int zeropadding;

  protected int insertionRetryLimit;

  protected int insertionRetryInterval;

  protected UnixEpochTimestampGenerator tt;

  private Measurements measurements = Measurements.getMeasurements();

  protected static NumberGenerator getFieldLengthGenerator(Properties p) throws WorkloadException {
    HistogramGenerator histogramGenerator;
    String fieldlengthdistribution = p.getProperty("fieldlengthdistribution", "constant");
    int fieldlength = Integer.parseInt(p.getProperty("fieldlength", "1000"));
    String fieldlengthhistogram = p.getProperty("fieldlengthhistogram", "hist.txt");
    if (fieldlengthdistribution.compareTo("constant") == 0) {
      return new ConstantIntegerGenerator(fieldlength);
    } else if (fieldlengthdistribution.compareTo("uniform") == 0) {
      return new UniformIntegerGenerator(1, fieldlength);
    } else if (fieldlengthdistribution.compareTo("zipfian") == 0) {
      return new ZipfianGenerator(1L, fieldlength);
    } else if (fieldlengthdistribution.compareTo("histogram") == 0) {
      try {
        histogramGenerator = new HistogramGenerator(fieldlengthhistogram);
      } catch (IOException e) {
        throw new WorkloadException("Couldn't read field length histogram file: " + fieldlengthhistogram, e);
      }
    } else {
      throw new WorkloadException("Unknown field length distribution \"" + fieldlengthdistribution + "\"");
    }
    return histogramGenerator;
  }

  public void init(Properties p) throws WorkloadException {
    this.table = p.getProperty("table", "usertable");
    this.client = p.getProperty("client", "client1");
    this.runType = p.getProperty("runtype", "run");
    System.out.println("Run Type = " + this.runType);
    this
        .fieldcount = Integer.parseInt(p.getProperty("fieldcount", "1"));
    this.fieldnames = new ArrayList<>();
    for (int i = 0; i < this.fieldcount; i++)
      this.fieldnames.add("field" + i);
    this.fieldlengthgenerator = getFieldLengthGenerator(p);
    this
        .recordcount = Long.parseLong(p.getProperty("recordcount", "0"));
    if (this.recordcount == 0L)
      this.recordcount = Long.MAX_VALUE;
    String requestdistrib = p.getProperty("requestdistribution", "uniform");
    int maxscanlength = Integer.parseInt(p.getProperty("maxscanlength", "100"));
    String scanlengthdistrib = p.getProperty("scanlengthdistribution", "uniform");
    long insertstart = Long.parseLong(p.getProperty("insertstart", "0"));
    long insertcount = Long.parseLong(p.getProperty("insertcount", String.valueOf(this.recordcount - insertstart)));
    if (this.recordcount < insertstart + insertcount) {
      System.err.println("Invalid combination of insertstart, insertcount and recordcount.");
      System.err.println("recordcount must be bigger than insertstart + insertcount.");
      System.exit(-1);
    }
    this
        .zeropadding = Integer.parseInt(p.getProperty("zeropadding", "1"));
    this.readallfields = Boolean.parseBoolean(p
        .getProperty("readallfields", "true"));
    this.writeallfields = Boolean.parseBoolean(p
        .getProperty("writeallfields", "false"));
    this.dataintegrity = Boolean.parseBoolean(p
        .getProperty("dataintegrity", "true"));
    if (this.dataintegrity &&

        !p.getProperty("fieldlengthdistribution", "constant").equals("constant")) {
      System.err.println("Must have constant field size to check data integrity.");
      System.exit(-1);
    }
    if (p.getProperty("insertorder", "hashed").compareTo("hashed") == 0) {
      this.orderedinserts = false;
    } else if (requestdistrib.compareTo("exponential") == 0) {
      double percentile = Double.parseDouble(p.getProperty("exponential.percentile", "95"));
      double frac = Double.parseDouble(p.getProperty("exponential.frac", "0.8571428571"));
      this.keychooser = (NumberGenerator)new ExponentialGenerator(percentile, this.recordcount * frac);
    } else {
      this.orderedinserts = true;
    }
    this.keysequence = (NumberGenerator)new CounterGenerator(insertstart);
    this.operationchooser = createOperationGenerator(p);
    this.transactioninsertkeysequence = new AcknowledgedCounterGenerator(this.recordcount);
    this.tt = new UnixEpochTimestampGenerator(100L, TimeUnit.MILLISECONDS, System.currentTimeMillis());
    if (requestdistrib.compareTo("uniform") == 0) {
      this.keychooser = (NumberGenerator)new UniformLongGenerator(insertstart, insertstart + insertcount - 1L);
    } else {
      throw new WorkloadException("Unknown request distribution \"" + requestdistrib + "\"");
    }
    this.fieldchooser = (NumberGenerator)new UniformIntegerGenerator(0, this.fieldcount - 1);
    if (scanlengthdistrib.compareTo("uniform") == 0) {
      this.scanlength = (NumberGenerator)new UniformIntegerGenerator(1, maxscanlength);
    } else if (scanlengthdistrib.compareTo("zipfian") == 0) {
      this.scanlength = (NumberGenerator)new ZipfianGenerator(1L, maxscanlength);
    } else {
      throw new WorkloadException("Distribution \"" + scanlengthdistrib + "\" not allowed for scan length");
    }
    this.insertionRetryLimit = Integer.parseInt(p.getProperty("core_workload_insertion_retry_limit", "0"));
    this.insertionRetryInterval = Integer.parseInt(p.getProperty("core_workload_insertion_retry_interval", "3"));
    this.runStartTime = System.currentTimeMillis();
  }

  protected String buildKeyName(long keynum) {
    int index = ((Number)this.writeKeyChooser.nextValue()).intValue();
    String prekey = this.prekeys[index];
    String key = this.client + ":" + prekey + ":" + keynum;
    try {
      wait(1000L);
    } catch (Exception exception) {}
    return key;
  }

  protected String buildKeyNameForRead(long keynum) {
    int index = ((Number)this.readKeyChooser.nextValue()).intValue();
    String prekey = this.prekeys[index];
    long t = this.tt.lastValue().longValue() - 5000L;
    return this.client + ":" + prekey + ":" + t;
  }

  private HashMap<String, ByteIterator> buildSingleValue(String key) {
    RandomByteIterator randomByteIterator;
    HashMap<String, ByteIterator> value = new HashMap<>();
    String fieldkey = this.fieldnames.get(((Number)this.fieldchooser.nextValue()).intValue());
    if (this.dataintegrity) {
      StringByteIterator stringByteIterator = new StringByteIterator(buildDeterministicValue(key, fieldkey));
      value.put(fieldkey, stringByteIterator);
    } else {
      randomByteIterator = new RandomByteIterator(((Number)this.fieldlengthgenerator.nextValue()).longValue());
      value.put(fieldkey, randomByteIterator);
    }
    return value;
  }

  private HashMap<String, ByteIterator> buildValues(String key) {
    HashMap<String, ByteIterator> values = new HashMap<>();
    for (String fieldkey : this.fieldnames) {
      RandomByteIterator randomByteIterator;
      if (this.dataintegrity) {
        StringByteIterator stringByteIterator = new StringByteIterator(buildDeterministicValue(key, fieldkey));
        values.put(fieldkey, stringByteIterator);
      } else {
        randomByteIterator = new RandomByteIterator(((Number)this.fieldlengthgenerator.nextValue()).longValue());
        values.put(fieldkey, randomByteIterator);
      }
    }
    return values;
  }

  private String buildDeterministicValue(String key, String fieldkey) {
    int size = ((Number)this.fieldlengthgenerator.nextValue()).intValue();
    String iotParameter = null;
    if (key.contains(":")) {
      iotParameter = key.split(":")[1];
    } else {
      iotParameter = key;
    }
    Random r = new Random();
    StringBuilder sb = new StringBuilder(size);
    sb.append(iotParameter);
    BigDecimal val = BigDecimal.valueOf(r.nextDouble()).setScale(4, RoundingMode.HALF_UP);
    while (sb.length() < size) {
      sb.append(':');
      sb.append(iotParameter);
      sb.append('_');
      sb.append("value");
      sb.append(':');
      sb.append(val);
      sb.append(':');
      sb.append("timestamp");
      sb.append(':');
      sb.append(System.currentTimeMillis());
      sb.append(":");
      sb.append(sb.toString().hashCode());
    }
    sb.setLength(size);
    return sb.toString();
  }

  public boolean doInsert(DB db, Object threadstate) {
    Status status;
    int keynum = ((Number)this.keysequence.nextValue()).intValue();
    String dbkey = buildKeyName(keynum);
    HashMap<String, ByteIterator> values = buildValues(dbkey);
    int numOfRetries = 0;
    while (true) {
      status = db.insert(this.table, dbkey, values);
      if (null != status && status.isOk())
        break;
      if (++numOfRetries <= this.insertionRetryLimit) {
        System.err.println("Retrying insertion, retry count: " + numOfRetries);
        try {
          int sleepTime = (int)((1000 * this.insertionRetryInterval) * (0.8D + 0.4D * Math.random()));
          Thread.sleep(sleepTime);
          continue;
        } catch (InterruptedException e) {
          break;
        }
      }
      System.err.println("Error inserting, not retrying any more. number of attempts: " + numOfRetries + "Insertion Retry Limit: " + this.insertionRetryLimit);
      break;
    }
    return (null != status && status.isOk());
  }

  public boolean doTransaction(DB db, Object threadstate) {
    String operation = this.operationchooser.nextString();
    if (operation == null)
      return false;
    switch (operation) {
      case "READ":
        doTransactionRead(db);
        return true;
      case "UPDATE":
        doTransactionUpdate(db);
        return true;
      case "INSERT":
        doTransactionInsert(db);
        return true;
      case "SCAN":
        doTransactionScanWithFilter(db, this.runStartTime);
        return true;
    }
    doTransactionReadModifyWrite(db);
    return true;
  }

  protected void verifyRow(String key, HashMap<String, ByteIterator> cells) {
    Status verifyStatus = Status.OK;
    long startTime = System.nanoTime();
    if (!cells.isEmpty()) {
      for (Map.Entry<String, ByteIterator> entry : cells.entrySet()) {
        if (!((ByteIterator)entry.getValue()).toString().equals(buildDeterministicValue(key, entry.getKey()))) {
          verifyStatus = Status.UNEXPECTED_STATE;
          break;
        }
      }
    } else {
      verifyStatus = Status.ERROR;
    }
    long endTime = System.nanoTime();
    this.measurements.measure("VERIFY", (int)(endTime - startTime) / 1000);
    this.measurements.reportStatus("VERIFY", verifyStatus);
  }

  protected long nextKeynum() {
    long keynum;
    if (this.keychooser instanceof ExponentialGenerator) {
      do {
        keynum = this.transactioninsertkeysequence.lastValue().longValue() - ((Number)this.keychooser.nextValue()).longValue();
      } while (keynum < 0L);
    } else {
      do {
        keynum = ((Number)this.keychooser.nextValue()).longValue();
      } while (keynum > this.transactioninsertkeysequence.lastValue().longValue());
    }
    return keynum;
  }

  public void doTransactionRead(DB db) {
    String startkeyname = buildKeyNameForRead(nextKeynum());
    int len = ((Number)this.scanlength.nextValue()).intValue();
    HashSet<String> fields = null;
    if (!this.readallfields) {
      String fieldname = this.fieldnames.get(((Number)this.fieldchooser.nextValue()).intValue());
      fields = new HashSet<>();
      fields.add(fieldname);
    }
    Vector<HashMap<String, ByteIterator>> results = new Vector<>();
    db.scan(this.table, startkeyname, len, fields, results);
  }

  public void doTransactionReadModifyWrite(DB db) {
    HashMap<String, ByteIterator> values;
    long keynum = nextKeynum();
    String keyname = buildKeyName(keynum);
    HashSet<String> fields = null;
    if (!this.readallfields) {
      String fieldname = this.fieldnames.get(((Number)this.fieldchooser.nextValue()).intValue());
      fields = new HashSet<>();
      fields.add(fieldname);
    }
    if (this.writeallfields) {
      values = buildValues(keyname);
    } else {
      values = buildSingleValue(keyname);
    }
    HashMap<String, ByteIterator> cells = new HashMap<>();
    long ist = this.measurements.getIntendedtartTimeNs();
    long st = System.nanoTime();
    db.read(this.table, keyname, fields, cells);
    db.update(this.table, keyname, values);
    long en = System.nanoTime();
    if (this.dataintegrity)
      verifyRow(keyname, cells);
    this.measurements.measure("READ-MODIFY-WRITE", (int)((en - st) / 1000L));
    this.measurements.measureIntended("READ-MODIFY-WRITE", (int)((en - ist) / 1000L));
  }

  public void doTransactionScan(DB db) {
    String startkeyname = buildKeyNameForRead(nextKeynum());
    int len = ((Number)this.scanlength.nextValue()).intValue();
    HashSet<String> fields = null;
    if (!this.readallfields) {
      String fieldname = this.fieldnames.get(((Number)this.fieldchooser.nextValue()).intValue());
      fields = new HashSet<>();
      fields.add(fieldname);
    }
    Vector<HashMap<String, ByteIterator>> results = new Vector<>();
    long t1 = System.currentTimeMillis();
    db.scan(this.table, startkeyname, len, fields, results);
    long t2 = System.currentTimeMillis();
  }

  public void doTransactionScanWithFilter(DB db, long runStartTime) {
    long keynum = nextKeynum();
    String startKeyName = buildKeyNameForRead(keynum);
    String client = startKeyName.split(":")[0];
    String filter = startKeyName.split(":")[1];
    String timestamp = startKeyName.split(":")[2];
    HashSet<String> fields = null;
    Vector<HashMap<String, ByteIterator>> results1 = new Vector<>();
    Vector<HashMap<String, ByteIterator>> results2 = new Vector<>();
    db.scan(this.table, filter, client, timestamp, fields, runStartTime, results1, results2);
  }

  public void doTransactionUpdate(DB db) {
    HashMap<String, ByteIterator> values;
    long keynum = nextKeynum();
    String keyname = buildKeyName(keynum);
    if (this.writeallfields) {
      values = buildValues(keyname);
    } else {
      values = buildSingleValue(keyname);
    }
    db.update(this.table, keyname, values);
  }

  public void doTransactionInsert(DB db) {
    long keynum = this.tt.nextValue().longValue();
    String dbkey = buildKeyName(keynum);
    HashMap<String, ByteIterator> values = buildValues(dbkey);
    db.insert(this.table, dbkey, values);
  }

  protected static DiscreteGenerator createOperationGenerator(Properties p) {
    if (p == null)
      throw new IllegalArgumentException("Properties object cannot be null");
    double insertproportion = Double.parseDouble(p
        .getProperty("insertproportion", "1.0"));
    double scanproportion = Double.parseDouble(p
        .getProperty("scanproportion", "0.00005"));
    DiscreteGenerator operationchooser = new DiscreteGenerator();
    if (insertproportion > 0.0D)
      operationchooser.addValue(insertproportion, "INSERT");
    if (scanproportion > 0.0D)
      operationchooser.addValue(scanproportion, "SCAN");
    return operationchooser;
  }
}
