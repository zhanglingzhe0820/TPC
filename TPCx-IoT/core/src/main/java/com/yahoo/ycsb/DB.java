package com.yahoo.ycsb;

import java.util.HashMap;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;

public abstract class DB {
  private Properties properties = new Properties();

  public void setProperties(Properties p) {
    this.properties = p;
  }

  public Properties getProperties() {
    return this.properties;
  }

  public void init() throws DBException {}

  public void cleanup() throws DBException {}

  public abstract Status read(String paramString1, String paramString2, Set<String> paramSet, HashMap<String, ByteIterator> paramHashMap);

  public abstract Status scan(String paramString1, String paramString2, int paramInt, Set<String> paramSet, Vector<HashMap<String, ByteIterator>> paramVector);

  public abstract Status scan(String paramString1, String paramString2, String paramString3, String paramString4, Set<String> paramSet, long paramLong, Vector<HashMap<String, ByteIterator>> paramVector1, Vector<HashMap<String, ByteIterator>> paramVector2);

  public abstract Status update(String paramString1, String paramString2, HashMap<String, ByteIterator> paramHashMap);

  public abstract Status insert(String paramString1, String paramString2, HashMap<String, ByteIterator> paramHashMap);

  public abstract Status delete(String paramString1, String paramString2);
}
