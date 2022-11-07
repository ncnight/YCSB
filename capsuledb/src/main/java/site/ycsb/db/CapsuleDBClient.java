package site.ycsb.db;

import site.ycsb.ByteIterator;
import site.ycsb.DB;
import site.ycsb.DBException;
import site.ycsb.Status;
import site.ycsb.ByteArrayByteIterator;

import java.util.Vector;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.Map;

/**
 * Client to access CapsuleDB. NOTE: The table name is not used in any methods. ./bin/ycsb load
 * capsuledb -s -P workloads/workloada.
 * 
 */
public class CapsuleDBClient extends DB {
  // public class CapsuledbClient {
  private static final Object INIT_COORDINATOR = new Object();

  private static CapsuleDB capsuledb;

  private static final String CAPSULEDB_BINARY = "/home/gdpmobile7/CapsuleDB-nithin/YCSB/capsuledb/bin/cdbhost";

  private static final String CAPSULEDB_SIG = "/home/gdpmobile7/CapsuleDB-nithin/YCSB/capsuledb/bin/cdbenc.signed";

  private static final String CONFIG_F = "/home/gdpmobile7/CapsuleDB-nithin/YCSB/capsuledb/bin/test_config.ini";

  private static final char FIELD_DELIM = '|';

  @Override
  public void init() throws DBException {
    // public void init() {
    System.out.println("Got to init client");
    // Quick check to see if we need to make a new instance
    if (capsuledb != null) {
      return;
    }

    synchronized (INIT_COORDINATOR) {
      // Synchronized null check
      if (capsuledb != null) {
        return;
      }
      CapsuleDBClient.capsuledb = new CapsuleDB();
      try {
        CapsuleDBClient.capsuledb.init(CAPSULEDB_BINARY, CAPSULEDB_SIG, CONFIG_F);
      } catch (IOException e) {
        System.err.println("Unable to load CapsuleDB.");
        e.printStackTrace();
        System.exit(-1);
      }
      System.out.println("Loaded DB!");

    }
  }

  @Override
  public void cleanup() {
    synchronized (CapsuleDBClient.capsuledb) {
      try {
        CapsuleDBClient.capsuledb.close();
      } catch (IOException e) {
        System.err.println("Error closing CapsuleDB.");
        e.printStackTrace();
      }
    }
  }

  private byte[] serialize(Map<String, ByteIterator> fields) {
    // final ObjectMapper objectMapper = new ObjectMapper();
    // final ObjectWriter objectWriter = objectMapper.writer();
    return null;
  }

  private void deserialize(byte[] rawData, Map<String, ByteIterator> result) {
    return;
  }

  private Status readFields(String key, Set<String> result) {
    ArrayList<Byte> fieldsBytes;
    try {
      fieldsBytes = CapsuleDBClient.capsuledb.read(key);
      if (fieldsBytes == null) {
        return Status.NOT_FOUND;
      }
    } catch (IOException e) {
      e.printStackTrace();
      return Status.ERROR;
    }
    StringBuilder fieldsBuilder = new StringBuilder();
    for (Byte b : fieldsBytes) {
      fieldsBuilder.append((char) (byte) b);
    }
    String[] splitFields = fieldsBuilder.toString().split("\\|");
    for (int i = 0; i < splitFields.length; i++) {
      result.add(splitFields[i]);
    }
    return Status.OK;
  }

  private Status dumpFields(String key, Set<String> fieldNames) {
    StringBuilder fields = new StringBuilder();
    for (String fieldName : fieldNames) {
      fields.append(fieldName);
      fields.append("|");
    }
    char[] finalFields = fields.toString().toCharArray();
    byte[] finalFieldsBytes = new byte[finalFields.length];
    for (int i = 0; i < finalFieldsBytes.length; i++) {
      finalFieldsBytes[i] = (byte) finalFields[i];
    }
    try {
      CapsuleDBClient.capsuledb.write(key, new ByteArrayByteIterator(finalFieldsBytes));
    } catch (IOException e) {
      e.printStackTrace();
      return Status.ERROR;
    }
    return Status.OK;
  }

  // Read a single record
  @Override
  public Status read(String table, String key, Set<String> providedfields, Map<String, ByteIterator> result) {
    synchronized (CapsuleDBClient.capsuledb) {
      // Check if we have fields, otherwise read in field
      Set<String> fields = providedfields;
      if (fields == null || fields.size() == 0) {
        fields = new HashSet<String>();
        Status readFieldStatus = this.readFields(key, fields);
        if (readFieldStatus != Status.OK) {
          return readFieldStatus;
        }
      }
      String readKey = null;
      ArrayList<Byte> outputBuf = null;
      byte[] outputBufRaw = null;
      for (String fieldName : fields) {
        readKey = key + ":" + fieldName;
        try {
          outputBuf = CapsuleDBClient.capsuledb.read(readKey);
          if (outputBuf == null) {
            return Status.NOT_FOUND;
          }
        } catch (IOException e) {
          return Status.ERROR;
        }
        outputBufRaw = new byte[outputBuf.size()];
        for (int i = 0; i < outputBuf.size(); i++) {
          outputBufRaw[i] = outputBuf.get(i);
        }
        result.put(fieldName, new ByteArrayByteIterator(outputBufRaw));
      }
    }
    return Status.OK;
  }

  // Perform a range scan
  @Override
  public Status scan(String table, String startkey, int recordcount, Set<String> fields,
      Vector<HashMap<String, ByteIterator>> result) {
    return null;
  }

  // Update a single record
  @Override
  public Status update(String table, String key, Map<String, ByteIterator> values) {
    synchronized (CapsuleDBClient.capsuledb) {
      String writeKey = null;
      for (String fieldName : values.keySet()) {
        writeKey = key + ":" + fieldName;
        try {
          CapsuleDBClient.capsuledb.write(writeKey, values.get(fieldName));
        } catch (IOException e) {
          return Status.ERROR;
        }
        Set<String> currentFields = new HashSet<String>();
        if (this.readFields(key, currentFields) != Status.OK) {
          return Status.ERROR;
        }
        for (String k : values.keySet()) {
          currentFields.add(k);
        }
        if (this.dumpFields(key, currentFields) != Status.OK) {
          return Status.ERROR;
        }
      }
    }
    return Status.OK;
  }

  // Insert a single record
  @Override
  public Status insert(String table, String key, Map<String, ByteIterator> values) {
    synchronized (CapsuleDBClient.capsuledb) {
      String writeKey = null;
      for (String fieldName : values.keySet()) {
        writeKey = key + ":" + fieldName;
        try {
          CapsuleDBClient.capsuledb.write(writeKey, values.get(fieldName));
        } catch (IOException e) {
          return Status.ERROR;
        }
        if (this.dumpFields(key, values.keySet()) != Status.OK) {
          return Status.ERROR;
        }
      }
    }
    return Status.OK;
  }

  // Delete a single record
  @Override
  public Status delete(String table, String key) {
    return Status.OK;
  }
}
