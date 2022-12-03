package site.ycsb.db;

import site.ycsb.ByteIterator;
import site.ycsb.DB;
import site.ycsb.DBException;
import site.ycsb.Status;
import site.ycsb.ByteArrayByteIterator;

import java.util.Vector;
import java.io.IOException;
import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Set;
import java.util.Map;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Client to access CapsuleDB. NOTE: The table name is not used in any methods. ./bin/ycsb load
 * capsuledb -s -P workloads/workloada.
 * 
 */
public class CapsuleDBClient extends DB {

  private static int references = 0;

  private static CapsuleDB capsuledb;

  private static final String CAPSULEDB_BINARY = "/home/azureuser/YCSB/capsuledb/bin/cdbhos";

  private static final String CAPSULEDB_SIG = "/home/azureuser/YCSB/capsuledb/bin/cdbenc.signed";

  private static final String CONFIG_F = "/home/azureuser/YCSB/capsuledb/bin/test_config.ini";

  private static final char FIELD_DELIM = '|';

  @Override
  public void init() throws DBException {
    synchronized (CapsuleDBClient.class) {
      references++;
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
    }
  }

  @Override
  public void cleanup() {
    System.out.println("cleanup: try");
    synchronized (CapsuleDBClient.class) {
      if (references == 1) {
        try {
          CapsuleDBClient.capsuledb.close();
        } catch (IOException e) {
          System.err.println("cleanup: Error closing CapsuleDB.");
          e.printStackTrace();
        }
      }
      references--;
    }
  }

  private byte[] serializeValues(final Map<String, ByteIterator> values) throws IOException {
    //Follow Rocksdb serialization scheme
    try (final ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
      final ByteBuffer buf = ByteBuffer.allocate(4);

      for (final Map.Entry<String, ByteIterator> value : values.entrySet()) {
        final byte[] keyBytes = value.getKey().getBytes(UTF_8);
        final byte[] valueBytes = value.getValue().toArray();

        buf.putInt(keyBytes.length);
        baos.write(buf.array());
        baos.write(keyBytes);

        buf.clear();

        buf.putInt(valueBytes.length);
        baos.write(buf.array());
        baos.write(valueBytes);

        buf.clear();
      }
      return baos.toByteArray();
    }
  }

  private Map<String, ByteIterator> deserializeValues(final ArrayList<Byte> valuesList, final Set<String> fields,
      final Map<String, ByteIterator> result) {
    
    final byte[] values = new byte[valuesList.size()];
    for (int i = 0; i < valuesList.size(); i++) {
      values[i] = (byte) valuesList.get(i);
    }

    //Follow Rocksdb deserialization scheme
    final ByteBuffer buf = ByteBuffer.allocate(4);

    int offset = 0;
    while (offset < values.length) {
      buf.put(values, offset, 4);
      buf.flip();
      final int keyLen = buf.getInt();
      buf.clear();
      offset += 4;

      final String key = new String(values, offset, keyLen);
      offset += keyLen;

      buf.put(values, offset, 4);
      buf.flip();
      final int valueLen = buf.getInt();
      buf.clear();
      offset += 4;

      if (fields == null || fields.contains(key)) {
        result.put(key, new ByteArrayByteIterator(values, offset, valueLen));
      }

      offset += valueLen;
    }
    return result;
  }

  private Status readFields(String key, Set<String> result) {
    ArrayList<Byte> fieldsBytes;
    try {
      fieldsBytes = CapsuleDBClient.capsuledb.read(key);
      if (fieldsBytes == null) {
        System.out.println("Read NotFound");
        System.out.println(key);
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
    synchronized (CapsuleDBClient.class) {
      final ArrayList<Byte> readData;
      try {
        readData = CapsuleDBClient.capsuledb.read(key);
      } catch (IOException e) {
        return Status.ERROR;
      }

      if (readData == null) {
        return Status.NOT_FOUND;
      }

      deserializeValues(readData, providedfields, result);
      return Status.OK;
    }
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
    synchronized (CapsuleDBClient.class) {
      // Read existing data
      final Map<String, ByteIterator> existingData = new HashMap<>();
      final ArrayList<Byte> readResult;
      try {
        readResult = CapsuleDBClient.capsuledb.read(key);
      } catch (IOException e) {
        return Status.ERROR;
      }
      deserializeValues(readResult, null, existingData);

      // Add new updated values
      existingData.putAll(values);

      // Rewrite
      try {
        final ByteIterator serializedData = new ByteArrayByteIterator(serializeValues(existingData));
        CapsuleDBClient.capsuledb.write(key, serializedData);
      } catch (IOException e) {
        return Status.ERROR;
      }
      return Status.OK;
    }
  }

  // Insert a single record
  @Override
  public Status insert(String table, String key, Map<String, ByteIterator> values) {
    synchronized (CapsuleDBClient.class) {
      try {
        CapsuleDBClient.capsuledb.write(key, new ByteArrayByteIterator(serializeValues(values)));
      } catch (IOException e) {
        return Status.ERROR;
      }
      return Status.OK;
    }
  }

  // Delete a single record
  @Override
  public Status delete(String table, String key) {
    return Status.OK;
  }
}
