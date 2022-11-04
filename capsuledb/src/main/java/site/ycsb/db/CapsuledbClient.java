package site.ycsb.db;

// import site.ycsb.DB;
import java.util.Vector;
import java.util.HashMap;
import java.util.Set;

class Driver {
    public static void main(String[] args) {
        System.out.println("Test");
    }
}

/**
 * Client to access CapsuleDB.
 * 
 */
// public class CapsuledbClient extends DB {
public class CapsuledbClient {
    private static final Object INIT_COORDINATOR = new Object();

    private static CapsuleDB capsuledb;

    private static final String CAPSULEDB_BINARY = "";

    private static final String CAPSULEDB_SIG = "";

    private static final String CONFIG_F = "";

    // @Override
    // public void init() throws DBException {
    public void init() {
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
            CapsuledbClient.capsuledb = new CapsuleDB();

        }
    }

    // Read a single record
    // @Override
    public int read(String table, String key, Set<String> fields, HashMap<String, String> result) {
        return -1;
    }

    // Perform a range scan
    // @Override
    public int scan(String table, String startkey, int recordcount, Set<String> fields,
            Vector<HashMap<String, String>> result) {
        return -1;
    }

    // Update a single record
    // @Override
    public int update(String table, String key, HashMap<String, String> values) {
        return -1;
    }

    // Insert a single record
    // @Override
    public int insert(String table, String key, HashMap<String, String> values) {
        return -1;
    }

    // Delete a single record
    // @Override
    public int delete(String table, String key) {
        return -1;
    }

}