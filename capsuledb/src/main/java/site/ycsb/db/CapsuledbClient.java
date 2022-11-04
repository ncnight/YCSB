package site.ycsb.db;


import site.ycsb.DB;

/**
 * Client to access CapsuleDB. 
 * 
 */
public class CapsuledbClient extends DB {
    
    private static final Object INIT_COORDINATOR = new Object();
    
    private static CapsuleDB capsuledb;



    @Override
    public void init() throws DBException {
        // Quick check to see if we need to make a new instance
        if (capsuledb != null) {
            return;
        }
        
        synchronized(INIT_COORDINATOR) {
            // Synchronized null check
            if (capsuledb != null) {
                return;
            }
            CapsuledbClient.capsuledb = new CapsuleDB();

        }
    }

    //Read a single record
    @Override
    public int read(String table, String key, Set<String> fields, HashMap<String,String> result) {
        return -1;
    }

    //Perform a range scan
    @Override
    public int scan(String table, String startkey, int recordcount, Set<String> fields, Vector<HashMap<String,String>> result) {
        return -1;
    }
        
    //Update a single record
    @Override
    public int update(String table, String key, HashMap<String,String> values) {
        return -1;
    }

    //Insert a single record
    @Override
    public int insert(String table, String key, HashMap<String,String> values) {
        return -1;
    }

    //Delete a single record
    @Override
    public int delete(String table, String key) {
        return -1;
    }

}

public class CapsuleDB {
    public void init() {

    }

    // public void write()
}