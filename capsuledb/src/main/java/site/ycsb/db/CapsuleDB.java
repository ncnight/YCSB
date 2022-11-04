package site.ycsb.db;

import java.lang.ProcessBuilder;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.lang.Process;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.lang.StringBuilder;

public class CapsuleDB {

    private Process db_process;
    private BufferedInputStream db_output;
    private BufferedOutputStream db_input;

    public void init() throws Exception {
        System.out.println("Booting up CapsuleDB...");
        // TODO Make these args?
        ProcessBuilder db_builder = new ProcessBuilder("/home/gdpmobile7/CapsuleDB-nithin/YCSB/capsuledb/bin/cdbhost",
                "/home/gdpmobile7/CapsuleDB-nithin/YCSB/capsuledb/bin/cdbenc.signed",
                "/home/gdpmobile7/CapsuleDB-nithin/YCSB/capsuledb/bin/test_config.ini");
        db_process = db_builder.start();
        InputStream _db_output = db_process.getInputStream();
        OutputStream _db_input = db_process.getOutputStream();
        db_output = new BufferedInputStream(_db_output);
        db_input = new BufferedOutputStream(_db_input);
        char c = 0;
        // Ensure db is ready for input
        while (c == 0 || c != '\n') {
            c = (char) db_output.read();
        }
    }

    public void write(String key, String value) throws Exception {
        String write_cmd = "WRITE " + key + " " + value + "\n";
        byte[] write_cmd_bytes = write_cmd.getBytes();
        db_input.write(write_cmd_bytes);
        db_input.flush();
        ArrayList<Character> buf = new ArrayList<>();
        char c = 0;
        while (c != '\n') {
            c = (char) db_output.read();
            buf.add(c);
        }
    }

    public String read(String key) throws Exception {
        String read_cmd = "READ " + key + "\n";
        db_input.write(read_cmd.getBytes());
        db_input.flush();
        ArrayList<Character> buf = new ArrayList<>();
        char c = 0;
        while (c != '\n') {
            c = (char) db_output.read();
            buf.add(c);
        }
        return getStringRepresentation(buf);
    }

    String getStringRepresentation(ArrayList<Character> list) {
        StringBuilder builder = new StringBuilder(list.size());
        for (Character ch : list) {
            builder.append(ch);
        }
        return builder.toString();
    }

    public void close() throws Exception {
        db_input.close();
        db_output.close();
        db_process.destroy();
    }

}
