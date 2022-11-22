package site.ycsb.db;

// import java.lang.ProcessBuilder;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
// import java.lang.Process;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import site.ycsb.ByteIterator;
// import java.lang.StringBuilder;
import java.io.IOException;

/**
 * javadoc.
 */
public class CapsuleDB {

  private Process dbProcess;
  private BufferedInputStream dbOutput;
  private BufferedOutputStream dbInput;

  public void init(String capsuledbBinary, String capsuledbSig, String config) throws IOException {
    System.out.println("Booting up CapsuleDB...");
    ProcessBuilder dbBuilder = new ProcessBuilder(
        "/home/gdpmobile7/CapsuleDB-nithin/towncrier/bin/go_echo_client", 
        "client", "cdb", "127.0.0.1:2502"); 
    //new ProcessBuilder(capsuledbBinary, capsuledbSig, config);
    dbBuilder.redirectError(new File("capsuleDB.err"));
    dbProcess = dbBuilder.start();
    InputStream rawDbOutput = dbProcess.getInputStream();
    OutputStream rawDbInput = dbProcess.getOutputStream();
    dbOutput = new BufferedInputStream(rawDbOutput);
    dbInput = new BufferedOutputStream(rawDbInput);
    char c = 0;
    // Ensure db is ready for input
    while (c == 0 || c != '\n') {
      c = (char) dbOutput.read();
    }
    System.out.println("Finished init.");
  }

  public void write(String key, ByteIterator value) throws IOException {
    String writeCmd = "WRITE " + key + " ";
    byte[] writeCmdBytes = writeCmd.getBytes();
    dbInput.write(writeCmdBytes);
    while (value.hasNext()) {
      dbInput.write(value.nextByte());
    }
    dbInput.write((byte) '\n');
    dbInput.flush();
    ArrayList<Character> buf = new ArrayList<>();
    char c = 0;
    while (c != '\n') {
      c = (char) dbOutput.read();
      buf.add(c);
    }
  }

  private void clearOutput() throws IOException {
    char c = 0;
    while (c == 0 || c != '\n') {
      c = (char) dbOutput.read();
    }
  }

  public ArrayList<Byte> read(String key) throws IOException {
    String readCmd = "READ " + key + "\n";
    dbInput.write(readCmd.getBytes());
    dbInput.flush();
    ArrayList<Byte> buf = new ArrayList<>();
    byte c = 0;
    // test if read passed
    char[] passedMsg = "READ_PASS".toCharArray();
    byte[] outputMsg = dbOutput.readNBytes(passedMsg.length);
    boolean passed = true;
    for (int i = 0; i < passedMsg.length; i++) {
      passed = passed && (passedMsg[i] == (char) outputMsg[i]);
    }
    if (!passed) {
      this.clearOutput();
      return null;
    }
    // If it passed, need to read the two spacing bytes
    dbOutput.readNBytes(2);
    while ((char) c != '\n') {
      c = Integer.valueOf(dbOutput.read()).byteValue();
      if ((char) c != '\n') {
        buf.add(c);
      }
    }
    return buf;
  }

  String getStringRepresentation(ArrayList<Character> list) {
    StringBuilder builder = new StringBuilder(list.size());
    for (Character ch : list) {
      builder.append(ch);
    }
    return builder.toString();
  }

  public void close() throws IOException {
    dbInput.close();
    dbOutput.close();
    dbProcess.destroy();
  }

}
