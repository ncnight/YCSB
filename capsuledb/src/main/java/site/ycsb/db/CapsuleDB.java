package site.ycsb.db;

import java.io.IOException;
import edu.berkeley.eecs.gdp.Client;
import edu.berkeley.eecs.gdp.KV_Status;
import com.google.protobuf.ByteString;
import site.ycsb.ByteIterator;
import java.util.ArrayList;
import org.javatuples.Pair;
import org.javatuples.Triplet;



/**
 * javadoc.
 */
public class CapsuleDB {

  private Client client;

  public void init(String capsuledbBinary, String capsuledbSig, String config) throws IOException {
    System.out.println("Booting up CapsuleDB...");

    this.client = new Client("client-2", "cdb", "127.0.0.1:2501");
    this.client.Init();

    this.client.Read(ByteString.copyFromUtf8("garbage"));
    this.client.Read(ByteString.copyFromUtf8("garbage"));
    this.client.Write(ByteString.copyFromUtf8("garbage"), ByteString.copyFromUtf8("garbage"));
    System.out.println("Finished init.");
  }

  public void write(String key, ByteIterator value) throws IOException {
    byte[] buf = new byte[2000]; //ByteBuffer.allocate(200);
    int len = 0;
    while (len < 200 && value.hasNext()){
      Byte b = value.nextByte();
      // System.out.print(b);
      buf[len] = b;
      len++;
    }
    byte[] bbuf = new byte[len];
    for (int i = 0; i < len; i++){
      bbuf[i] = buf[i];
    }
    ByteString bvalue = ByteString.copyFrom(bbuf);
    System.out.println("WRITE_REQ: " + ByteString.copyFromUtf8(key) + " " + bvalue);
    
    Triplet<KV_Status, ByteString, ByteString> ans = client.Write(ByteString.copyFromUtf8(key), bvalue);

    if (ans.getValue0() == KV_Status.WRITE_PASS){
      System.out.println("WRITE_PASS: " + ans.getValue1() + " " + ans.getValue2());
    }else{
      System.out.println("WRITE_FAIL");
    }
  }

  private void clearOutput() throws IOException {
    // char c = 0;
    // while (c == 0 || c != '\n') {
    //   c = (char) dbOutput.read();
    // }
  }

  public ArrayList<Byte> read(String key) throws IOException {
    System.out.println("READ_REQ: " + ByteString.copyFromUtf8(key));
    Pair<KV_Status, ByteString> resp = client.Read(ByteString.copyFromUtf8(key));
    if (resp.getValue0() == KV_Status.READ_PASS){
      ByteString val = resp.getValue1();
      ArrayList<Byte> result = new ArrayList<>();
      for (Byte b: val){
        result.add(b);
      }
      System.out.println("READ_PASS: " + resp.getValue1());
      return result;
    }else{
      System.out.println("READ_FAIL");
      return null;
    }
  }

  String getStringRepresentation(ArrayList<Character> list) {
    StringBuilder builder = new StringBuilder(list.size());
    for (Character ch : list) {
      builder.append(ch);
    }
    return builder.toString();
  }

  public void close() throws IOException {
    // dbInput.close();
    // dbOutput.close();
    // dbProcess.destroy();
    client.Close();
  }

}
