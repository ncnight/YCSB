package site.ycsb.db;

import site.ycsb.db.CapsuledbClient;
import site.ycsb.db.CapsuleDB;

public class driver {
    public static void main(String[] args) throws Exception {
        System.out.println("test");
        CapsuledbClient db_client = new CapsuledbClient();
        db_client.init();
        CapsuleDB db = new CapsuleDB();
        db.init();
        String[] w_cmd = { "59f4476cx7cd8x42",
                "rzmkwohgtayuwnkyfessevggcnowslmilaotifztmlbjegxlzgierekjtfxytzieqsljiryoxcdnwsqatghqewqrelmxiedshvpgtifouffevyikmwgxcdujnmkdyxpdxumkxvytdrlicshjevlfxpkpyeeddsnkicbdkcrsgplxeejitowgajxlsiktktmxnqamerqvjatthppctcvpizrjvhonysgyavpcsxlcrmkkuzhmcjbnztcthwrfyixhrxrdbwtjlziuwtjygsueukdpqaopnqbaactuegxoozwlofunxabmlbuzovwfatxrpfwwfquvqjhpvcjnyjeswlglzcwhuepksjrhyjrpqynkcfahwclomcqseavgtepmonsfuiifwxbilqeoqaotbufspyownoglzetegcixtqtbziyhxnubflkuclqhndohurpoqhkcavasfejgmgzyztjgtciuncvsiwfsyrehqwwnjoxcraaehcmxgdttywhvdpwtwasjocevxvtlxtijnctsepjwgkyjdunugzznrhjdomdkrgwxintafwxbbptyqjismnsltkcgazoiimkucnkxztqhpowqupnimgxrwhlwaylphvdzjqwanifgzttapeufdiksdbuzihpasagewgngbqmdjlmjobanpyijgmrcfxbfcxgnzolhlxeuoxyeiyooddligbyabfrukggxkpocludneexpdnshbukspfngshwoinyipwirgobfbjwlblyruomxlpgyyzcyyfdufnnenamnnoneqebtyhutecqnpwaqckvthkngyjducxhlfbmgyiszemtgcvuaglelcrcluqxsfnldfumczmqpvvtekutzhzwndcnzpcpywyknbuyyfpmbqfrwjlqhedpcitemathbdyptggxgahrddobeotfqmzihsyezruxaewcgqyhzlamamrqfrtiqrlgqvzndyrcsdfiwnmjaljz" };
        String r_key = "59f4476cx7cd8x42";
        System.out.println("Working Directory = " + System.getProperty("user.dir"));
        db.write(w_cmd[0], w_cmd[1]);
        String out = db.read(r_key);
        System.out.print(out);
        db.close();
    }
}
