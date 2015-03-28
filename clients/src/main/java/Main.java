import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;

import java.io.*;
import java.net.URI;


/**
 * @author m-erofeev
 * @since 11.01.15
 */
public class Main {

  public static void main(String[] args) throws Exception {
//    String uri = args[0];
    String host = "172.17.0.35";
    int port = 49188;
    String addr = "hdfs://" + host + "/";
    String dstUri = addr + "user/root/sample-20150101.data";
    String srcUri = "/Users/m-erofeev/sample-20150101.data";
    write(srcUri, dstUri);
  }

  private static void read(String uri) throws IOException {
    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.get(URI.create(uri), conf);
    InputStream in = null;
    try {
      in = fs.open(new Path(uri));
      IOUtils.copyBytes(in, System.out, 4096, false);
    } finally {
      IOUtils.closeStream(in);
    }
  }

  public static void write(String localSrc, String dst) throws Exception {
    InputStream in = new BufferedInputStream(new FileInputStream(localSrc));
    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.get(URI.create(dst), conf);
    OutputStream out = fs.create(new Path(dst), new Progressable() {
      public void progress() {
        System.out.print(".");
      }
    });
    IOUtils.copyBytes(in, out, 4096, true);
  }
}
