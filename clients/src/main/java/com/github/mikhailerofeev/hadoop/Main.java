package com.github.mikhailerofeev.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.*;
import java.net.URI;


/**
 * @author m-erofeev
 * @since 11.01.15
 */
public class Main {

  public static final String BOOT_TO_DOCKER_IP = "192.168.59.103";

  public static void main(String[] args) throws Exception {
    String host = BOOT_TO_DOCKER_IP;
    int port = 9000;
    String addr = "hdfs://" + host + ":" + port + "/";
    String dstUri = addr + "user/m-erofeev/sample-20150101.data";
    String srcUri = "/Users/m-erofeev/sample-20150101.data";
//    write(srcUri, dstUri);
    InputStream read = read(dstUri);
    IOUtils.copyBytes(read, System.out, 4096);
  }


  private static InputStream read(String uri) throws IOException {
    return AccessUtils.execAsRoot(() -> {
      try {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(uri), conf);
        InputStream in = null;
        in = fs.open(new Path(uri));
        return in;
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    });
  }


  public static void write(final String localSrc, final String dst) {
    AccessUtils.execAsRootUnsafe(() -> {
          InputStream in = new BufferedInputStream(new FileInputStream(localSrc));
          Configuration conf = new Configuration();
          FileSystem fs = FileSystem.get(URI.create(dst), conf);
          OutputStream out = null;
          out = fs.create(new Path(dst));
          IOUtils.copyBytes(in, out, 4096, true);
        }
    );
  }

}
