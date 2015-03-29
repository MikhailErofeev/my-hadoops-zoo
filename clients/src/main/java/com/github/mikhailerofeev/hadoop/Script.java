package com.github.mikhailerofeev.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.*;
import java.net.InetSocketAddress;
import java.net.URI;


/**
 * @author m-erofeev
 * @since 11.01.15
 */
public class Script extends Configured implements Tool {

  public static final String BOOT_TO_DOCKER_IP = "192.168.59.103";

  /*
    * TODO map failed, jobhistory not enabled.
    * enable - http://stackoverflow.com/questions/13656138/how-do-i-view-my-hadoop-job-history-and-logs-using-cdh4-and-yarn
   */

  public static void main(String[] args) throws Exception {
    ToolRunner.run(new Script(), args);
  }


  @Override
  public int run(String[] args) throws Exception {
    String host = BOOT_TO_DOCKER_IP;
    int nameNodeHdfsPort = 9000;
    int yarnPort = 8032;
    String hdfsAddr = "hdfs://" + host + ":" + nameNodeHdfsPort + "/";
    String yarnAddr = host + ":" + yarnPort;
    String sampleHdfsPath = hdfsAddr + "user/m-erofeev/sample-20150101.data";
    String srcUri = "/Users/m-erofeev/sample-20150101.data";

//    write(srcUri, sampleHdfsPath);

//    InputStream read = read(sampleHdfsPath);
//    IOUtils.copyBytes(read, System.out, 4096);
    System.out.println("start map");
    simpleMr(hdfsAddr, yarnAddr, sampleHdfsPath);
    return 0;
  }

  private static void simpleMr(String hdfsAddr, String yarnAddr, String sampleHdfsPath) throws IOException {

    JobConf conf = new JobConf(MyMapper.class);
    conf.set("yarn.resourcemanager.address", yarnAddr);
    conf.set("mapreduce.framework.name", "yarn");
    conf.set("fs.default.name", hdfsAddr);
    conf.setJobName("fun");
    conf.setJarByClass(MyMapper.class);
    conf.setMapperClass(MyMapper.class);

    conf.setInputFormat(TextInputFormat.class);
    conf.setOutputFormat(TextOutputFormat.class);

    FileInputFormat.setInputPaths(conf, sampleHdfsPath);
    String tmpMRreturn = hdfsAddr + "user/m-erofeev/map-test.data";
    Path returnPath = new Path(tmpMRreturn);
    FileOutputFormat.setOutputPath(conf, returnPath);

    AccessUtils.execAsRootUnsafe(() -> {
      Configuration con = new Configuration();
      FileSystem fs = FileSystem.get(URI.create(hdfsAddr), con);
      if (fs.exists(returnPath)) {
        fs.delete(returnPath, true);
      }
    });

    String hostname = yarnAddr.split(":")[0];
    int port = Integer.valueOf(yarnAddr.split(":")[1]);
    AccessUtils.execAsRootUnsafe(() -> {
      RunningJob runningJob = new JobClient(new InetSocketAddress(hostname, port), conf).submitJob(conf);
      runningJob.waitForCompletion();
    });
  }

  static class MyMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {

    @Override
    public void map(LongWritable key, Text value, OutputCollector<Text, Text> outputCollector, Reporter reporter) throws IOException {
      String[] split = value.toString().split("\t+");
      outputCollector.collect(new Text(split[0]), new Text("blah-blah"));
    }
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