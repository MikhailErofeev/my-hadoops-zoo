package com.github.mikhailerofeev.hadoop;

import com.github.mikhailerofeev.hadoop.fun.UncheckedProcedure;
import com.google.common.base.Supplier;
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
import java.net.URI;


/**
 * @author m-erofeev
 * @since 11.01.15
 */
public class Script extends Configured implements Tool {

  public static final String BOOT_TO_DOCKER_IP = "172.17.0.4";
//  public static final String BOOT_TO_DOCKER_IP = "localhost";

  /*
    * TODO map failed, jobhistory not enabled.
    * enable - http://stackoverflow.com/questions/13656138/how-do-i-view-my-hadoop-job-history-and-logs-using-cdh4-and-yarn
   */

  public static void main(String[] args) throws Exception {
    ToolRunner.run(new Script(), args);
  }


  @Override
  public int run(String[] args) throws Exception {
    String sampleHdfsPath = "sample-20150101.data";
    String srcUri = "/Users/m-erofeev/sample-20150101.data";
//    write(srcUri, "/user/m-erofeev/" + sampleHdfsPath);
    System.out.println("start map");
    simpleMr(sampleHdfsPath);
    return 0;
  }

  @Override
  public Configuration getConf() {
    String host = BOOT_TO_DOCKER_IP;
    int nameNodeHdfsPort = 9000;
    int yarnPort = 8032;
    String yarnAddr = host + ":" + yarnPort;
    String hdfsAddr = "hdfs://" + host + ":" + nameNodeHdfsPort + "/";

    Configuration configutation = new Configuration();
    configutation.set("yarn.resourcemanager.address", yarnAddr);
    configutation.set("mapreduce.framework.name", "yarn");
    configutation.set("fs.defaultFS", hdfsAddr);
    return configutation;
  }


  private void simpleMr(String inputPath) throws IOException {

    final JobConf conf = new JobConf(getConf(), Script.class);
    conf.setJobName("fun");
    conf.setJarByClass(MyMapper.class);
    conf.setMapperClass(MyMapper.class);
    conf.setInputFormat(TextInputFormat.class);
    conf.setOutputFormat(TextOutputFormat.class);

    FileInputFormat.setInputPaths(conf, inputPath);
    String tmpMRreturn = "/user/m-erofeev/map-test.data";
    final Path returnPath = new Path(tmpMRreturn);
    FileOutputFormat.setOutputPath(conf, returnPath);

    AccessUtils.execAsRootUnsafe(new UncheckedProcedure() {
      @Override
      public void call() throws Exception {
        FileSystem fs = FileSystem.get(Script.this.getConf());
        if (fs.exists(returnPath)) {
          fs.delete(returnPath, true);
        }
      }
    });
    AccessUtils.execAsRootUnsafe(new UncheckedProcedure() {
      @Override
      public void call() throws Exception {
        RunningJob runningJob = JobClient.runJob(conf);
        runningJob.waitForCompletion();
      }
    });
  }

  public static class MyMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {

    @Override
    public void map(LongWritable key, Text value, OutputCollector<Text, Text> outputCollector, Reporter reporter) throws IOException {
      String[] split = value.toString().split("\t+");
      outputCollector.collect(new Text(split[0]), new Text("blah-blah"));
    }
  }


  private static InputStream read(final String uri) throws IOException {
    return AccessUtils.execAsRoot(new Supplier<InputStream>() {
      @Override
      public InputStream get() {
        try {
          Configuration conf = new Configuration();
          FileSystem fs = FileSystem.get(URI.create(uri), conf);
          InputStream in = null;
          in = fs.open(new Path(uri));
          return in;
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }
    });
  }


  public void write(final String localSrc, final String dst) {
    AccessUtils.execAsRootUnsafe(new UncheckedProcedure() {
                                   @Override
                                   public void call() throws Exception {
                                     InputStream in = new BufferedInputStream(new FileInputStream(localSrc));
                                     FileSystem fs = FileSystem.get(Script.this.getConf());
                                     OutputStream out = null;
                                     out = fs.create(new Path(dst));
                                     IOUtils.copyBytes(in, out, 4096, true);
                                   }
                                 }
    );
  }

}
