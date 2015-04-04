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
import java.util.Iterator;


/**
 * @author m-erofeev
 * @since 11.01.15
 */
public class Script extends Configured implements Tool {

  public static final String HADOOP_MASTER_HOST = "had00p-master";

  /*
    * TODO map failed, jobhistory not enabled.
    * enable - http://stackoverflow.com/questions/13656138/how-do-i-view-my-hadoop-job-history-and-logs-using-cdh4-and-yarn
   */

  public static void main(String[] args) throws Exception {
    ToolRunner.run(new Script(), args);
  }


  @Override
  public int run(String[] args) throws Exception {
    String srcUri = "tolstoy.txt";
    write(srcUri, "/user/m-erofeev/" + srcUri);
    simpleMr(srcUri);
    return 0;
  }

  @Override
  public Configuration getConf() {
    String host = HADOOP_MASTER_HOST;
    int nameNodeHdfsPort = 9000;
    int yarnPort = 8032;
    String yarnAddr = host + ":" + yarnPort;
    String hdfsAddr = "hdfs://" + host + ":" + nameNodeHdfsPort + "/";

    Configuration configutation = new Configuration();
    configutation.set("yarn.resourcemanager.address", yarnAddr);
    configutation.set("mapreduce.framework.name", "yarn");
    configutation.set("fs.defaultFS", hdfsAddr);

    configutation.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
    configutation.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
    return configutation;
  }


  private void simpleMr(String inputPath) throws IOException {

    final JobConf conf = new JobConf(getConf(), Script.class);
    conf.setJobName("fun");
    conf.setJar("target/clients-1.0-SNAPSHOT.jar");
    conf.setMapperClass(MyMapper.class);
    conf.setCombinerClass(MyReducer.class);
    conf.setReducerClass(MyReducer.class);

    conf.setInputFormat(TextInputFormat.class);
    conf.setOutputFormat(TextOutputFormat.class);

    conf.setMapOutputKeyClass(Text.class);
    conf.setMapOutputValueClass(LongWritable.class);

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
    String output = org.apache.commons.io.IOUtils.toString(read(tmpMRreturn));
    System.out.println(output);
  }

  public static class MyMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, LongWritable> {

    @Override
    public void map(LongWritable key, Text value, OutputCollector<Text, LongWritable> outputCollector, Reporter reporter) throws IOException {
      String[] split = value.toString().split(" ");
      for (String s : split) {
        outputCollector.collect(new Text(s), new LongWritable(1));
      }

    }
  }

  public static class MyReducer extends MapReduceBase implements Reducer<Text, LongWritable, Text, LongWritable> {


    @Override
    public void reduce(Text key, Iterator<LongWritable> values, OutputCollector<Text, LongWritable> output, Reporter reporter) throws IOException {
      long ret = 0;
      while (values.hasNext()) {
        ret += values.next().get();
      }
      output.collect(key, new LongWritable(ret));
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
