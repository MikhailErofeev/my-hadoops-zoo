package com.github.mikhailerofeev.hadoop;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;

/**
 * @author m-erofeev
 * @since 04.04.15
 */
public class MyMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {

  @Override
  public void map(LongWritable key, Text value, OutputCollector<Text, Text> outputCollector, Reporter reporter) throws IOException {
    String[] split = value.toString().split("\t+");
    outputCollector.collect(new Text(split[0]), new Text("blah-blah"));
  }
}
