package com.github.mikhailerofeev.hadoop.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

/**
 * @author erofeev
 * @since 19.06.15
 */
public class SparkScript {

  public static void main(String[] args) {
    SparkConf conf = new SparkConf();
    //4 - threads
    JavaSparkContext spark = new JavaSparkContext("local[4]", "simple count", conf);
    JavaRDD<String> data = spark.textFile("/Users/erofeev/bin/SearchInfo.tsv");
    data = rmFirstLine(data);

    JavaPairRDD<Long, Integer> users2count = getIds2ActionsCount(data);
    users2count = users2count.cache();
    System.out.println("uniq users " + users2count.count());

    System.out.println("sessions sizes distrib:");

    JavaPairRDD<Integer, Integer> sessionSizeCount = users2count
      .mapToPair(v -> new Tuple2<>(v._2(), 1))
      .reduceByKey((v1, v2) -> v1 + v2)
      .sortByKey();
    sessionSizeCount.foreach(v -> System.out.println(v._1() + "\t" + v._2()));
  }

  private static JavaPairRDD<Long, Integer> getIds2ActionsCount(JavaRDD<String> data) {
    JavaPairRDD<Long, String> id2actionTs = data.mapToPair(s -> {
      String[] tabs = s.split("\t");
      return new Tuple2<>(Long.valueOf(tabs[2]), tabs[1]);
    });
    return id2actionTs.aggregateByKey(0, (v1, v2) -> v1 + 1, (v1, v2) -> v1 + v2);
  }

  private static JavaRDD<String> rmFirstLine(JavaRDD<String> data) {
    return data
      .zipWithIndex()
      .filter(v1 -> v1._2() > 0)
      .map(Tuple2::_1);
  }
}
