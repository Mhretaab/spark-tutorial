package com.spark.tutorial.ch05.datasources;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

public class HDFS {
	public static void main(String[] args) {
		//scalability, high availability, and fault tolerance
		SparkConf conf = new SparkConf().setAppName("Interaction with Hadoop Distributed File System (HDFS)").setMaster("local");
		JavaSparkContext javaSparkContext = new JavaSparkContext(conf);

		//JavaRDD<String> hadoopFile= jsc.textFile("hdfs://"+"namenode-host:port/"+"absolute-file-path-on-hadoop");
		JavaRDD<String> hadoopFile= javaSparkContext.textFile("hdfs://localhost:8020/user/spark/data.csv");
		//javaSparkContext.textFile("hdfs://"+"namenode-host:port/"+"absolute-file-path-of-file1"+", "hdfs://"+"namenode-host:port/"+"absolute-file-path-of-file2"+")
		//javaSparkContext.textFile("hdfs://"+"namenode-host:port/"+"absolute-file-path-of-dir/*");

		JavaPairRDD<String, Integer> hadoopWordCount = hadoopFile.flatMapToPair(text -> Arrays.asList(text.split(" ")).stream()
				.map(word-> new Tuple2<>(word, 1)).iterator());

		JavaPairRDD<String, Integer> wordCountRDD = hadoopWordCount.reduceByKey((v1, v2) -> v1 + v2);

		//To write the results back to HDFS
		//hadoopWordCount.saveAsTextFile("hdfs://"+"namenode-host:port/"+"absolute-file-path-of-output-dir")
	}
}
