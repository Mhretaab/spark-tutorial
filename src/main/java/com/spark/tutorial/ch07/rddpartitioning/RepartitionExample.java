package com.spark.tutorial.ch07.rddpartitioning;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class RepartitionExample {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setMaster("local").setAppName("Repartitioning Example");
		JavaSparkContext jsc = new JavaSparkContext(conf);

		JavaRDD<String> textFile = jsc.textFile("test.gz",3);
		System.out.println("Before repartition:"+textFile.getNumPartitions());
		JavaRDD<String> textFileRepartitioned = textFile.repartition(4);
		System.out.println("After repartition:"+textFileRepartitioned.getNumPartitions());
	}
}
