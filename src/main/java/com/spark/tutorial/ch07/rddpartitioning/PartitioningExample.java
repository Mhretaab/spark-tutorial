package com.spark.tutorial.ch07.rddpartitioning;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;

public class PartitioningExample {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setMaster("local").setAppName("Partitioning Example");
		JavaSparkContext jsc = new JavaSparkContext(conf);
		JavaRDD<Integer> intRDD= jsc.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10), 3);

		System.out.println("Number of partitions: " + intRDD.getNumPartitions());
	}
}
