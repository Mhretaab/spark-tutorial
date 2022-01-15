package com.spark.tutorial.ch04.actions;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;

public class Count {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("Count Basics").setMaster("local");
		JavaSparkContext javaSparkContext = new JavaSparkContext(conf);

		JavaRDD<Integer> intRDD = javaSparkContext.parallelize(Arrays.asList(1, 2, 3));

		long count = intRDD.count();

		System.out.println("Total items: " + count);
	}
}
