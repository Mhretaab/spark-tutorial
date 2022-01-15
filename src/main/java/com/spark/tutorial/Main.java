package com.spark.tutorial;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.SparkConf;

import java.util.Arrays;
import java.util.List;

public class Main {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("appName").setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);

		List<Integer> data = Arrays.asList(1, 2, 3, 4, 5);
		JavaRDD<Integer> distributedData = sc.parallelize(data, 2);

		Integer sum = distributedData.reduce((a, b) -> a + b);

		System.out.println(sum);

	}
}
