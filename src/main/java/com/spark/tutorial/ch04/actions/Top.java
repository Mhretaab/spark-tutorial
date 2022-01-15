package com.spark.tutorial.ch04.actions;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;

public class Top {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("Top  Basics").setMaster("local");
		JavaSparkContext javaSparkContext = new JavaSparkContext(conf);

		JavaRDD<Integer> intRDD = javaSparkContext.parallelize(Arrays.asList(1, 2, 9, 3, 4, 8, 13));

		List<Integer> topTwo=intRDD.top(2);

		System.out.println(topTwo);
	}
}
