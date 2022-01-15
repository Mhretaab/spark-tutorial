package com.spark.tutorial.chapterfour.actions;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;

public class Collect {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("Collect Basics").setMaster("local");
		JavaSparkContext javaSparkContext = new JavaSparkContext(conf);

		JavaRDD<Integer> intRDD = javaSparkContext.parallelize(Arrays.asList(1, 2, 3));

		List<Integer> result = intRDD.collect(); //Not for large dataset, driver program could crash

		result.forEach(i-> System.out.println(i));
	}
}
