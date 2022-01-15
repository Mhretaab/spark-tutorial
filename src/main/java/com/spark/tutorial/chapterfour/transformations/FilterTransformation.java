package com.spark.tutorial.chapterfour.transformations;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;

public class FilterTransformation {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("Filter Basics").setMaster("local");
		JavaSparkContext javaSparkContext = new JavaSparkContext(conf);

		List<Integer> intList = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);

		JavaRDD<Integer> intRDD = javaSparkContext.parallelize(intList, 2);
		JavaRDD<Integer> filteredRDD = intRDD.filter(x -> x % 2 == 0);
		List<Integer> result = filteredRDD.collect();
		System.out.println(result);
	}
}
