package com.spark.tutorial.ch04.transformations;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;

public class IntersectTransformation {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("Intersection Basics").setMaster("local");
		JavaSparkContext javaSparkContext = new JavaSparkContext(conf);

		JavaRDD<Integer> intRDD1 = javaSparkContext.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6, 7));
		JavaRDD<Integer> intRDD2 = javaSparkContext.parallelize(Arrays.asList(5, 6, 7, 8, 9, 10));

		JavaRDD<Integer> intersection = intRDD1.intersection(intRDD2);

		List<Integer> result = intersection.collect();

		System.out.println(result);
	}
}
