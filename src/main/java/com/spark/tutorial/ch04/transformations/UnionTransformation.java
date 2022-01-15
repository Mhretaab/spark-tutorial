package com.spark.tutorial.ch04.transformations;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;

public class UnionTransformation {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("Union Basics").setMaster("local");
		JavaSparkContext javaSparkContext = new JavaSparkContext(conf);

		JavaRDD<Integer> intRDD1 = javaSparkContext.parallelize(Arrays.asList(1, 2, 3, 4, 5));
		JavaRDD<Integer> intRDD2 = javaSparkContext.parallelize(Arrays.asList(6, 7, 8, 9, 10));

		JavaRDD<Integer> union = intRDD1.union(intRDD2);

		List<Integer> result = union.collect();

		System.out.println(result);
	}
}
