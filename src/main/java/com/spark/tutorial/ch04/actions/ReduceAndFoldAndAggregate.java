package com.spark.tutorial.ch04.actions;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;

public class ReduceAndFoldAndAggregate {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("Reduce, Fold and Aggregate  Basics").setMaster("local");
		JavaSparkContext javaSparkContext = new JavaSparkContext(conf);

		JavaRDD<Integer> intRDD = javaSparkContext.parallelize(Arrays.asList(1, 2, 4));

		Integer sum = intRDD.reduce((v1, v2) -> v1 + v2);
		System.out.println("Sum by reduce: " + sum);
		Integer multiply = intRDD.reduce((v1, v2) -> v1 * v2);
		System.out.println("Multiply by reduce: " + multiply);

		Integer sumByFold = intRDD.fold(0, (a, b) -> a + b);
		System.out.println("Sum by fold: " + sumByFold);

		Integer multiplyByFold = intRDD.fold(1, (a, b) -> a * b);
		System.out.println("Multiply by fold: " + multiplyByFold);

		JavaRDD<Integer> rdd = javaSparkContext.parallelize(Arrays.asList(0, 1, 2, 3, 4, 5), 3);
		System.out.println("The no of partitions are ::" + rdd.getNumPartitions());
		String concat = rdd.aggregate("X", (x, y) -> x + y, (x, z) -> x + z);
		System.out.println("The aggerate value is ::" + concat);
		Integer sumByAgg = rdd.aggregate(0, (x, y) -> x + y, (x, z) -> x + z);
		System.out.println("The aggerate sum is ::" + sumByAgg);
	}
}
