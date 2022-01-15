package com.spark.tutorial.ch04.actions;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

public class TakeAndTakeOrderedAndTakeSample {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("Take, TakeOrdered, And, TakeSample  Basics").setMaster("local");
		JavaSparkContext javaSparkContext = new JavaSparkContext(conf);

		JavaRDD<Integer> intRDD = javaSparkContext.parallelize(Arrays.asList(1, 2, 9, 3, 4, 8, 13));

		List<Integer> result1 = intRDD.take(3);
		System.out.println(result1);

		List<Integer> result2 = intRDD.takeOrdered(3);
		System.out.println(result2);

		List<Integer> result3 = intRDD.takeOrdered(3, Comparator.reverseOrder());
		System.out.println(result3);

		List<Integer> result4 = intRDD.takeSample(true, 4);
		System.out.println(result4);

		List<Integer> takeSamepTrueSeededList = intRDD.takeSample(true, 4, 9);
		System.out.println(takeSamepTrueSeededList);
	}
}
