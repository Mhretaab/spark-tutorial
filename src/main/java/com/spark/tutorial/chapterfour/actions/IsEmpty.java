package com.spark.tutorial.chapterfour.actions;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;

public class IsEmpty {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("IsEmpaty Basics").setMaster("local");
		JavaSparkContext javaSparkContext = new JavaSparkContext(conf);

		JavaRDD<Integer> intRDD = javaSparkContext.parallelize(Arrays.asList(1, 2, 3));
		JavaRDD<Integer> greaterThan10 = intRDD.filter(i -> i > 10);
		boolean empty = greaterThan10.isEmpty();

		System.out.println("greaterThan10 RDD is empty: " + empty);
	}
}
