package com.spark.tutorial.ch04.transformations;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;

public class MapTransformation {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("Map Basics").setMaster("local");
		JavaSparkContext javaSparkContext = new JavaSparkContext(conf);

		List<Integer> intList = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);

		JavaRDD<Integer> intRDD = javaSparkContext.parallelize(intList, 2);

		JavaRDD<Integer> mappedInt = intRDD.map(i -> i + 1);

		List<Integer> collect = mappedInt.collect();

		System.out.println(collect);
	}
}
