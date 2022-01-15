package com.spark.tutorial.ch04.transformations;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;

public class DistinctTransformation {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("Distinct Basics").setMaster("local");
		JavaSparkContext javaSparkContext = new JavaSparkContext(conf);

		JavaRDD<Integer> rddwithdupElements = javaSparkContext.parallelize(Arrays.asList(1,1,2,4,5,6,8,8));

		JavaRDD<Integer> distinct = rddwithdupElements.distinct();

		List<Integer> result = distinct.collect();
	}
}
