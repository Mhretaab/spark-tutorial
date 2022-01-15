package com.spark.tutorial.chapterfour;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class FlatMapTransformation {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("FlatMap Basics").setMaster("local");
		JavaSparkContext javaSparkContext = new JavaSparkContext(conf);

		JavaRDD<String> stringRDD = javaSparkContext.parallelize(Arrays.asList("Hello World", "Hello Spark"));
		JavaRDD<String> wordsRDD = stringRDD.flatMap(s -> Arrays.asList(s.split(" ")).iterator());

		List<String> result = wordsRDD.collect();
		System.out.println(result);
	}
}
