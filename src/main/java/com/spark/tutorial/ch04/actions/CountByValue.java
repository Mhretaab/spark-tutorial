package com.spark.tutorial.ch04.actions;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Map;

public class CountByValue {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("CountByValue Basics").setMaster("local");
		JavaSparkContext javaSparkContext = new JavaSparkContext(conf);

		JavaPairRDD<String, Integer> pairRDD1 = javaSparkContext.parallelizePairs (
				Arrays.asList(
						new Tuple2<>("B", 2),
						new Tuple2<>("B", 7),
						new Tuple2<>("D", 7),
						new Tuple2<>("A", 8),
						new Tuple2<>("A", 18),
						new Tuple2<>("E", 8)
				));

		Map<Tuple2<String, Integer>, Long> result1 = pairRDD1.countByValue();

		System.out.println(result1);

		JavaRDD<Integer> intRDD = javaSparkContext.parallelize(Arrays.asList(1, 1, 2, 3, 4, 4));
		Map<Integer, Long> result2 = intRDD.countByValue();

		System.out.println(result2);
	}
}
