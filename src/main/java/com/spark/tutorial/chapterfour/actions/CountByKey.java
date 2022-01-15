package com.spark.tutorial.chapterfour.actions;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Map;

public class CountByKey {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("CountByKey Basics").setMaster("local");
		JavaSparkContext javaSparkContext = new JavaSparkContext(conf);

		JavaPairRDD<String, Integer> pairRDD1 = javaSparkContext.parallelizePairs (
				Arrays.asList(
						new Tuple2<>("B", 2),
						new Tuple2<>("B", 5),
						new Tuple2<>("D", 7),
						new Tuple2<>("A", 8),
						new Tuple2<>("A", 18),
						new Tuple2<>("E", 8)
				));

		Map<String, Long> result = pairRDD1.countByKey();

		System.out.println(result);
	}
}
