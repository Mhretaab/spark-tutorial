package com.spark.tutorial.chapterfour.transformations;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class CoGroupTransformation {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("CoGroup Basics").setMaster("local");
		JavaSparkContext javaSparkContext = new JavaSparkContext(conf);

		JavaPairRDD<String, String> pairRDD1 = javaSparkContext.parallelizePairs (
				Arrays.asList(
						new Tuple2<>("B", "A"),
						new Tuple2<>("B", "D"),
						new Tuple2<>("A", "E"),
						new Tuple2<>("A", "B")
				));

		JavaPairRDD<String, Integer> pairRDD2 = javaSparkContext.parallelizePairs (
				Arrays.asList(
						new Tuple2<>("B", 2),
						new Tuple2<>("B", 5),
						new Tuple2<>("A", 7),
						new Tuple2<>("A", 8)
				));

		JavaPairRDD<String, Tuple2<Iterable<String>, Iterable<Integer>>> cogroupedRDD = pairRDD1.cogroup(pairRDD2);

		List<Tuple2<String, Tuple2<Iterable<String>, Iterable<Integer>>>> result = cogroupedRDD.collect();

		System.out.println(result);
	}
}
