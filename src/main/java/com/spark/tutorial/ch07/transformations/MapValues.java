package com.spark.tutorial.ch07.transformations;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class MapValues {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setMaster("local").setAppName("MapValues Example");
		JavaSparkContext jsc = new JavaSparkContext(conf);

		List<Tuple2<String, Integer>> tuples = Arrays.asList(new Tuple2<>("a", 1), new Tuple2<>("b", 2),
				new Tuple2<>("c", 3));
		JavaPairRDD<String, Integer> pairRDD = jsc.parallelizePairs(tuples);
		JavaPairRDD<String, Integer> mapPairRDD = pairRDD.mapValues(v1 -> v1 * 3);
		System.out.println(mapPairRDD.collect());
	}
}
