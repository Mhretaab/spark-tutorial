package com.spark.tutorial.chapterfour.transformations;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class SortByKeyTransformation {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("SortByKey Basic").setMaster("local");
		JavaSparkContext javaSparkContext = new JavaSparkContext(conf);

		JavaPairRDD<String, Integer> unsortedPairRDD = javaSparkContext.parallelizePairs (
				Arrays.asList(
						new Tuple2<String, Integer>("B", 2),
						new Tuple2<String, Integer>("C", 5),
						new Tuple2<String, Integer>("D", 7),
						new Tuple2<String, Integer>("A", 8)
				));

		JavaPairRDD<String, Integer> sortedTuplesRDD = unsortedPairRDD.sortByKey();

		List<Tuple2<String, Integer>> result = sortedTuplesRDD.collect();

		System.out.println(result);
	}
}
