package com.spark.tutorial.ch07.transformations;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import scala.Tuple2;

import java.util.Arrays;

public class AggregateByKey {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("AggregateByKey Example").setMaster("local");
		JavaSparkContext jsc = new JavaSparkContext(conf);

		JavaPairRDD<String, String> pairRDD = jsc.parallelizePairs(Arrays.asList(
				new Tuple2<>("key1", "Austria"), new Tuple2<>("key2", "Australia"),
				new Tuple2<>("key3", "Antartica"), new Tuple2<>("key1", "Asia"),
				new Tuple2<>("key2", "France"),new Tuple2<>("key3", "Canada"),
				new Tuple2<>("key1", "Argentina"),new Tuple2<>("key2", "American Samoa"),
				new Tuple2<>("key3", "Germany")),2);

		JavaPairRDD<String, Integer> resultRDD = pairRDD.aggregateByKey(0, (accum, value) -> {
			if (value.startsWith("A"))
				accum += 1;
			return accum;
		}, (v1, v2) -> v1 + v2);

		System.out.println(resultRDD.collect());
	}
}
