package com.spark.tutorial.ch07.transformations;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import scala.Tuple2;

import java.util.Arrays;

public class CombineByKey {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("CombineByKey Example").setMaster("local");
		JavaSparkContext jsc = new JavaSparkContext(conf);

		JavaPairRDD<String, String> pairRDD = jsc.parallelizePairs(Arrays.asList(
				new Tuple2<>("key1", "Austria"), new Tuple2<>("key2", "Australia"),
				new Tuple2<>("key3", "Antartica"), new Tuple2<>("key1", "Asia"),
				new Tuple2<>("key2", "France"),new Tuple2<>("key3", "Canada"),
				new Tuple2<>("key1", "Argentina"),new Tuple2<>("key2", "American Samoa"),
				new Tuple2<>("key3", "Germany")),2);

		//(createCombiner, mergeValueWithinPartition, mergeCombiners)
		JavaPairRDD<String, Integer> resultRDD = pairRDD.combineByKey(v1 -> {
			if (v1.startsWith("A"))
				return 1;
			else
				return 0;
		}, (v1, v2) -> {
			if (v2.startsWith("A"))
				v1 += 1;
			return v1;
		}, (v1, v2) -> v1 + v2);
	}
}
