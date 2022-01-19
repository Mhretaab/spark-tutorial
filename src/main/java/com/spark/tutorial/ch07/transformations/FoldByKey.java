package com.spark.tutorial.ch07.transformations;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class FoldByKey {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("foldByKey basics").setMaster("local");
		JavaSparkContext jsc = new JavaSparkContext(conf);

		JavaRDD<String> stringRDD = jsc.parallelize(Arrays.asList("Hello Spark", "Hello Java"));

		JavaPairRDD<String, Integer> flatMapToPair = stringRDD.flatMapToPair(
				text -> Arrays.asList(text.split(" ")).stream().map(word -> new Tuple2<String, Integer>(word, 1))
						.collect(Collectors.toList()).iterator());

		List<Tuple2<String, Integer>> result = flatMapToPair.foldByKey(0, (v1, v2) -> v1 + v2).collect();
		System.out.println(result);
	}
}
