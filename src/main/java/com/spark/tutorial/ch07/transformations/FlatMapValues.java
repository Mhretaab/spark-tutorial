package com.spark.tutorial.ch07.transformations;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

public class FlatMapValues {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("FlatMapValues Example").setMaster("local");
		JavaSparkContext jsc = new JavaSparkContext(conf);

		JavaPairRDD<String, String> monExpRDD = jsc
				.parallelizePairs(Arrays.asList(new Tuple2<>("Jan", "50,100,214,10"),
						new Tuple2<>("Feb", "60,314,223,77")));

		JavaPairRDD<String, Integer> pairRDD = monExpRDD.flatMapValues(
				s -> Arrays.stream(s.split(",")).map(v -> Integer.parseInt(v)).collect(Collectors.toList()).iterator());
	}
}
