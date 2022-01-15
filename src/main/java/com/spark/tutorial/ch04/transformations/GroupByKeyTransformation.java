package com.spark.tutorial.ch04.transformations;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class GroupByKeyTransformation {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("GroupByKey Basics").setMaster("local");
		JavaSparkContext javaSparkContext = new JavaSparkContext(conf);

		List<Integer> intList = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);

		JavaRDD<Integer> intRDD = javaSparkContext.parallelize(intList, 2);

		JavaPairRDD<String, Integer> intTupleRDD = intRDD.mapToPair(
				integer -> integer % 2 == 0 ? new Tuple2<>("EVEN", integer) : new Tuple2<>("ODD", integer));

		JavaPairRDD<String, Iterable<Integer>> groupedNumbers = intTupleRDD.groupByKey();

		List<Tuple2<String, Iterable<Integer>>> result = groupedNumbers.collect();
		Map<String, Iterable<Integer>> result2 = groupedNumbers.collectAsMap();

		System.out.println(result);
		System.out.println(result2);
	}
}
