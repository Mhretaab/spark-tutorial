package com.spark.tutorial.ch04.transformations;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class ReduceByKeyTransformation {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("ReduceByKey Basic").setMaster("local");
		JavaSparkContext javaSparkContext = new JavaSparkContext(conf);

		List<Integer> intList = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);

		JavaRDD<Integer> intRDD = javaSparkContext.parallelize(intList, 2);

		JavaPairRDD<String, Integer> intTupleRDD = intRDD.mapToPair(
				integer -> integer % 2 == 0 ? new Tuple2<>("EVEN", integer) : new Tuple2<>("ODD", integer));

		JavaPairRDD<String, Integer> sumsRDD = intTupleRDD.reduceByKey((v1, v2) -> v1 + v2);
		JavaPairRDD<String, Integer> maxRDD = intTupleRDD.reduceByKey((a, b) -> Math.max(a, b));

		List<Tuple2<String, Integer>> result = sumsRDD.collect();
		Map<String, Integer> max = maxRDD.collectAsMap();

		System.out.println(result);
		System.out.println(max);
	}
}
