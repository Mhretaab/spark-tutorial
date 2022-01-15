package com.spark.tutorial.ch04.transformations;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class CartesianTransformation {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("Cartesian Basic").setMaster("local");
		JavaSparkContext javaSparkContext = new JavaSparkContext(conf);

		JavaRDD<String> stringRDD = javaSparkContext.parallelize(Arrays.asList("A", "B", "C"));
		JavaRDD<Integer> intRDD = javaSparkContext.parallelize(Arrays.asList(1, 2, 3));

		JavaPairRDD<String, Integer> cartesian = stringRDD.cartesian(intRDD);

		List<Tuple2<String, Integer>> result = cartesian.collect();

		System.out.println(result);
	}
}
