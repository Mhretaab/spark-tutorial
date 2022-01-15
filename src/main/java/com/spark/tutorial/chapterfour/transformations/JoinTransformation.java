package com.spark.tutorial.chapterfour.transformations;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class JoinTransformation {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("Join Basics").setMaster("local");
		JavaSparkContext javaSparkContext = new JavaSparkContext(conf);

		JavaPairRDD<String, Integer> pairRDD1 = javaSparkContext.parallelizePairs (
				Arrays.asList(
						new Tuple2<String, Integer>("B", 2),
						new Tuple2<String, Integer>("C", 5),
						new Tuple2<String, Integer>("D", 7),
						new Tuple2<String, Integer>("A", 8)
				));

		JavaPairRDD<String, String> pairRDD2 = javaSparkContext.parallelizePairs (
				Arrays.asList(
						new Tuple2<String, String>("B", "A"),
						new Tuple2<String, String>("C", "D"),
						new Tuple2<String, String>("D", "A"),
						new Tuple2<String, String>("A", "B")
				));

		JavaPairRDD<String, Tuple2<Integer, String>> joinedRDD = pairRDD1.join(pairRDD2);

		//pairRDD1.leftOuterJoin(pairRDD2);
		//pairRDD1.rightOuterJoin(pairRDD2);
		//pairRDD1.fullOuterJoin(pairRDD2);

		List<Tuple2<String, Tuple2<Integer, String>>> result = joinedRDD.collect();

		System.out.println(result);
	}
}
