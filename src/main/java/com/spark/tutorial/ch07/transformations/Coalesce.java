package com.spark.tutorial.ch07.transformations;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

public class Coalesce {
	public static void main(String[] args) {
		//used to reduce the number of partitions
		//repartition requires a full shuffle of data
		//coalesce avoids full shuffle.
		SparkConf conf = new SparkConf().setAppName("coalesce basics").setMaster("local");
		JavaSparkContext jsc = new JavaSparkContext(conf);

		JavaPairRDD<Integer, String> pairRDD = jsc.parallelizePairs(
				Arrays.asList(new Tuple2<>(8, "h"),
						new Tuple2<>(5, "e"), new Tuple2<>(4, "d"),
						new Tuple2<>(2, "a"), new Tuple2<>(7, "g"),
						new Tuple2<>(6, "f"), new Tuple2<>(1, "a"),
						new Tuple2<>(3, "c"), new Tuple2<>(3, "z")), 4);

		JavaPairRDD<Integer, String> coalescePartitions = pairRDD.coalesce(2);

		System.out.println("# of partitions: " + coalescePartitions.getNumPartitions());
	}
}
