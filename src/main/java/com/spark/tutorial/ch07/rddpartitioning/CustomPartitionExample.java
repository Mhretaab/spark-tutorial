package com.spark.tutorial.ch07.rddpartitioning;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class CustomPartitionExample {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setMaster("local").setAppName("Custom Partition Example");
		JavaSparkContext jsc = new JavaSparkContext(conf);

		JavaPairRDD<String, String> pairRdd = jsc.parallelizePairs(
				Arrays.asList(new Tuple2<>("India", "Asia"),new Tuple2<>("Germany", "Europe"),
						new Tuple2<>("Japan", "Asia"),new Tuple2<>("France", "Europe"))
				,3);

		JavaPairRDD<String, String> customPartitioned = pairRdd.partitionBy(new CustomPartitioner());

		JavaRDD<String> mapPartitionsWithIndex = customPartitioned.mapPartitionsWithIndex((index, tupleIterator) -> {

			List<String> list = new ArrayList<>();

			while (tupleIterator.hasNext()) {
				list.add("Partition number:" + index + ",key:" + tupleIterator.next()._1());
			}

			return list.iterator();
		}, true);

		System.out.println(mapPartitionsWithIndex.collect());
	}
}
