package com.spark.tutorial.ch07.rddpartitioning;

import org.apache.spark.HashPartitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class HashPartitionExample {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setMaster("local").setAppName("Hash Partition Example");
		JavaSparkContext jsc = new JavaSparkContext(conf);

		JavaPairRDD<Integer, String> pairRdd = jsc.parallelizePairs(
				Arrays.asList(new Tuple2<>(1, "A"), new Tuple2<>(2, "B"),
						new Tuple2<>(3, "C"), new Tuple2<>(4, "D"),
						new Tuple2<>(5, "E"), new Tuple2<>(6, "F"),
						new Tuple2<>(7, "G"), new Tuple2<>(8, "H")), 3);

		System.out.println("The # of partitions: " + pairRdd.getNumPartitions());

		JavaPairRDD<Integer, String> hashPartitioned = pairRdd.partitionBy(new HashPartitioner(2));

		System.out.println("The # of partitions: " + hashPartitioned.getNumPartitions());

		JavaRDD<String> mapPartitionsWithIndex = hashPartitioned.mapPartitionsWithIndex((index, tupleIterator) -> {

			List<String> list = new ArrayList<>();

			while (tupleIterator.hasNext()) {
				list.add("Partition number:" + index + ",key:" + tupleIterator.next()._1());
			}

			return list.iterator();
		}, true);

		System.out.println(mapPartitionsWithIndex.collect());
	}
}
