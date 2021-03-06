package com.spark.tutorial.ch07.rddpartitioning;

import org.apache.spark.RangePartitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.rdd.RDD;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class RangePartitionExample {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setMaster("local").setAppName("Range Partition Example");
		JavaSparkContext jsc = new JavaSparkContext(conf);

		JavaPairRDD<Integer, String> pairRdd = jsc.parallelizePairs(
				Arrays.asList(new Tuple2<>(1, "A"), new Tuple2<>(2, "B"),
						new Tuple2<>(3, "C"), new Tuple2<>(4, "D"),
						new Tuple2<>(5, "E"), new Tuple2<>(6, "F"),
						new Tuple2<>(7, "G"), new Tuple2<>(8, "H")), 3);

		RDD<Tuple2<Integer, String>> rdd = JavaPairRDD.toRDD(pairRdd);

		RangePartitioner rangePartitioner = new RangePartitioner(4, rdd, true, scala.math.Ordering.Int$.MODULE$,
				scala.reflect.ClassTag$.MODULE$.apply(Integer.class));

		JavaPairRDD<Integer, String> rangePartitioned = pairRdd.partitionBy(rangePartitioner);

		JavaRDD<String> mapPartitionsWithIndex = rangePartitioned.mapPartitionsWithIndex((index, tupleIterator) -> {

			List<String> list = new ArrayList<>();

			while (tupleIterator.hasNext()) {
				list.add("Partition number:" + index + ",key:" + tupleIterator.next()._1());
			}

			return list.iterator();
		}, true);

		System.out.println(mapPartitionsWithIndex.collect());
	}
}
