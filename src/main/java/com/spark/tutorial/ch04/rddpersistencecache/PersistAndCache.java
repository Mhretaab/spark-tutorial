package com.spark.tutorial.ch04.rddpersistencecache;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;

import java.util.Arrays;

public class PersistAndCache {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("Persist And Cache Basics").setMaster("local");
		JavaSparkContext javaSparkContext = new JavaSparkContext(conf);

		//cache(): persists the data unserialized in the memory by default
		//cache(): same as persist() method with storage level as MEMORY_ONLY
		//Least Recently Used(LRU) algorithms: when rdd requires more than available memory
		JavaRDD<Integer> rdd = javaSparkContext.parallelize(Arrays.asList(1, 2, 3, 4, 5), 3).cache();

		//persist(): with optimal replication and serializing the data
		JavaRDD<Integer> evenRDD = rdd.filter(x -> x % 2 == 0);
		evenRDD.persist(StorageLevel.MEMORY_AND_DISK());
		evenRDD.foreach(t -> System.out.println("The value of RDD are :" + t));

		//unpersisting the RDD
		evenRDD.unpersist();
		rdd.unpersist();
	}
}
