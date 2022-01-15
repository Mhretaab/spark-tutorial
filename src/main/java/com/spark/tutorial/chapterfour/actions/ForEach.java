package com.spark.tutorial.chapterfour.actions;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;

public class ForEach {
	public static void main(String[] args) {
		//In order to iterate over all the elements of the RDD to perform some
		//repetitive tasks without fetching the entire set of data to the Driver program
		SparkConf conf = new SparkConf().setAppName("ForEach Basics").setMaster("local");
		JavaSparkContext javaSparkContext = new JavaSparkContext(conf);

		JavaRDD<Integer> rdd = javaSparkContext.parallelize(Arrays.asList(1, 2, 3,4,5),3);
		rdd.foreach(x->System.out.println("The element values of the RDD are ::"+x));

	}
}
