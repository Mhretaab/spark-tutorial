package com.spark.tutorial.chapterfour.actions;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Comparator;

public class MaxMin {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("Max and Min Basics").setMaster("local");
		JavaSparkContext javaSparkContext = new JavaSparkContext(conf);

		JavaRDD<Integer> intRDD = javaSparkContext.parallelize(Arrays.asList(1, 2, 3, 10, 19));

		Integer max = intRDD.max(Comparator.naturalOrder());
		//IntegerComparator integerComparator = new IntegerComparator();
		Integer max2 = intRDD.max(new IntegerComparator());

		System.out.println("Max item: " + max);
		System.out.println("Max2 item: " + max2);

		Integer min = intRDD.min(Comparator.naturalOrder());

		System.out.println("Min item: " + min);
	}

	static class IntegerComparator implements Comparator<Integer>, Serializable {
		private static final long serialVersionUID = 1L;

		@Override
		public int compare(Integer a, Integer b) {
			return a.compareTo(b);
		}
	}
}
