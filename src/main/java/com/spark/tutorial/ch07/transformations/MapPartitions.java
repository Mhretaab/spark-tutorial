package com.spark.tutorial.ch07.transformations;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class MapPartitions {
	public static void main(String[] args) {
		//acts upon each partition of the RDD
		//executed once per RDD partition
		SparkConf conf = new SparkConf().setMaster("local").setAppName("Map Partitions Example");
		JavaSparkContext jsc = new JavaSparkContext(conf);

		JavaRDD<Integer> intRDD = jsc.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10), 2);

		JavaRDD<Integer> integerJavaRDD = intRDD.mapPartitions(iterator -> {
			List<Integer> intList = new ArrayList<>();
			while (iterator.hasNext()) {
				intList.add(iterator.next() + 1);
			}
			return intList.iterator();
		});

		System.out.println("# partitions: " + integerJavaRDD.getNumPartitions());

		//useful if an interaction is required with external systems

		/*intRDD.mapPartitions(iterator -> {
			Class.forName("com.mysql.jdbc.Driver");
			Connection con = DriverManager.getConnection("jdbc:mysql://localhost:3306/test", "root", "root");
			Statement stmt = con.createStatement();
			List<String> names = new ArrayList<>();
			while (iterator.hasNext()) {
				ResultSet rs = stmt.executeQuery("select name from emp where empId = " + iterator.next());
				if (rs.first())
				{
					names.add(rs.getString("empName"));
				}
			}
			return names.iterator();
		});*/

	}
}
