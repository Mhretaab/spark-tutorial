package com.spark.tutorial.ch05.datasources;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.HashMap;
import java.util.Map;

public class CassandraWithSparkSession {
	public static void main(String[] args) {

		//SparkSession is the single entry point for a Spark program
		//SparkSession performs all roles of SparkContext , SQLContext
		SparkSession sparkSession = SparkSession.builder().master("local").appName("My App").config("spark.cassandra.connection.host", "localhost").getOrCreate();

		Map<String, String> map = new HashMap<>();
		map.put("table", "emp");
		map.put("keyspace", "my_keyspace");

		Dataset<Row> empDeptDataSet = sparkSession.read()
				.format("org.apache.spark.sql.cassandra")
				.option("keyspace", "my_keysapce")
				.option("table", "emp")
				.load();

		//to read the data
		//show() function is an action
		empDeptDataSet.show();

		//temporary view can be created on the dataset, using which, users can run SQL on the data
		empDeptDataSet.createOrReplaceTempView("cass_data_view");
		sparkSession.sql("Select * from cass_data_view");
	}
}
