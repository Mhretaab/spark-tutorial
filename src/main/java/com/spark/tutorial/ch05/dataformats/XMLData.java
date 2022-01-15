package com.spark.tutorial.ch05.dataformats;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.HashMap;

public class XMLData {
	public static void main(String[] args) {
		SparkSession sparkSession = SparkSession
				.builder()
				.appName("XML Data Basics")
				.master("local")
				.config("spark.some.config.option", "some-value")
				.getOrCreate();

		HashMap<String, String> params = new HashMap<String, String>();
		params.put("rowTag", "food");
		params.put("failFast", "true");

		Dataset<Row> docDF = sparkSession.read()
				.format("com.databricks.spark.xml")
				.options(params)
				.load("/home/mberhe/Projects/tutorials/spark-tutorial/data/breakfast_menu.xml");

		docDF.printSchema();
		docDF.show();

		docDF.write().format("com.databricks.spark.xml")
				.option("rootTag", "foods")
				.option("rowTag", "food")
				.save("/home/mberhe/Projects/tutorials/spark-tutorial/data/newMenu.xml");
	}
}
