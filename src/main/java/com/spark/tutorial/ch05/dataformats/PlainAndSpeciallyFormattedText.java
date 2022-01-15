package com.spark.tutorial.ch05.dataformats;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.ArrayList;

public class PlainAndSpeciallyFormattedText {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("Plain And Specially Formatted Text").setMaster("local");
		JavaSparkContext javaSparkContext = new JavaSparkContext(conf);

		JavaRDD<String> inputData = javaSparkContext.textFile("/home/mberhe/Projects/tutorials/spark-tutorial/data/persons.txt", 3);

		//POJO objects should be serializable, as RDD is a distributed
		//data structure and while shuffling operations during
		//transformations/actions, data gets transferred over the network
		JavaRDD<Person> persons = inputData.map(line -> {
			String[] data = line.split("~");
			Person person = new Person();
			person.setName(data[0]);
			person.setAge(Integer.parseInt(data[1].trim()));
			person.setOccupation(data[2]);
			return person;
		});

		persons.foreach(person -> System.out.println(person));

		//there is some performance gain if we choose
		//mapPartitions() over map() in general, as it gives an iterator of the elements on
		//the RDD for each partition
		JavaRDD<Person> peoplePart = inputData.mapPartitions( personIterator -> {
			ArrayList<Person> personList = new ArrayList<>();
			while (personIterator.hasNext()) {
				String[] parts = personIterator.next().split("~");
				Person person = new Person();
				person.setName(parts [0]);
				person.setAge(Integer.parseInt(parts [1].trim()));
				person.setOccupation(parts [2]);
				personList.add(person);
			}
			return personList.iterator();
		});
		peoplePart.foreach(p -> System.out.println(p));

		//The number of output files are equal to the number of partitions in the RDD,
		//hence it can be controlled by calling the repartition()
		//expensive process because of data shuffle
		peoplePart.repartition(1).saveAsTextFile("/home/mberhe/Projects/tutorials/spark-tutorial/data/persons_dir");

		//Coalesce avoids the shuffling of data
		peoplePart.coalesce(1).saveAsTextFile( "/home/mberhe/Projects/tutorials/spark-tutorial/data/persons_dir_2" );
	}
}
