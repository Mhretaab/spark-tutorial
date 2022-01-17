package com.spark.tutorial.ch07.rddpartitioning;

import org.apache.spark.Partitioner;

public class CustomPartitioner extends Partitioner {
	final int maxPartitions = 2;

	@Override
	public int numPartitions() {
		return this.maxPartitions;
	}

	@Override
	public int getPartition(Object key) {
		return (((String) key).length() % maxPartitions);
	}
}
