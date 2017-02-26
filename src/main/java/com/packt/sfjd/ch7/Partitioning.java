package com.packt.sfjd.ch7;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class Partitioning {
	public static void main(String[] args) {

		SparkConf conf = new SparkConf().setMaster("local").setAppName("Partitioning");
		JavaSparkContext jsc = new JavaSparkContext(conf);

		JavaRDD<Integer> parallelize = jsc.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10), 3);

		System.out.println(parallelize.getNumPartitions());
	}
}
