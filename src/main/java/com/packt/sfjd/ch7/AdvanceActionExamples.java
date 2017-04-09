package com.packt.sfjd.ch7;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.partial.BoundedDouble;
import org.apache.spark.partial.PartialResult;

public class AdvanceActionExamples {

	public static void main(String[] args) {
		 SparkConf conf = new SparkConf().setMaster("local").setAppName("ActionExamples").set("spark.hadoop.validateOutputSpecs", "false");
			JavaSparkContext sparkContext = new JavaSparkContext(conf);
			 //	isEmpty
			  JavaRDD<Integer> intRDD = sparkContext.parallelize(Arrays.asList(1,4,3)); 
			  
			PartialResult<BoundedDouble> countAprx = intRDD.countApprox(1);
			
			//countAprx.

	}

}
