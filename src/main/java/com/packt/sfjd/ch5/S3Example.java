package com.packt.sfjd.ch5;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class S3Example {

	
	public static void main(String[] args) {
		SparkConf conf =new SparkConf().setMaster("local").setAppName("S3 Example");
		JavaSparkContext jsc=new JavaSparkContext(conf);
		jsc.hadoopConfiguration().set("fs.s3n.awsAccessKeyId", "AKIAIEWGWKBBL6UWWH4Q");
		jsc.hadoopConfiguration().set("fs.s3n.awsSecretAccessKey", "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");
		
		JavaRDD<String> textFile = jsc.textFile("s3n://"+"databricks-notebooks-data"+"/"+"s3data.txt");
		
		System.out.println(textFile.collect());
	}
}
