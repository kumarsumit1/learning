package com.packt.sfjd.ch5;

import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class JsonFileOperations {

	public static void main(String[] args) {
		System.setProperty("hadoop.home.dir", "E:\\sumitK\\Hadoop");
		Logger rootLogger = LogManager.getRootLogger();
		rootLogger.setLevel(Level.WARN); 
		      SparkSession spark = SparkSession
		      .builder()
		      .master("local")
			  .config("spark.sql.warehouse.dir","file:///E:/sumitK/Hadoop/warehouse")
		      .appName("JavaALSExample")
		      .getOrCreate();
		      
		   RDD<String> textFile = spark.sparkContext().textFile("C:/Users/sumit.kumar/git/learning/src/main/resources/pep_json.json",2); 
		   
		   Dataset<Row> anotherPeople = spark.read().json(textFile);
		   
		 //  anotherPeople.
		   
		   anotherPeople.printSchema();
		   anotherPeople.show();
		      
		      
		      Dataset<Row> json_rec = spark.read().json("C:/Users/sumit.kumar/git/learning/src/main/resources/pep_json.json");
		      
		      
		      json_rec.printSchema();
		      
		      json_rec.show();
		      

	}

}
