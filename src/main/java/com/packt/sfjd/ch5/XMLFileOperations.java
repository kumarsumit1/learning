package com.packt.sfjd.ch5;

import java.util.HashMap;

import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class XMLFileOperations {

	public static void main(String[] args) {
		 System.setProperty("hadoop.home.dir", "E:\\sumitK\\Hadoop");
			
	      SparkSession spark = SparkSession
	      .builder()
	      .master("local")
		  .config("spark.sql.warehouse.dir","file:///E:/sumitK/Hadoop/warehouse")
	      .appName("JavaALSExample")
	      .getOrCreate();
	      Logger rootLogger = LogManager.getRootLogger();
			rootLogger.setLevel(Level.WARN); 

		
		String formatClass = "com.databricks.spark.xml";
		HashMap<String, String> params = new HashMap<String, String>();
		params.put("rowTag", "food");
		// params.put("failFast", "true");
		 Dataset<Row> docDF = spark.read().format(formatClass).options(params).load("C:/Users/sumit.kumar/git/learning/src/main/resources/breakfast_menu.xml");
		 
		 docDF.printSchema();
		 
		 docDF.show();

	}

}
