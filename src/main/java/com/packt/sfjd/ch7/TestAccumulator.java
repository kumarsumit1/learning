package com.packt.sfjd.ch7;

import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.util.CollectionAccumulator;
import org.apache.spark.util.LongAccumulator;

public class TestAccumulator {

	public static void main(String[] args) {
		System.setProperty("hadoop.home.dir", "E:\\sumitK\\Hadoop");
		Logger rootLogger = LogManager.getRootLogger();
		rootLogger.setLevel(Level.WARN); 
		      SparkSession sparkSession = SparkSession
		      .builder()
		      .master("local")
			  .config("spark.sql.warehouse.dir","file:///E:/sumitK/Hadoop/warehouse")
		      .appName("JavaALSExample")
		      .getOrCreate();
		
		ListAccumulator listAccumulator=new ListAccumulator();
		
		sparkSession.sparkContext().register(listAccumulator, "ListAccumulator");
		
		LongAccumulator longAccumulator = sparkSession.sparkContext().longAccumulator("longAccumulator");
		//sparkSession.sparkContext().doubleAccumulator()
		//CollectionAccumulator<String> ses = sparkSession.sparkContext().collectionAccumulator();
		
		
		
		//listAccumulator
		
		
		

	}

}
