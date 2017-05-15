package com.packt.sfjd.ch8;

import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF2;
import org.apache.spark.sql.types.DataTypes;

public class UDFExample {

	public static void main(String[] args) {
		//Window Specific property if Hadoop is not instaalled or HADOOP_HOME is not set
		 System.setProperty("hadoop.home.dir", "E:\\hadoop");
		
		 //Build a Spark Session	
	      SparkSession sparkSession = SparkSession
	      .builder()
	      .master("local")
		  .config("spark.sql.warehouse.dir","file:///E:/hadoop/warehouse")
	      .appName("EdgeBuilder")
	      .getOrCreate();
	      Logger rootLogger = LogManager.getRootLogger();
		  rootLogger.setLevel(Level.WARN); 
		// Read the CSV data
			 Dataset<Row> emp_ds = sparkSession.read()
					 .format("com.databricks.spark.csv")
	   		         .option("header", "true")
	   		         .option("inferSchema", "true")
	   		         .load("src/main/resources/employee.txt");    
	    		
		  UDF2 calcDays=new CalcDaysUDF();
		  //Registering the UDFs in Spark Session created above      
		    sparkSession.udf().register("calcDays", calcDays, DataTypes.LongType);
		    
		    emp_ds.createOrReplaceTempView("emp_ds");
		    
		    emp_ds.printSchema();
		    emp_ds.show();
		    
		    sparkSession.sql("select calcDays(hiredate,'dd-MM-yyyy') from emp_ds").show();   
		    
		    CaclUDAF udaf= new CaclUDAF();
		    
		    sparkSession.udf().register("calcUD", udaf);
		    
		    sparkSession.sql("select deptno,calcUD(deptno) from emp_ds group by deptno ").show();  
		    

	}

}
