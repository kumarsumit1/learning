package com.packt.sfjd.ch8;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.network.protocol.Encoders;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class DsExample {
	public static void main(String[] args) {
		System.setProperty("hadoop.home.dir", "C:\\softwares\\Winutils");
		SparkSession sparkSession = SparkSession.builder()
				.master("local")
				.appName("Spark Session Example")
				.config("spark.driver.memory", "2G")
				.config("spark.sql.warehouse.dir", "file:////C:/Users/sgulati/spark-warehouse")
				.getOrCreate();
		
		JavaSparkContext jsc = new JavaSparkContext(sparkSession.sparkContext());
		JavaRDD<Employee> empRDD = jsc.parallelize(Arrays.asList(new Employee("Foo", 1),new Employee("Bar", 2)));
		
		//Dataset<Row> dataset = sparkSession.createDataFrame(empRDD, Employee.class);
		Dataset<Employee> dsEmp = sparkSession.createDataset(empRDD.rdd(), org.apache.spark.sql.Encoders.bean(Employee.class));
		Dataset<Employee> filter = dsEmp.filter(emp->emp.getId()>1);
		//filter.show();
	
		
		Dataset<Row> dfEmp = sparkSession.createDataFrame(empRDD, Employee.class);
		
		Dataset<Row> filter2 = dfEmp.filter(row->row.getInt(0)>1);
		filter2.show();
		
	}
}
