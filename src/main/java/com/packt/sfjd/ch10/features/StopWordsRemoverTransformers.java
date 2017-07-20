package com.packt.sfjd.ch10.features;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.ml.feature.StopWordsRemover;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class StopWordsRemoverTransformers {
	public static void main(String[] args) {
		 System.setProperty("hadoop.home.dir", "D:\\hadoop");
		SparkSession spark = SparkSession
			      .builder().master("local")
			      .appName("JavaStopWordsRemoverExample")
			      .getOrCreate();

			

			    List<Row> data = Arrays.asList(
			      RowFactory.create(Arrays.asList("This", "is", "a", "book", "on","Spark")),
			      RowFactory.create(Arrays.asList("The", "language", "used", "is", "Java"))
			    );

			    StructType schema = new StructType(new StructField[]{
			      new StructField(
			        "raw", DataTypes.createArrayType(DataTypes.StringType), false, Metadata.empty())
			    });

			    StopWordsRemover remover = new StopWordsRemover()
					      .setInputCol("raw")
					      .setOutputCol("filtered");
			    
			    Dataset<Row> dataset = spark.createDataFrame(data, schema);
			    remover.transform(dataset).show(false);

			    spark.stop();
	}

}
