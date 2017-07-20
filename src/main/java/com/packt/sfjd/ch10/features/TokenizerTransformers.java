package com.packt.sfjd.ch10.features;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.ml.feature.Tokenizer;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class TokenizerTransformers {
	public static void main(String[] args) {
		 System.setProperty("hadoop.home.dir", "D:\\hadoop"); 
		SparkSession spark = SparkSession
			      .builder().master("local")
			      .appName("JavaTokenizerExample")
			      .getOrCreate();


			    List<Row> data = Arrays.asList(
			      RowFactory.create(0, "Learning Apache Spark"),
			      RowFactory.create(1, "Spark has API for Java, Scala, and Python"),
			      RowFactory.create(2, "API in above laguage are very richly developed")
			    );

			    StructType schema = new StructType(new StructField[]{
			      new StructField("id", DataTypes.IntegerType, false, Metadata.empty()),
			      new StructField("sentence", DataTypes.StringType, false, Metadata.empty())
			    });

			    Dataset<Row> sentenceDataFrame = spark.createDataFrame(data, schema);

			    Tokenizer tokenizer = new Tokenizer().setInputCol("sentence").setOutputCol("words");

			    Dataset<Row> tokenized = tokenizer.transform(sentenceDataFrame);
			    tokenized.show(false);

			    spark.stop();
	}
}
