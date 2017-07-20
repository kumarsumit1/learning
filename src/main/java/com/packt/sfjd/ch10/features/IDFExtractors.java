package com.packt.sfjd.ch10.features;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.ml.feature.HashingTF;
import org.apache.spark.ml.feature.IDF;
import org.apache.spark.ml.feature.IDFModel;
import org.apache.spark.ml.feature.Tokenizer;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class IDFExtractors {
	public static void main(String[] args) {
		 System.setProperty("hadoop.home.dir", "D:\\hadoop");
		 SparkSession spark = SparkSession
			      .builder()
			      .master("local")
			      .appName("JavaTfIdfExample")
			      .getOrCreate();

			    List<Row> data = Arrays.asList(
			      RowFactory.create(0.0, "Spark"),
			      RowFactory.create(0.0, "Spark"),
			      RowFactory.create(0.5, "MLIB"),
			      RowFactory.create(0.6, "MLIB"),
			      RowFactory.create(0.7, "MLIB"),
			      RowFactory.create(1.0, "ML")
			    );
			    StructType schema = new StructType(new StructField[]{
			      new StructField("label", DataTypes.DoubleType, false, Metadata.empty()),
			      new StructField("words", DataTypes.StringType, false, Metadata.empty())
			    });
			    Dataset<Row> rowData = spark.createDataFrame(data, schema);

			    Tokenizer tokenizer = new Tokenizer().setInputCol("words").setOutputCol("tokenizedWords");
			    Dataset<Row> tokenizedData = tokenizer.transform(rowData);

			    int numFeatures = 20;
			    HashingTF hashingTF = new HashingTF()
			      .setInputCol("tokenizedWords")
			      .setOutputCol("rawFeatures")
			      .setNumFeatures(numFeatures);
			    Dataset<Row> featurizedData = hashingTF.transform(tokenizedData);

			    IDF idf = new IDF().setInputCol("rawFeatures").setOutputCol("features");
			    IDFModel idfModel = idf.fit(featurizedData);

			    Dataset<Row> rescaledData = idfModel.transform(featurizedData);
			    rescaledData.select("label", "features").show(false);

			spark.stop();
		
	}
}
