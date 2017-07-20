package com.packt.sfjd.ch10.ml.algo;

import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.classification.LogisticRegression;
import org.apache.spark.ml.classification.LogisticRegressionModel;
import org.apache.spark.ml.feature.StandardScaler;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.param.ParamMap;
import org.apache.spark.ml.regression.LinearRegression;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.col;

public class LR {
	
	public static void main(String[] args) {
		System.setProperty("hadoop.home.dir", "D:\\Hadoop");
		SparkSession sparkSession = SparkSession
				.builder()
				.master("local")
				.config("spark.sql.warehouse.dir",
						"file:///E:/Hadoop/warehouse")
				.appName("LR").getOrCreate();
		Logger rootLogger = LogManager.getRootLogger();
		rootLogger.setLevel(Level.WARN);
		Dataset<Row> ds=sparkSession.read()
				  .format("csv")
				  .option("header", "true")
				  .option("inferSchema", "true")
				  .option("delimiter", ";")
				  .load("D:\\Testing\\dataset_Facebook.csv");
		ds.show();
		
		Dataset<Row> formattedDF = ds.select(col("Page total likes"),col("Category"),col("Post Month"),col("Post Weekday"),col("Post Hour"),col("Paid"),col("Lifetime People who have liked your Page and engaged with your post"),col("Total Interactions"));
		formattedDF.show();
		//Split the sample into Training and Test data
		Dataset<Row>[] data=	formattedDF.na().drop().randomSplit(new double[]{0.7,0.3});
		System.out.println("We have training examples count :: "+ data[0].count()+" and test examples count ::"+data[1].count());
		
		String[] featuresCols = formattedDF.drop("Total Interactions").columns();
		
    	//This concatenates all feature columns into a single feature vector in a new column "rawFeatures".
		VectorAssembler vectorAssembler = new VectorAssembler().setInputCols(featuresCols).setOutputCol("rawFeatures");
		
		//StandardScaler scaler = new StandardScaler().setInputCol("rawFeatures").setOutputCol("features").setWithStd(true).setWithMean(false);
		
		LinearRegression lr = new LinearRegression().setLabelCol("Total Interactions").setFeaturesCol("rawFeatures")
				  .setMaxIter(10)
				  .setRegParam(0.3)
				  .setElasticNetParam(0.8);

		
		// Chain vectorAssembler and lr in a Pipeline
		Pipeline lrPipeline = new Pipeline().setStages(new PipelineStage[] {vectorAssembler, lr});
		// Train model. 
		PipelineModel lrModel = lrPipeline.fit(data[0]);
		// Make predictions.
		Dataset<Row> lrPredictions = lrModel.transform(data[1]);
		// Select example rows to display.
		lrPredictions.show(20);
		//lrPredictions.select("prediction", "Total Interactions","probability", "rawFeatures").show(200);
	}

}
