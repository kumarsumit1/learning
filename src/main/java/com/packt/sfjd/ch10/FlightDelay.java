package com.packt.sfjd.ch10;

import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

//https://community.hortonworks.com/articles/53903/spark-machine-learning-pipeline-by-example.html
public class FlightDelay {

	public static void main(String[] args) {
		System.setProperty("hadoop.home.dir", "E:\\sumitK\\Hadoop");
		SparkSession sparkSession = SparkSession
				.builder()
				.master("local")
				.config("spark.sql.warehouse.dir",
						"file:///E:/sumitK/Hadoop/warehouse")
				.appName("FlightDelay").getOrCreate();
		Logger rootLogger = LogManager.getRootLogger();
		rootLogger.setLevel(Level.WARN);
		//
		Dataset<String> flight2007 = sparkSession.read()
				.textFile("E:\\sumitK\\Hadoop\\flights_2007.csv.bz2")
				.limit(999999);
		Dataset<String> flight2008 = sparkSession.read()
				.textFile("E:\\sumitK\\Hadoop\\flights_2008.csv.bz2")
				.limit(999999);

		String header = flight2007.head();
		System.out.println("The header in the file is :: " + header);
		// val trainingData = flight2007
		// .filter(x => x != header)
		// .map(x => x.split(","))
		// .filter(x => x(21) == "0")
		// .filter(x => x(17) == "ORD")
		// .filter(x => x(14) != "NA")
		// .map(p => Flight(p(1), p(2), p(3), getMinuteOfDay(p(4)),
		// getMinuteOfDay(p(5)), getMinuteOfDay(p(6)), getMinuteOfDay(p(7)),
		// p(8), p(11).toInt, p(12).toInt, p(13).toInt, p(14).toDouble,
		// p(15).toInt, p(16), p(18).toInt))
		// .toDF
		// / header.s
		// p-> new Flight(p[1], p[2], p[3], getMinuteOfDay(p[4]),
		// getMinuteOfDay(p[5]), getMinuteOfDay(p[6]), getMinuteOfDay(p[7]),
		// p[8], (p[11].equalsIgnoreCase("NA"))?0:Integer.parseInt(p[11]),
		// (p[12].equalsIgnoreCase("NA"))?0:Integer.parseInt(p[12]),
		// (p[13].equalsIgnoreCase("NA"))?0:Integer.parseInt(p[13]),
		// Double.parseDouble(p[14]),
		// (p[15].equalsIgnoreCase("NA"))?0:Integer.parseInt(p[15]), p[16],
		// (p[18].equalsIgnoreCase("NA"))?0:Integer.parseInt(p[18])
		JavaRDD<Flight> trainingRDD = flight2007
				.filter(x -> !x.equalsIgnoreCase(header))
				.javaRDD()
				.map(x -> x.split(","))
				.filter(x -> x[21].equalsIgnoreCase("0"))
				.filter(x -> x[17].equalsIgnoreCase("ORD"))
				.filter(x -> x[14].equalsIgnoreCase("NA"))
				.map(p -> new Flight(p[1], p[2], p[3], getMinuteOfDay(p[4]),
						getMinuteOfDay(p[5]), getMinuteOfDay(p[6]),
						getMinuteOfDay(p[7]), p[8], (p[11]
								.equalsIgnoreCase("NA")) ? 0 : Integer
								.parseInt(p[11]),
						(p[12].equalsIgnoreCase("NA")) ? 0 : Integer
								.parseInt(p[12]),
						(p[13].equalsIgnoreCase("NA")) ? 0 : Integer
								.parseInt(p[13]),
						(p[14].equalsIgnoreCase("NA")) ? 0 : Double
								.parseDouble(p[14]), (p[15]
								.equalsIgnoreCase("NA")) ? 0 : Integer
								.parseInt(p[15]), p[16], (p[18]
								.equalsIgnoreCase("NA")) ? 0 : Integer
								.parseInt(p[18])));
		Dataset<Row> trainingData = sparkSession.createDataFrame(trainingRDD,Flight.class);

		// String header1 = flight2008.head();
		JavaRDD<Flight> testingRDD = flight2008
				.filter(x -> !x.equalsIgnoreCase(header))
				.javaRDD()
				.map(x -> x.split(","))
				.filter(x -> x[21].equalsIgnoreCase("0"))
				.filter(x -> x[17].equalsIgnoreCase("ORD"))
				.filter(x -> x[14].equalsIgnoreCase("NA"))
				.map(p -> new Flight(p[1], p[2], p[3], getMinuteOfDay(p[4]),
						getMinuteOfDay(p[5]), getMinuteOfDay(p[6]),
						getMinuteOfDay(p[7]), p[8], (p[11]
								.equalsIgnoreCase("NA")) ? 0 : Integer
								.parseInt(p[11]),
						(p[12].equalsIgnoreCase("NA")) ? 0 : Integer
								.parseInt(p[12]),
						(p[13].equalsIgnoreCase("NA")) ? 0 : Integer
								.parseInt(p[13]),
						(p[14].equalsIgnoreCase("NA")) ? 0 : Double
								.parseDouble(p[14]), (p[15]
								.equalsIgnoreCase("NA")) ? 0 : Integer
								.parseInt(p[15]), p[16], (p[18]
								.equalsIgnoreCase("NA")) ? 0 : Integer
								.parseInt(p[18])));

		Dataset<Row> testingData = sparkSession.createDataFrame(testingRDD,	Flight.class);

		System.out.println("The training data count is "+trainingData.count());

		System.out.println("The testing data count is "+testingData.count());

		//sparkSession.stop();

	}

	// calculate minuted from midnight, input is military time format
	private static Integer getMinuteOfDay(String depTime) {
		System.out.println("the deptTime is:" + depTime);
		return (depTime.equalsIgnoreCase("NA")) ? 0
				: ((Integer.parseInt(depTime) / 100) * 60 + (Integer.parseInt(depTime) % 100));
	}

}
