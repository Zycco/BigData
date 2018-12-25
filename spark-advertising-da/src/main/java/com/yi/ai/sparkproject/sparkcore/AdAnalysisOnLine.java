package com.yi.ai.sparkproject.sparkcore;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import com.yi.ai.sparkproject.util.HourUtils;

import scala.Tuple2;

public class AdAnalysisOnLine {
	public static void main(String[] args) {

		// args : adplatformlog 3
		// 启动命令 ./AdAnalysisWithCondition0712.sh adplatformlog 3
		String tableName = args[0];
		String Condition = args[1];
		String logHour = HourUtils.getLogHour(Integer.valueOf(Condition));
		SparkSession sparkSession = SparkSession.builder().appName("AdAnalysisWithConditionOnLine").enableHiveSupport()
				.config("spark.shuffle.blockTransferService", "nio").getOrCreate();
		sparkSession.sql("use adplatformdatabase");

		// select * from adplatformlog where log_hour = 3 limit 20
		// String initialSql = "select * from " + " " + tableName + " " + "
		// where log_hour = '" + Condition + "'limit 50";

		String initialSql = "select * from " + tableName + " " + "where log_hour = " + logHour;
		System.out.println(initialSql);
		Dataset<Row> logInfoRow = sparkSession.sql(initialSql);
		// System.out.println(initialSql);
		// logInfoRow.show();

		JavaRDD<Row> logInfoRDD = logInfoRow.javaRDD();

		JavaRDD<String> logInfoRDDString = logInfoRDD.map(new Function<Row, String>() {

			private static final long serialVersionUID = 1L;

			@Override
			public String call(Row r1) throws Exception {
				// TODO Auto-generated method stub
				return r1.getString(0) + "|" + r1.getString(1) + "|" + r1.getString(2) + "|" + r1.getString(3) + "|"
						+ r1.getString(4) + "|" + r1.getString(5) + "|" + r1.getString(6) + "|" + r1.getString(7) + "|"
						+ r1.getString(8) + "|" + r1.getString(9) + "|" + r1.getString(10) + "|" + r1.getString(11)
						+ "|" + r1.getString(12) + "|" + r1.getString(13) + "|" + r1.getString(14) + "|"
						+ r1.getString(15) + "|" + r1.getString(16) + "|" + r1.getString(17) + "|" + r1.getString(18)
						+ "|" + r1.getString(19) + "|" + r1.getString(20);
			}

		});
		// JavaRDD<String> logInfoRDDCache = logInfoRDDString.cache();
		AdAnalysisOnLine.getOneHourExpose(sparkSession, logInfoRDDString);
		AdAnalysisOnLine.getOneHourAdserving(sparkSession, logInfoRDDString);

	}

	private static void getOneHourExpose(SparkSession sparkSession, JavaRDD<String> LogInfoRDDString) {

		JavaPairRDD<String, Long> logInfoMapToPair = LogInfoRDDString
				.mapToPair(new PairFunction<String, String, Long>() {

					/**
					 * 
					 */
					private static final long serialVersionUID = 1L;

					@Override
					public Tuple2<String, Long> call(String v1) throws Exception {
						String[] adlogInfo = v1.split("\\|");
						String log_date = adlogInfo[1];
						String log_hour = adlogInfo[2];
						String media_id = adlogInfo[4];
						String video_resource_id = adlogInfo[6];
						String video_name = adlogInfo[7];
						String scene_id = adlogInfo[5];
						String style_id = adlogInfo[9];
						String event_type = adlogInfo[0];
						long sum = 0;
						if (event_type.equals("click")) {
							sum = 100000000;
						} else {
							sum = 1;
						}

						String logInfo = log_date + "|" + log_hour + "|" + media_id + "|" + video_resource_id + "|"
								+ video_name + "|" + scene_id + "|" + style_id;
						return new Tuple2<String, Long>(logInfo, sum);
					}

				});

		JavaPairRDD<String, Long> logInfoReduceByKey = logInfoMapToPair.reduceByKey(new Function2<Long, Long, Long>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Long call(Long v1, Long v2) throws Exception {
				// TODO Auto-generated method stub
				return v1 + v2;
			}
		});

		JavaRDD<Row> logInfoRow = logInfoReduceByKey.map(new Function<Tuple2<String, Long>, Row>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Row call(Tuple2<String, Long> v1) throws Exception {
				long count = v1._2;
				long click_count = (long) Math.floor(count / 100000000);
				long expose_count = (long) (count % 100000000);
				return RowFactory.create(String.valueOf(v1._1.split("\\|")[0]), String.valueOf(v1._1.split("\\|")[1]),
						String.valueOf(v1._1.split("\\|")[2]), String.valueOf(v1._1.split("\\|")[3]),
						String.valueOf(v1._1.split("\\|")[4]), String.valueOf(v1._1.split("\\|")[5]),
						String.valueOf(v1._1.split("\\|")[6]), click_count, expose_count);
			}

		});

		List<StructField> asList = Arrays.asList(DataTypes.createStructField("log_date", DataTypes.StringType, true),
				DataTypes.createStructField("log_hour", DataTypes.StringType, true),
				DataTypes.createStructField("media_id", DataTypes.StringType, true),
				DataTypes.createStructField("video_resource_id", DataTypes.StringType, true),
				DataTypes.createStructField("video_name", DataTypes.StringType, true),
				DataTypes.createStructField("scene_id", DataTypes.StringType, true),
				DataTypes.createStructField("style_id", DataTypes.StringType, true),
				DataTypes.createStructField("click_count", DataTypes.LongType, true),
				DataTypes.createStructField("expose_count", DataTypes.LongType, true));

		StructType schema = DataTypes.createStructType(asList);

		Dataset<Row> createDataFrame = sparkSession.createDataFrame(logInfoRow, schema);

		String url = "jdbc:mysql://172.17.133.113:3306/adsys?useUnicode=true&characterEncoding=utf-8";
		String table = "onehouradexposure";
		Properties connectionProperties = new Properties();
		connectionProperties.put("user", "root");
		connectionProperties.put("password", "123");
		connectionProperties.put("driver", "com.mysql.jdbc.Driver");
		// 每天的数据都append，因为每天的数据都对应一个hive表
		createDataFrame.write().mode(SaveMode.Append).jdbc(url, table, connectionProperties);

	}

	private static void getOneHourAdserving(SparkSession sparkSession, JavaRDD<String> LogInfoRDDString) {

		JavaPairRDD<String, Long> oneHourAdservingPair = LogInfoRDDString
				.mapToPair(new PairFunction<String, String, Long>() {

					private static final long serialVersionUID = 1L;

					@Override
					public Tuple2<String, Long> call(String v1) throws Exception {
						String[] adlogInfo = v1.split("\\|");
						String myDate = adlogInfo[1];
						String myhour = adlogInfo[2];
						String ad_id = adlogInfo[11];
						String ad_name = adlogInfo[12];
						String advertiser_id = adlogInfo[14];
						String event_type = adlogInfo[0];
						long sum = 0;
						if (event_type.equals("click")) {
							sum = 100000000;
						} else {
							sum = 1;
						}
						String Info = myDate + "|" + myhour + "|" + ad_id + "|" + ad_name + "|" + advertiser_id;

						return new Tuple2<String, Long>(Info, sum);
					}

				});
		JavaPairRDD<String, Long> oneHourAdservingreduceByKey = oneHourAdservingPair
				.reduceByKey(new Function2<Long, Long, Long>() {

					/**
					 * 
					 */
					private static final long serialVersionUID = 1L;

					@Override
					public Long call(Long v1, Long v2) throws Exception {

						return v1 + v2;
					}
				});

		JavaRDD<Row> oneHourAdservingRow = oneHourAdservingreduceByKey.map(new Function<Tuple2<String, Long>, Row>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Row call(Tuple2<String, Long> v1) throws Exception {

				long count = v1._2;
				long click_count = (long) Math.floor(count / 100000000);
				long expose_count = (long) (count % 100000000);
				return RowFactory.create(String.valueOf(v1._1.split("\\|")[0]), String.valueOf(v1._1.split("\\|")[1]),
						String.valueOf(v1._1.split("\\|")[2]), String.valueOf(v1._1.split("\\|")[3]),
						String.valueOf(v1._1.split("\\|")[4]), click_count, expose_count);
			}
		});

		List<StructField> asList = Arrays.asList(DataTypes.createStructField("log_date", DataTypes.StringType, true),
				DataTypes.createStructField("log_hour", DataTypes.StringType, true),
				DataTypes.createStructField("ad_id", DataTypes.StringType, true),
				DataTypes.createStructField("ad_name", DataTypes.StringType, true),
				DataTypes.createStructField("advertiser_id", DataTypes.StringType, true),
				DataTypes.createStructField("click_count", DataTypes.LongType, true),
				DataTypes.createStructField("expose_count", DataTypes.LongType, true));

		StructType schema = DataTypes.createStructType(asList);
		Dataset<Row> createDataFrame = sparkSession.createDataFrame(oneHourAdservingRow, schema);

		String url = "jdbc:mysql://172.17.133.113:3306/adsys?useUnicode=true&characterEncoding=utf-8";
		String table = "onehouradserving";
		Properties connectionProperties = new Properties();
		connectionProperties.put("user", "root");
		connectionProperties.put("password", "123");
		connectionProperties.put("driver", "com.mysql.jdbc.Driver");
		// 每天的数据都append，因为每天的数据都对应一个hive表
		createDataFrame.write().mode(SaveMode.Append).jdbc(url, table, connectionProperties);
	}

}
