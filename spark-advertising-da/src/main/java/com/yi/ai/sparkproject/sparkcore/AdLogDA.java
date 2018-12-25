package com.yi.ai.sparkproject.sparkcore;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import com.yi.ai.sparkproject.constant.Constants;

import scala.Tuple2;

/**
 * @author May
 * 
 * 
 *         广告产品二期需求
 * 
 *         分析点击量 曝光量 纬度（时间 日期 省市 性别 等）
 * 
 *         本地测试版本
 *
 */
public class AdLogDA {

	public static void main(String[] args) {

		SparkSession sparkSession = SparkSession.builder().appName("AdSysStaAnalysis").master(Constants.SPARK_LOCAL)
				.config("spark.sql.warehouse.dir", "D:\\Spark\\spark-warehouse").getOrCreate();

		// 获取基础的数据 本地测试和Spark on hive 两个版本
		JavaRDD<String> basicLogInfoLocal = AdLogDA.getBasicLogInfo(sparkSession);

		AdLogDA.getOneHourExpose(sparkSession, basicLogInfoLocal);

	}

	private static JavaRDD<String> getBasicLogInfo(SparkSession sparkSession) {

		@SuppressWarnings("resource")
		JavaSparkContext jsc = new JavaSparkContext(sparkSession.sparkContext());

		JavaRDD<String> mokeFile = jsc.textFile("./FlumeData.txt");

		return mokeFile;
	}

	private static void getOneHourExpose(SparkSession sparkSession, JavaRDD<String> basicLogInfoLocal) {

		JavaPairRDD<String, String> logInfoMapToPair = basicLogInfoLocal
				.mapToPair(new PairFunction<String, String, String>() {

					/**
					 * 
					 */
					private static final long serialVersionUID = 1L;

					@Override
					public Tuple2<String, String> call(String v1) throws Exception {
						String[] adlogInfo = v1.split("\\|");
						String log_date = adlogInfo[1];
						String log_hour = adlogInfo[2];

						String media_id = adlogInfo[4];

						String video_resource_id = adlogInfo[6];
						String video_name = adlogInfo[7];
						String scene_id = adlogInfo[5];
						String style_id = adlogInfo[9];

						String logInfo = media_id + "|" + video_resource_id + "|" + video_name + "|" + scene_id + "|"
								+ style_id;
						return new Tuple2<String, String>(log_date + "|" + log_hour, logInfo);
					}

				});

		JavaPairRDD<String, Iterable<String>> groupByKey = logInfoMapToPair.groupByKey();

		JavaRDD<Row> map = groupByKey.map(new Function<Tuple2<String, Iterable<String>>, Row>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Row call(Tuple2<String, Iterable<String>> v1) throws Exception {
				// TODO Auto-generated method stub
				String _1 = v1._1;
				String[] split = _1.split("\\|");
				String logData = split[0];
				String logHour = split[1];
				Iterable<String> _2 = v1._2;
				String valueOf = String.valueOf(_2);

				String logInfo = valueOf.replaceAll("[\\[\\]]", "");
				return RowFactory.create(logData, logHour, logInfo);
			}

		});

		List<StructField> asList = Arrays.asList(DataTypes.createStructField("log_data", DataTypes.StringType, true),
				DataTypes.createStructField("log_time", DataTypes.StringType, true),
				DataTypes.createStructField("log_info", DataTypes.StringType, true));

		StructType schema = DataTypes.createStructType(asList);

		Dataset<Row> createDataFrame = sparkSession.createDataFrame(map, schema);

		String url = "jdbc:mysql://localhost:3306/adsys?useUnicode=true&characterEncoding=utf-8";
		String table = "testInfo";
		Properties connectionProperties = new Properties();
		connectionProperties.put("user", "root");
		connectionProperties.put("password", "123123");
		connectionProperties.put("driver", "com.mysql.jdbc.Driver");
		// 每天的数据都append，因为每天的数据都对应一个hive表
		createDataFrame.write().mode(SaveMode.Overwrite).jdbc(url, table, connectionProperties);

	}

}
