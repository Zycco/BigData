package com.yi.ai.sparkproject.test.sparkcoretest;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
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

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

import scala.Tuple2;

public class TestAdSysLogStatsLocal {

	public static void main(String[] args) {
		SparkSession sparkSession = SparkSession.builder().appName("testjson").master("local")
				.config("spark.sql.warehouse.dir", "D:\\Spark\\spark-warehouse").getOrCreate();

		// 获取基础的数据 本地测试和Spark on hive 两个版本

		JavaSparkContext jsc = new JavaSparkContext(sparkSession.sparkContext());

		JavaRDD<String> mokeFile = jsc.textFile("./logjson.txt");

		// 获取基础的数据过滤掉关键字段不为空的数据
		JavaRDD<String> basicLogFilter = TestAdSysLogStatsLocal.getBasicLog(sparkSession, mokeFile);

		JavaRDD<String> basicLogFilterCache = basicLogFilter.cache();

		// 媒资管理平台获取广告曝光统计
		// TestAdSysLogStatsLocal.getOneHourExpose(sparkSession,
		// basicLogFilterCache);
		// 获取广告投放统计
		// TestAdSysLogStatsLocal.getOneHourServing(sparkSession,
		// basicLogFilterCache);
		// Yi+运营平台 和 广告投放平台 广告曝光统计
		TestAdSysLogStatsLocal.getMasterOneHourExpose(sparkSession, basicLogFilterCache);
		// 运营平台设备统计
		// TestAdSysLogStatsLocal.devicesStats(sparkSession,
		// basicLogFilterCache);
		// 运营平台媒体主统计
		// TestAdSysLogStatsLocal.mediaMasterStats(sparkSession,
		// basicLogFilterCache);
		// 设备行为信息统计 输入的是最开始的数据 不是过滤后的
		// TestAdSysLogStatsLocal.devInfo(sparkSession, mokeFile);

	}

	/**
	 * @param sparkSession
	 * @param basicLog
	 * @return
	 * 
	 * 		获取基础数据 并过滤关键字段不为空的数据
	 */
	private static JavaRDD<String> getBasicLog(SparkSession sparkSession, JavaRDD<String> basicLog) {

		JavaRDD<String> filter = basicLog.filter(new Function<String, Boolean>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@SuppressWarnings("null")
			@Override
			public Boolean call(String v1) throws Exception {
				// TODO Auto-generated method stub
				JSONObject jsonObject = JSON.parseObject(v1);
				String log_date = jsonObject.getString("date");
				String log_hour = jsonObject.getString("hour");
				String media_id = jsonObject.getString("media_id");
				String media_name = jsonObject.getString("media_name");
				String video_resource_id = jsonObject.getString("video_resource_id");
				String video_name = jsonObject.getString("video_name");
				String scene_id = jsonObject.getString("scene_id");
				String style_id = jsonObject.getString("style_id");
				String event_type = jsonObject.getString("action");
				String ad_id = jsonObject.getString("ad_id");
				String ad_name = jsonObject.getString("ad_name");
				String advertiser_id = jsonObject.getString("advertiser_id");
				String advertiser_name = jsonObject.getString("advertiser_name");
				String platform = jsonObject.getString("platform");
				String user_current_province = jsonObject.getString("user_current_province");
				String user_current_city = jsonObject.getString("user_current_city");
				String ui_sex = jsonObject.getString("ui_sex");
				String ctype = jsonObject.getString("ctype");
				String media_industry = jsonObject.getString("media_industry");
				return (log_date != null && !"".equals(log_date.trim()) && log_hour != null
						&& !"".equals(log_hour.trim()) && media_id != null && !"".equals(media_id.trim())
						&& media_name != null && !"".equals(media_name.trim()) && video_resource_id != null
						&& !"".equals(video_resource_id.trim()) && video_name != null && !"".equals(video_name.trim())
						&& scene_id != null && !"".equals(scene_id.trim()) && style_id != null
						&& !"".equals(style_id.trim()) && event_type != null && !"".equals(event_type.trim())
						&& ad_id != null && !"".equals(ad_id.trim()) && ad_name != null && !"".equals(ad_name.trim())
						&& advertiser_id != null && !"".equals(advertiser_id.trim()) && advertiser_name != null
						&& !"".equals(advertiser_name.trim()) && platform != null && !"".equals(platform.trim())
						&& user_current_province != null && !"".equals(user_current_province.trim())
						&& user_current_city != null && !"".equals(user_current_city.trim()) && ui_sex != null
						&& !"".equals(ui_sex.trim()) && ctype != null && !"".equals(ctype.trim())
						&& media_industry != null && !"".equals(media_industry.trim()));
			}

		});

		return filter;

	}

	/**
	 * @param sparkSession
	 * @param basicLogFilter
	 * 
	 *            获取一个小时的曝光量
	 */
	private static void getOneHourExpose(SparkSession sparkSession, JavaRDD<String> basicLogFilter) {

		JavaPairRDD<String, Long> mapToPair = basicLogFilter.mapToPair(new PairFunction<String, String, Long>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<String, Long> call(String t) throws Exception {
				// TODO Auto-generated method stub
				JSONObject jsonObject = JSON.parseObject(t);
				String log_date = jsonObject.getString("date");
				String log_hour = jsonObject.getString("hour");
				String media_id = jsonObject.getString("media_id");
				String media_name = jsonObject.getString("media_name");
				String video_resource_id = jsonObject.getString("video_resource_id");
				String video_name = jsonObject.getString("video_name");
				String scene_id = jsonObject.getString("scene_id");
				String style_id = jsonObject.getString("style_id");
				String event_type = jsonObject.getString("action");
				long sum = 0;
				if (event_type.equals("click")) {
					sum = 100000000;
				} else if (event_type.equals("expose")) {
					sum = 1;
				} else {
					sum = 0;
				}

				String logInfo = log_date + "|" + log_hour + "|" + media_id + "|" + media_name + "|" + video_resource_id
						+ "|" + video_name + "|" + scene_id + "|" + style_id;
				System.out.println("广告曝光统计接收到的log数据为-------→" + logInfo);
				return new Tuple2<String, Long>(logInfo, sum);
			}
		});

		JavaPairRDD<String, Long> reduceByKey = mapToPair.reduceByKey(new Function2<Long, Long, Long>() {

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

		JavaRDD<Row> map = reduceByKey.map(new Function<Tuple2<String, Long>, Row>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Row call(Tuple2<String, Long> v1) throws Exception {
				// TODO Auto-generated method stub
				long count = v1._2;
				long click_count = (long) Math.floor(count / 100000000);
				long expose_count = (long) (count % 100000000);
				return RowFactory.create(String.valueOf(v1._1.split("\\|")[0]), String.valueOf(v1._1.split("\\|")[1]),
						String.valueOf(v1._1.split("\\|")[2]), String.valueOf(v1._1.split("\\|")[3]),
						String.valueOf(v1._1.split("\\|")[4]), String.valueOf(v1._1.split("\\|")[5]),
						String.valueOf(v1._1.split("\\|")[6]), String.valueOf(v1._1.split("\\|")[7]), click_count,
						expose_count);
			}
		});

		List<StructField> asList = Arrays.asList(DataTypes.createStructField("log_date", DataTypes.StringType, true),
				DataTypes.createStructField("log_hour", DataTypes.StringType, true),
				DataTypes.createStructField("media_id", DataTypes.StringType, true),
				DataTypes.createStructField("media_name", DataTypes.StringType, true),
				DataTypes.createStructField("video_resource_id", DataTypes.StringType, true),
				DataTypes.createStructField("video_name", DataTypes.StringType, true),
				DataTypes.createStructField("scene_id", DataTypes.StringType, true),
				DataTypes.createStructField("style_id", DataTypes.StringType, true),
				DataTypes.createStructField("click_count", DataTypes.LongType, true),
				DataTypes.createStructField("expose_count", DataTypes.LongType, true));

		StructType schema = DataTypes.createStructType(asList);

		Dataset<Row> createDataFrame = sparkSession.createDataFrame(map, schema);

		String url = "jdbc:mysql://localhost:3306/adsys?useUnicode=true&characterEncoding=utf-8";
		String table = "onehouradexposure";
		Properties connectionProperties = new Properties();
		connectionProperties.put("user", "root");
		connectionProperties.put("password", "123123");
		connectionProperties.put("driver", "com.mysql.jdbc.Driver");
		// 每天的数据都append，因为每天的数据都对应一个hive表
		createDataFrame.write().mode(SaveMode.Append).jdbc(url, table, connectionProperties);
	}

	/**
	 * @param sparkSession
	 * @param basicLogFilterCache
	 * 
	 *            获取一个小时的广告投放统计
	 */
	private static void getOneHourServing(SparkSession sparkSession, JavaRDD<String> basicLogFilterCache) {
		// TODO Auto-generated method stub

		JavaPairRDD<String, Long> mapToPair = basicLogFilterCache.mapToPair(new PairFunction<String, String, Long>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<String, Long> call(String t) throws Exception {
				// TODO Auto-generated method stub
				JSONObject jsonObject = JSON.parseObject(t);
				String log_date = jsonObject.getString("date");
				String log_hour = jsonObject.getString("hour");
				String ad_id = jsonObject.getString("ad_id");
				String ad_name = jsonObject.getString("ad_name");
				String advertiser_id = jsonObject.getString("advertiser_id");
				String advertiser_name = jsonObject.getString("advertiser_name");
				String event_type = jsonObject.getString("action");
				long sum = 0;
				if (event_type.equals("click")) {
					sum = 100000000;
				} else if (event_type.equals("expose")) {
					sum = 1;
				} else {
					sum = 0;
				}

				String logInfo = log_date + "|" + log_hour + "|" + ad_id + "|" + ad_name + "|" + advertiser_id + "|"
						+ advertiser_name;
				System.out.println("广告投放统计接收到的log数据为-------→" + logInfo);
				return new Tuple2<String, Long>(logInfo, sum);
			}

		});

		JavaPairRDD<String, Long> reduceByKey = mapToPair.reduceByKey(new Function2<Long, Long, Long>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Long call(Long v1, Long v2) throws Exception {

				return v1 + v2;
			}

		});

		JavaRDD<Row> map = reduceByKey.map(new Function<Tuple2<String, Long>, Row>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Row call(Tuple2<String, Long> v1) throws Exception {
				// TODO Auto-generated method stub
				long count = v1._2;
				long click_count = (long) Math.floor(count / 100000000);
				long expose_count = (long) (count % 100000000);
				return RowFactory.create(String.valueOf(v1._1.split("\\|")[0]), String.valueOf(v1._1.split("\\|")[1]),
						String.valueOf(v1._1.split("\\|")[2]), String.valueOf(v1._1.split("\\|")[3]),
						String.valueOf(v1._1.split("\\|")[4]), String.valueOf(v1._1.split("\\|")[5]), click_count,
						expose_count);
			}
		});

		List<StructField> asList = Arrays.asList(DataTypes.createStructField("log_date", DataTypes.StringType, true),
				DataTypes.createStructField("log_hour", DataTypes.StringType, true),
				DataTypes.createStructField("ad_id", DataTypes.StringType, true),
				DataTypes.createStructField("ad_name", DataTypes.StringType, true),
				DataTypes.createStructField("advertiser_id", DataTypes.StringType, true),
				DataTypes.createStructField("advertiser_name", DataTypes.StringType, true),
				DataTypes.createStructField("click_count", DataTypes.LongType, true),
				DataTypes.createStructField("expose_count", DataTypes.LongType, true));

		StructType schema = DataTypes.createStructType(asList);

		Dataset<Row> createDataFrame = sparkSession.createDataFrame(map, schema);

		String url = "jdbc:mysql://localhost:3306/adsys?useUnicode=true&characterEncoding=utf-8";
		String table = "onehouradserving";
		Properties connectionProperties = new Properties();
		connectionProperties.put("user", "root");
		connectionProperties.put("password", "123123");
		connectionProperties.put("driver", "com.mysql.jdbc.Driver");
		// 每天的数据都append，因为每天的数据都对应一个hive表
		createDataFrame.write().mode(SaveMode.Append).jdbc(url, table, connectionProperties);

	}

	private static void getMasterOneHourExpose(SparkSession sparkSession, JavaRDD<String> basicLogFilter) {
		// TODO Auto-generated method stub
		JavaPairRDD<String, Long> mapToPair = basicLogFilter.mapToPair(new PairFunction<String, String, Long>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<String, Long> call(String t) throws Exception {
				// TODO Auto-generated method stub
				JSONObject jsonObject = JSON.parseObject(t);
				String log_date = jsonObject.getString("date");
				String log_hour = jsonObject.getString("hour");
				String user_id = jsonObject.getString("user_id");
				String media_id = jsonObject.getString("media_id");
				String media_name = jsonObject.getString("media_name");
				String advertiser_id = jsonObject.getString("advertiser_id");
				String advertiser_name = jsonObject.getString("advertiser_name");
				String ad_id = jsonObject.getString("ad_id");
				String ad_name = jsonObject.getString("ad_name");
				String scene_id = jsonObject.getString("scene_id");
				String style_id = jsonObject.getString("style_id");
				String platform = jsonObject.getString("platform");
				String user_current_province = jsonObject.getString("user_current_province");
				String user_current_city = jsonObject.getString("user_current_city");
				String ui_sex = jsonObject.getString("ui_sex");
				String ctype = jsonObject.getString("ctype");
				String event_type = jsonObject.getString("action");
				long sum = 0;
				if (event_type.equals("click")) {
					sum = 100000000;
				} else if (event_type.equals("expose")) {
					sum = 1;
				} else {
					sum = 0;
				}

				String logInfo = log_date + "|" + log_hour + "|" + user_id + "|" + media_id + "|" + media_name + "|"
						+ advertiser_id + "|" + advertiser_name + "|" + ad_id + "|" + ad_name + "|" + scene_id + "|"
						+ style_id + "|" + platform + "|" + user_current_province + "|" + user_current_city + "|"
						+ ui_sex + "|" + ctype;
				System.out.println("运营平台和广告平台曝光统计接收到的log数据为-------→" + logInfo);
				return new Tuple2<String, Long>(logInfo, sum);
			}
		});

		JavaPairRDD<String, Long> reduceByKey = mapToPair.reduceByKey(new Function2<Long, Long, Long>() {

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

		JavaRDD<Row> map = reduceByKey.map(new Function<Tuple2<String, Long>, Row>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Row call(Tuple2<String, Long> v1) throws Exception {
				// TODO Auto-generated method stub
				long count = v1._2;
				long click_count = (long) Math.floor(count / 100000000);
				long expose_count = (long) (count % 100000000);
				return RowFactory.create(String.valueOf(v1._1.split("\\|")[0]), String.valueOf(v1._1.split("\\|")[1]),
						String.valueOf(v1._1.split("\\|")[2]), String.valueOf(v1._1.split("\\|")[3]),
						String.valueOf(v1._1.split("\\|")[4]), String.valueOf(v1._1.split("\\|")[5]),
						String.valueOf(v1._1.split("\\|")[6]), String.valueOf(v1._1.split("\\|")[7]),
						String.valueOf(v1._1.split("\\|")[8]), String.valueOf(v1._1.split("\\|")[9]),
						String.valueOf(v1._1.split("\\|")[10]), String.valueOf(v1._1.split("\\|")[11]),
						String.valueOf(v1._1.split("\\|")[12]), String.valueOf(v1._1.split("\\|")[13]),
						String.valueOf(v1._1.split("\\|")[14]), String.valueOf(v1._1.split("\\|")[15]), click_count,
						expose_count);
			}
		});

		List<StructField> asList = Arrays.asList(DataTypes.createStructField("log_date", DataTypes.StringType, true),
				DataTypes.createStructField("log_hour", DataTypes.StringType, true),
				DataTypes.createStructField("user_id", DataTypes.StringType, true),
				DataTypes.createStructField("media_id", DataTypes.StringType, true),
				DataTypes.createStructField("media_name", DataTypes.StringType, true),
				DataTypes.createStructField("advertiser_id", DataTypes.StringType, true),
				DataTypes.createStructField("advertiser_name", DataTypes.StringType, true),
				DataTypes.createStructField("ad_id", DataTypes.StringType, true),
				DataTypes.createStructField("ad_name", DataTypes.StringType, true),
				DataTypes.createStructField("scene_id", DataTypes.StringType, true),
				DataTypes.createStructField("style_id", DataTypes.StringType, true),
				DataTypes.createStructField("platform", DataTypes.StringType, true),
				DataTypes.createStructField("province", DataTypes.StringType, true),
				DataTypes.createStructField("city", DataTypes.StringType, true),
				DataTypes.createStructField("sex", DataTypes.StringType, true),
				DataTypes.createStructField("ctype", DataTypes.StringType, true),
				DataTypes.createStructField("click_count", DataTypes.LongType, true),
				DataTypes.createStructField("expose_count", DataTypes.LongType, true));

		StructType schema = DataTypes.createStructType(asList);

		Dataset<Row> createDataFrame = sparkSession.createDataFrame(map, schema);

		String url = "jdbc:mysql://localhost:3306/adsys?useUnicode=true&characterEncoding=utf-8";
		String table = "ad_expose_matser";
		Properties connectionProperties = new Properties();
		connectionProperties.put("user", "root");
		connectionProperties.put("password", "123123");
		connectionProperties.put("driver", "com.mysql.jdbc.Driver");
		// 每天的数据都append，因为每天的数据都对应一个hive表
		createDataFrame.write().mode(SaveMode.Overwrite).jdbc(url, table, connectionProperties);
	}

	private static void devicesStats(SparkSession sparkSession, JavaRDD<String> basicLogFilter) {
		// TODO Auto-generated method stub
		JavaPairRDD<String, Long> mapToPair = basicLogFilter.mapToPair(new PairFunction<String, String, Long>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<String, Long> call(String t) throws Exception {
				// TODO Auto-generated method stub
				JSONObject jsonObject = JSON.parseObject(t);
				String log_date = jsonObject.getString("date");
				String log_hour = jsonObject.getString("hour");
				String media_id = jsonObject.getString("media_id");
				String media_name = jsonObject.getString("media_name");
				String platform = jsonObject.getString("platform");
				String user_current_province = jsonObject.getString("user_current_province");
				String user_current_city = jsonObject.getString("user_current_city");
				String ui_sex = jsonObject.getString("ui_sex");
				String logInfo = log_date + "|" + log_hour + "|" + media_id + "|" + media_name + "|" + platform + "|"
						+ user_current_province + "|" + user_current_city + "|" + ui_sex;
				System.out.println("运营平台和广告平台设备统计接收到的log数据为-------→" + logInfo);
				return new Tuple2<String, Long>(logInfo, (long) 1);
			}
		});

		JavaPairRDD<String, Long> reduceByKey = mapToPair.reduceByKey(new Function2<Long, Long, Long>() {

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

		JavaRDD<Row> map = reduceByKey.map(new Function<Tuple2<String, Long>, Row>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Row call(Tuple2<String, Long> v1) throws Exception {
				// TODO Auto-generated method stub
				long dev_count = v1._2;
				return RowFactory.create(String.valueOf(v1._1.split("\\|")[0]), String.valueOf(v1._1.split("\\|")[1]),
						String.valueOf(v1._1.split("\\|")[2]), String.valueOf(v1._1.split("\\|")[3]),
						String.valueOf(v1._1.split("\\|")[4]), String.valueOf(v1._1.split("\\|")[5]),
						String.valueOf(v1._1.split("\\|")[6]), String.valueOf(v1._1.split("\\|")[7]), dev_count);
			}
		});

		List<StructField> asList = Arrays.asList(DataTypes.createStructField("log_date", DataTypes.StringType, true),
				DataTypes.createStructField("log_hour", DataTypes.StringType, true),
				DataTypes.createStructField("media_id", DataTypes.StringType, true),
				DataTypes.createStructField("media_name", DataTypes.StringType, true),
				DataTypes.createStructField("platform", DataTypes.StringType, true),
				DataTypes.createStructField("province", DataTypes.StringType, true),
				DataTypes.createStructField("city", DataTypes.StringType, true),
				DataTypes.createStructField("sex", DataTypes.StringType, true),
				DataTypes.createStructField("dev_count", DataTypes.LongType, true));

		StructType schema = DataTypes.createStructType(asList);

		Dataset<Row> createDataFrame = sparkSession.createDataFrame(map, schema);

		String url = "jdbc:mysql://localhost:3306/adsys?useUnicode=true&characterEncoding=utf-8";
		String table = "devices_stats";
		Properties connectionProperties = new Properties();
		connectionProperties.put("user", "root");
		connectionProperties.put("password", "123123");
		connectionProperties.put("driver", "com.mysql.jdbc.Driver");
		// 每天的数据都append，因为每天的数据都对应一个hive表
		createDataFrame.write().mode(SaveMode.Append).jdbc(url, table, connectionProperties);
	}

	private static void mediaMasterStats(SparkSession sparkSession, JavaRDD<String> basicLogFilter) {
		// TODO Auto-generated method stub
		JavaPairRDD<String, Long> mapToPair = basicLogFilter.mapToPair(new PairFunction<String, String, Long>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<String, Long> call(String t) throws Exception {
				// TODO Auto-generated method stub
				JSONObject jsonObject = JSON.parseObject(t);
				String log_date = jsonObject.getString("date");
				String log_hour = jsonObject.getString("hour");
				String media_industry = jsonObject.getString("media_industry");
				String media_name = jsonObject.getString("media_name");
				String logInfo = log_date + "|" + log_hour + "|" + media_industry + "|" + media_name;
				System.out.println("媒体主行业曝光统计log数据为-------→" + logInfo);
				return new Tuple2<String, Long>(logInfo, (long) 1);
			}
		});

		JavaPairRDD<String, Long> reduceByKey = mapToPair.reduceByKey(new Function2<Long, Long, Long>() {

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

		JavaRDD<Row> map = reduceByKey.map(new Function<Tuple2<String, Long>, Row>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Row call(Tuple2<String, Long> v1) throws Exception {
				// TODO Auto-generated method stub
				long expose_count = v1._2;
				return RowFactory.create(String.valueOf(v1._1.split("\\|")[0]), String.valueOf(v1._1.split("\\|")[1]),
						String.valueOf(v1._1.split("\\|")[2]), String.valueOf(v1._1.split("\\|")[3]), expose_count);
			}
		});

		List<StructField> asList = Arrays.asList(DataTypes.createStructField("log_date", DataTypes.StringType, true),
				DataTypes.createStructField("log_hour", DataTypes.StringType, true),
				DataTypes.createStructField("media_industry", DataTypes.StringType, true),
				DataTypes.createStructField("media_name", DataTypes.StringType, true),
				DataTypes.createStructField("expose_count", DataTypes.LongType, true));

		StructType schema = DataTypes.createStructType(asList);
		Dataset<Row> createDataFrame = sparkSession.createDataFrame(map, schema);
		String url = "jdbc:mysql://localhost:3306/adsys?useUnicode=true&characterEncoding=utf-8";
		String table = "medowner_adexp_stats";
		Properties connectionProperties = new Properties();
		connectionProperties.put("user", "root");
		connectionProperties.put("password", "123123");
		connectionProperties.put("driver", "com.mysql.jdbc.Driver");
		// 每天的数据都append，因为每天的数据都对应一个hive表
		createDataFrame.write().mode(SaveMode.Append).jdbc(url, table, connectionProperties);
	}

	private static void devInfo(SparkSession sparkSession, JavaRDD<String> mokeFile) {

		JavaRDD<String> filter = mokeFile.filter(new Function<String, Boolean>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@SuppressWarnings("null")
			@Override
			public Boolean call(String v1) throws Exception {
				JSONObject jsonObject = JSON.parseObject(v1);
				String log_time = jsonObject.getString("time");
				String log_date = jsonObject.getString("date");
				String log_hour = jsonObject.getString("hour");
				String device_fingerprint = jsonObject.getString("device_fingerprint");
				String device_mac = jsonObject.getString("device_mac");
				String action = jsonObject.getString("action");
				return (log_date != null && !"".equals(log_date.trim()) && log_hour != null
						&& !"".equals(log_hour.trim()) && device_fingerprint != null
						&& !"".equals(device_fingerprint.trim()) && device_mac != null && !"".equals(device_mac.trim())
						&& action != null && !"".equals(action.trim()) && log_time != null
						&& !"".equals(log_time.trim()));

			}

		});

		JavaRDD<String> logInfoMap = filter.map(new Function<String, String>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public String call(String v1) throws Exception {
				// TODO Auto-generated method stub

				JSONObject jsonObject = JSON.parseObject(v1);
				String log_date = jsonObject.getString("date");
				String log_hour = jsonObject.getString("hour");
				String device_fingerprint = jsonObject.getString("device_fingerprint");
				String device_cpu = "未获取";
				if (jsonObject.getString("device_cpu") != null && jsonObject.getString("device_cpu").length() != 0) {
					device_cpu = jsonObject.getString("device_cpu");
				}
				String device_brand = "未获取";
				if (jsonObject.getString("device_brand") != null
						&& jsonObject.getString("device_brand").length() != 0) {
					device_brand = jsonObject.getString("device_brand");
				}
				String device_model = "未获取";
				if (jsonObject.getString("device_model") != null
						&& jsonObject.getString("device_model").length() != 0) {
					device_model = jsonObject.getString("device_model");
				}
				String device_name = "未获取";
				if (jsonObject.getString("device_name") != null && jsonObject.getString("device_name").length() != 0) {
					device_name = jsonObject.getString("device_name");
				}
				String device_memory = "未获取";
				if (jsonObject.getString("device_memory") != null
						&& jsonObject.getString("device_memory").length() != 0) {
					device_memory = jsonObject.getString("device_memory");
				}
				String device_mac = "未获取";
				if (jsonObject.getString("device_mac") != null && jsonObject.getString("device_mac").length() != 0) {
					device_mac = jsonObject.getString("device_mac");
				}
				String device_sys_version = "未获取";
				if (jsonObject.getString("device_sys_version") != null
						&& jsonObject.getString("device_sys_version").length() != 0) {
					device_sys_version = jsonObject.getString("device_sys_version");
				}
				String device_network = "未获取";
				if (jsonObject.getString("device_network") != null
						&& jsonObject.getString("device_network").length() != 0) {
					device_network = jsonObject.getString("device_network");
				}
				String device_carrier = "未获取";
				if (jsonObject.getString("device_carrier") != null
						&& jsonObject.getString("device_carrier").length() != 0) {
					device_carrier = jsonObject.getString("device_carrier");
				}
				String user_id = "未获取";
				if (jsonObject.getString("user_id") != null && jsonObject.getString("user_id").length() != 0) {
					user_id = jsonObject.getString("user_id");
				}
				String user_name = "未获取";
				if (jsonObject.getString("user_name") != null && jsonObject.getString("user_name").length() != 0) {
					user_name = jsonObject.getString("user_name");
				}
				String ui_age = "未获取";
				if (jsonObject.getString("ui_age") != null && jsonObject.getString("ui_age").length() != 0) {
					ui_age = jsonObject.getString("ui_age");
				}
				String ui_sex = "未获取";
				if (jsonObject.getString("ui_sex") != null && jsonObject.getString("ui_sex").length() != 0) {
					ui_sex = jsonObject.getString("ui_sex");
				}
				String user_interest = "未获取";
				if (jsonObject.getString("user_interest") != null
						&& jsonObject.getString("user_interest").length() != 0) {
					user_interest = jsonObject.getString("user_interest");
				}
				String user_current_province = "未获取";
				if (jsonObject.getString("user_current_province") != null
						&& jsonObject.getString("user_current_province").length() != 0) {
					user_current_province = jsonObject.getString("user_current_province");
				}
				String user_current_city = "未获取";
				if (jsonObject.getString("user_current_city") != null
						&& jsonObject.getString("user_current_city").length() != 0) {
					user_current_city = jsonObject.getString("user_current_city");
				}
				String behavior = "未获取";
				if (jsonObject.getString("action") != null && jsonObject.getString("action").length() != 0) {
					behavior = jsonObject.getString("action");
				}
				String media_name = "未获取";
				if (jsonObject.getString("media_name") != null && jsonObject.getString("media_name").length() != 0) {
					media_name = jsonObject.getString("media_name");
				}
				String video_name = "未获取";
				if (jsonObject.getString("video_name") != null && jsonObject.getString("video_name").length() != 0) {
					video_name = jsonObject.getString("video_name");
				}
				String video_episode = "未获取";
				if (jsonObject.getString("video_episode") != null
						&& jsonObject.getString("video_episode").length() != 0) {
					video_episode = jsonObject.getString("video_episode");
				}
				String ad_id = "未获取";
				if (jsonObject.getString("ad_id") != null && jsonObject.getString("ad_id").length() != 0) {
					ad_id = jsonObject.getString("ad_id");
				}
				String ad_name = "未获取";
				if (jsonObject.getString("ad_name") != null && jsonObject.getString("ad_name").length() != 0) {
					ad_name = jsonObject.getString("ad_name");
				}

				String log_time = "未获取";
				if (jsonObject.getString("time") != null && jsonObject.getString("time").length() != 0) {
					log_time = jsonObject.getString("time");
				}

				String logInfo = log_date + "|" + log_hour + "|" + device_fingerprint + "|" + device_cpu + "|"
						+ device_brand + "|" + device_model + "|" + device_name + "|" + device_memory + "|" + device_mac
						+ "|" + device_sys_version + "|" + device_network + "|" + device_carrier + "|" + user_id + "|"
						+ user_name + "|" + ui_age + "|" + ui_sex + "|" + user_interest + "|" + user_current_province
						+ "|" + user_current_city + "|" + behavior + "|" + media_name + "|" + video_name + "|"
						+ video_episode + "|" + ad_id + "|" + ad_name + "|" + log_time;
				return logInfo;
			}
		});

		JavaRDD<Row> map = logInfoMap.map(new Function<String, Row>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Row call(String v1) throws Exception {
				// TODO Auto-generated method stub
				return RowFactory.create(String.valueOf(v1.split("\\|")[0]), String.valueOf(v1.split("\\|")[1]),
						String.valueOf(v1.split("\\|")[2]), String.valueOf(v1.split("\\|")[3]),
						String.valueOf(v1.split("\\|")[4]), String.valueOf(v1.split("\\|")[5]),
						String.valueOf(v1.split("\\|")[6]), String.valueOf(v1.split("\\|")[7]),
						String.valueOf(v1.split("\\|")[8]), String.valueOf(v1.split("\\|")[9]),
						String.valueOf(v1.split("\\|")[10]), String.valueOf(v1.split("\\|")[11]),
						String.valueOf(v1.split("\\|")[12]), String.valueOf(v1.split("\\|")[13]),
						String.valueOf(v1.split("\\|")[14]), String.valueOf(v1.split("\\|")[15]),
						String.valueOf(v1.split("\\|")[16]), String.valueOf(v1.split("\\|")[17]),
						String.valueOf(v1.split("\\|")[18]), String.valueOf(v1.split("\\|")[19]),
						String.valueOf(v1.split("\\|")[20]), String.valueOf(v1.split("\\|")[21]),
						String.valueOf(v1.split("\\|")[22]), String.valueOf(v1.split("\\|")[23]),
						String.valueOf(v1.split("\\|")[24]), String.valueOf(v1.split("\\|")[25]));

			}
		});

		List<StructField> asList = Arrays.asList(DataTypes.createStructField("log_date", DataTypes.StringType, true),
				DataTypes.createStructField("log_hour", DataTypes.StringType, true),
				DataTypes.createStructField("device_fingerprint", DataTypes.StringType, true),
				DataTypes.createStructField("device_cpu", DataTypes.StringType, true),
				DataTypes.createStructField("device_brand", DataTypes.StringType, true),
				DataTypes.createStructField("device_model", DataTypes.StringType, true),
				DataTypes.createStructField("device_name", DataTypes.StringType, true),
				DataTypes.createStructField("device_memory", DataTypes.StringType, true),
				DataTypes.createStructField("device_mac", DataTypes.StringType, true),
				DataTypes.createStructField("device_system_version", DataTypes.StringType, true),
				DataTypes.createStructField("device_network", DataTypes.StringType, true),
				DataTypes.createStructField("device_carrier", DataTypes.StringType, true),
				DataTypes.createStructField("user_id", DataTypes.StringType, true),
				DataTypes.createStructField("user_name", DataTypes.StringType, true),
				DataTypes.createStructField("user_age", DataTypes.StringType, true),
				DataTypes.createStructField("user_sex", DataTypes.StringType, true),
				DataTypes.createStructField("user_interest", DataTypes.StringType, true),
				DataTypes.createStructField("province", DataTypes.StringType, true),
				DataTypes.createStructField("city", DataTypes.StringType, true),
				DataTypes.createStructField("behavior", DataTypes.StringType, true),
				DataTypes.createStructField("media_name", DataTypes.StringType, true),
				DataTypes.createStructField("video_name", DataTypes.StringType, true),
				DataTypes.createStructField("episode", DataTypes.StringType, true),
				DataTypes.createStructField("ad_id", DataTypes.StringType, true),
				DataTypes.createStructField("ad_name", DataTypes.StringType, true),
				DataTypes.createStructField("log_time", DataTypes.StringType, true));
		StructType schema = DataTypes.createStructType(asList);
		Dataset<Row> createDataFrame = sparkSession.createDataFrame(map, schema);
		String url = "jdbc:mysql://localhost:3306/adsys?useUnicode=true&characterEncoding=utf-8";
		String table = "device_detail_info";
		Properties connectionProperties = new Properties();
		connectionProperties.put("user", "root");
		connectionProperties.put("password", "123123");
		connectionProperties.put("driver", "com.mysql.jdbc.Driver");
		// 每天的数据都append，因为每天的数据都对应一个hive表
		createDataFrame.write().mode(SaveMode.Append).jdbc(url, table, connectionProperties);
	}

}
