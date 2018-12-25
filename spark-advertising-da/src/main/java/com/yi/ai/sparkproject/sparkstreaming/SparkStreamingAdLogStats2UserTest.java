package com.yi.ai.sparkproject.sparkstreaming;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

import kafka.serializer.StringDecoder;
import scala.Tuple2;

/**
 * @author May
 * 
 *         广告项目二期数据处理 和版本一1比较2加上了设备去重 和10分钟一次计算
 *
 */
public class SparkStreamingAdLogStats2UserTest {

	public static void main(String[] args) throws InterruptedException {

		// 创建sparksession对象
		SparkSession sparkSession = SparkSession.builder().appName("StreamingRealTimeLogOnLineUserTest").getOrCreate();

		// 通过sparksession对象获取JavaSparkContext
		JavaSparkContext jsc = new JavaSparkContext(sparkSession.sparkContext());

		// 通过JavaSparkContext获取JavaStreamingContext
		JavaStreamingContext jssc = new JavaStreamingContext(jsc, Durations.seconds(600));

		// jssc.checkpoint("hdfs://172.17.133.111:8020//adsysstreaming_checkpoint");

		// 首先，要创建一份kafka参数map
		Map<String, String> kafkaParams = new HashMap<String, String>();
		kafkaParams.put("metadata.broker.list", "172.17.133.117:9092,172.17.133.115:9092,172.17.133.116:9092");

		// 然后，要创建一个set，里面放入，你要读取的topic
		Set<String> topics = new HashSet<String>();
		topics.add("adSysLogUserTest");

		// 创建最基础的输入DStream
		JavaPairInputDStream<String, String> adRealTimeLogDStream = KafkaUtils.createDirectStream(jssc, String.class,
				String.class, StringDecoder.class, StringDecoder.class, kafkaParams, topics);

		// 获取最基础的DStream
		JavaDStream<String> basicLogInfo = SparkStreamingAdLogStats2UserTest.getBasicLogInfo(adRealTimeLogDStream);

		// 获取基础的数据过滤掉关键字段不为空的数据
		JavaDStream<String> filterLogInfo = SparkStreamingAdLogStats2UserTest.getBasicLog(basicLogInfo);

		// 媒资管理平台获取广告曝光统计
		SparkStreamingAdLogStats2UserTest.getOneHourExpose(sparkSession, filterLogInfo);
		// 获取广告投放统计
		SparkStreamingAdLogStats2UserTest.getOneHourServing(sparkSession, filterLogInfo);
		// Yi+运营平台 和 广告投放平台 广告曝光统计
		SparkStreamingAdLogStats2UserTest.getMasterOneHourExpose(sparkSession, filterLogInfo);
		// 运营平台设备统计
		SparkStreamingAdLogStats2UserTest.devicesStats(sparkSession, filterLogInfo);
		// 运营平台媒体主统计
		SparkStreamingAdLogStats2UserTest.mediaMasterStats(sparkSession, filterLogInfo);
		// 设备行为信息统计 输入的是最开始的数据 不是过滤后的
		SparkStreamingAdLogStats2UserTest.devInfo(sparkSession, basicLogInfo);

		jssc.start();
		jssc.awaitTermination();
		jssc.close();

	}

	private static JavaDStream<String> getBasicLogInfo(JavaPairInputDStream<String, String> adRealTimeLogDStream) {
		// TODO Auto-generated method stub

		JavaDStream<String> basicLogInfo = adRealTimeLogDStream.map(new Function<Tuple2<String, String>, String>() {

			private static final long serialVersionUID = 1L;

			@Override
			public String call(Tuple2<String, String> v1) throws Exception {
				String adlogInfo = v1._2;
				System.out.println("接收到的log信息为" + "--------------" + adlogInfo);
				return adlogInfo;
			}
		});

		JavaDStream<String> basicLogInfoFilter = basicLogInfo.filter(new Function<String, Boolean>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Boolean call(String v1) throws Exception {
				// TODO Auto-generated method stub
				try {
					JSONObject jsonStr = JSONObject.parseObject(v1);
					if (v1 != null && !"".equals(v1.trim()) && jsonStr != null) {
						return true;
					} else {
						return false;
					}
				} catch (Exception e) {
					return false;
				}
			}

		});

		return basicLogInfoFilter;
	}

	private static JavaDStream<String> getBasicLog(JavaDStream<String> basicLogInfo) {
		// TODO Auto-generated method stub

		JavaDStream<String> filterLoginfo = basicLogInfo.filter(new Function<String, Boolean>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

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
		return filterLoginfo;
	}

	private static void getOneHourExpose(SparkSession sparkSession, JavaDStream<String> filterLogInfoCache) {
		// TODO Auto-generated method stub
		JavaPairDStream<String, Long> mapToPair = filterLogInfoCache
				.mapToPair(new PairFunction<String, String, Long>() {

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

						String logInfo = log_date + "|" + log_hour + "|" + media_id + "|" + media_name + "|"
								+ video_resource_id + "|" + video_name + "|" + scene_id + "|" + style_id;
						System.out.println("广告曝光统计接收到的log数据为-------→" + logInfo);
						return new Tuple2<String, Long>(logInfo, sum);
					}

				});

		JavaPairDStream<String, Long> reduceByKey = mapToPair.reduceByKey(new Function2<Long, Long, Long>() {

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

		reduceByKey.foreachRDD(new VoidFunction<JavaPairRDD<String, Long>>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public void call(JavaPairRDD<String, Long> t) throws Exception {
				// TODO Auto-generated method stub

				JavaRDD<Row> rowRdd = t.map(new Function<Tuple2<String, Long>, Row>() {

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
						return RowFactory.create(String.valueOf(v1._1.split("\\|")[0]),
								String.valueOf(v1._1.split("\\|")[1]), String.valueOf(v1._1.split("\\|")[2]),
								String.valueOf(v1._1.split("\\|")[3]), String.valueOf(v1._1.split("\\|")[4]),
								String.valueOf(v1._1.split("\\|")[5]), String.valueOf(v1._1.split("\\|")[6]),
								String.valueOf(v1._1.split("\\|")[7]), click_count, expose_count);
					}

				});

				List<StructField> asList = Arrays.asList(
						DataTypes.createStructField("log_date", DataTypes.StringType, true),
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

				Dataset<Row> createDataFrame = sparkSession.createDataFrame(rowRdd, schema);

				String url = "jdbc:mysql://172.17.133.119:3306/adSys_user?useUnicode=true&characterEncoding=utf-8";
				String table = "onehouradexposure";
				Properties connectionProperties = new Properties();
				connectionProperties.put("user", "root");
				connectionProperties.put("password", "123");
				connectionProperties.put("driver", "com.mysql.jdbc.Driver");
				// 每天的数据都append，因为每天的数据都对应一个hive表
				createDataFrame.write().mode(SaveMode.Append).jdbc(url, table, connectionProperties);

			}
		});

	}

	private static void getOneHourServing(SparkSession sparkSession, JavaDStream<String> filterLogInfoCache) {
		// TODO Auto-generated method stub
		JavaPairDStream<String, Long> mapToPair = filterLogInfoCache
				.mapToPair(new PairFunction<String, String, Long>() {

					/**
					 * 
					 */
					private static final long serialVersionUID = 1L;

					@Override
					public Tuple2<String, Long> call(String t) throws Exception {
						// TODO Auto-generated method stub
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

						String logInfo = log_date + "|" + log_hour + "|" + ad_id + "|" + ad_name + "|" + advertiser_id
								+ "|" + advertiser_name;
						System.out.println("广告投放统计接收到的log数据为-------→" + logInfo);
						return new Tuple2<String, Long>(logInfo, sum);
					}

				});

		JavaPairDStream<String, Long> reduceByKey = mapToPair.reduceByKey(new Function2<Long, Long, Long>() {

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

		reduceByKey.foreachRDD(new VoidFunction<JavaPairRDD<String, Long>>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public void call(JavaPairRDD<String, Long> t) throws Exception {
				// TODO Auto-generated method stub

				JavaRDD<Row> map = t.map(new Function<Tuple2<String, Long>, Row>() {

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
						return RowFactory.create(String.valueOf(v1._1.split("\\|")[0]),
								String.valueOf(v1._1.split("\\|")[1]), String.valueOf(v1._1.split("\\|")[2]),
								String.valueOf(v1._1.split("\\|")[3]), String.valueOf(v1._1.split("\\|")[4]),
								String.valueOf(v1._1.split("\\|")[5]), click_count, expose_count);
					}
				});
				List<StructField> asList = Arrays.asList(
						DataTypes.createStructField("log_date", DataTypes.StringType, true),
						DataTypes.createStructField("log_hour", DataTypes.StringType, true),
						DataTypes.createStructField("ad_id", DataTypes.StringType, true),
						DataTypes.createStructField("ad_name", DataTypes.StringType, true),
						DataTypes.createStructField("advertiser_id", DataTypes.StringType, true),
						DataTypes.createStructField("advertiser_name", DataTypes.StringType, true),
						DataTypes.createStructField("click_count", DataTypes.LongType, true),
						DataTypes.createStructField("expose_count", DataTypes.LongType, true));

				StructType schema = DataTypes.createStructType(asList);

				Dataset<Row> createDataFrame = sparkSession.createDataFrame(map, schema);

				String url = "jdbc:mysql://172.17.133.119:3306/adSys_user?useUnicode=true&characterEncoding=utf-8";
				String table = "onehouradserving";
				Properties connectionProperties = new Properties();
				connectionProperties.put("user", "root");
				connectionProperties.put("password", "123");
				connectionProperties.put("driver", "com.mysql.jdbc.Driver");
				// 每天的数据都append，因为每天的数据都对应一个hive表
				createDataFrame.write().mode(SaveMode.Append).jdbc(url, table, connectionProperties);
			}

		});

	}

	private static void getMasterOneHourExpose(SparkSession sparkSession, JavaDStream<String> filterLogInfoCache) {
		// TODO Auto-generated method stub
		JavaPairDStream<String, Long> mapToPair = filterLogInfoCache
				.mapToPair(new PairFunction<String, String, Long>() {

					/**
					 * 
					 */
					private static final long serialVersionUID = 1L;

					@Override
					public Tuple2<String, Long> call(String t) throws Exception {
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

						String logInfo = log_date + "|" + log_hour + "|" + user_id + "|" + media_id + "|" + media_name
								+ "|" + advertiser_id + "|" + advertiser_name + "|" + ad_id + "|" + ad_name + "|"
								+ scene_id + "|" + style_id + "|" + platform + "|" + user_current_province + "|"
								+ user_current_city + "|" + ui_sex + "|" + ctype;
						System.out.println("运营平台和广告平台曝光统计接收到的log数据为-------→" + logInfo);
						return new Tuple2<String, Long>(logInfo, sum);
					}
				});

		JavaPairDStream<String, Long> reduceByKey = mapToPair.reduceByKey(new Function2<Long, Long, Long>() {

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

		reduceByKey.foreachRDD(new VoidFunction<JavaPairRDD<String, Long>>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public void call(JavaPairRDD<String, Long> t) throws Exception {
				// TODO Auto-generated method stub

				JavaRDD<Row> map = t.map(new Function<Tuple2<String, Long>, Row>() {

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
						return RowFactory.create(String.valueOf(v1._1.split("\\|")[0]),
								String.valueOf(v1._1.split("\\|")[1]), String.valueOf(v1._1.split("\\|")[2]),
								String.valueOf(v1._1.split("\\|")[3]), String.valueOf(v1._1.split("\\|")[4]),
								String.valueOf(v1._1.split("\\|")[5]), String.valueOf(v1._1.split("\\|")[6]),
								String.valueOf(v1._1.split("\\|")[7]), String.valueOf(v1._1.split("\\|")[8]),
								String.valueOf(v1._1.split("\\|")[9]), String.valueOf(v1._1.split("\\|")[10]),
								String.valueOf(v1._1.split("\\|")[11]), String.valueOf(v1._1.split("\\|")[12]),
								String.valueOf(v1._1.split("\\|")[13]), String.valueOf(v1._1.split("\\|")[14]),
								String.valueOf(v1._1.split("\\|")[15]), click_count, expose_count);
					}

				});

				List<StructField> asList = Arrays.asList(
						DataTypes.createStructField("log_date", DataTypes.StringType, true),
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

				String url = "jdbc:mysql://172.17.133.119:3306/adSys_user?useUnicode=true&characterEncoding=utf-8";
				String table = "ad_expose_matser";
				Properties connectionProperties = new Properties();
				connectionProperties.put("user", "root");
				connectionProperties.put("password", "123");
				connectionProperties.put("driver", "com.mysql.jdbc.Driver");
				// 每天的数据都append，因为每天的数据都对应一个hive表
				createDataFrame.write().mode(SaveMode.Append).jdbc(url, table, connectionProperties);

			}
		});

	}

	private static void devicesStats(SparkSession sparkSession, JavaDStream<String> filterLogInfoCache) {
		// TODO Auto-generated method stub
		JavaPairDStream<String, Long> mapToPair = filterLogInfoCache
				.mapToPair(new PairFunction<String, String, Long>() {

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
						String logInfo = log_date + "|" + log_hour + "|" + media_id + "|" + media_name + "|" + platform
								+ "|" + user_current_province + "|" + user_current_city + "|" + ui_sex;
						System.out.println("运营平台和广告平台设备统计接收到的log数据为-------→" + logInfo);
						return new Tuple2<String, Long>(logInfo, (long) 1);
					}

				});

		JavaPairDStream<String, Long> reduceByKey = mapToPair.reduceByKey(new Function2<Long, Long, Long>() {

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

		reduceByKey.foreachRDD(new VoidFunction<JavaPairRDD<String, Long>>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public void call(JavaPairRDD<String, Long> t) throws Exception {
				// TODO Auto-generated method stub

				JavaRDD<Row> map = t.map(new Function<Tuple2<String, Long>, Row>() {

					/**
					 * 
					 */
					private static final long serialVersionUID = 1L;

					@Override
					public Row call(Tuple2<String, Long> v1) throws Exception {
						// TODO Auto-generated method stub
						long dev_count = v1._2;
						return RowFactory.create(String.valueOf(v1._1.split("\\|")[0]),
								String.valueOf(v1._1.split("\\|")[1]), String.valueOf(v1._1.split("\\|")[2]),
								String.valueOf(v1._1.split("\\|")[3]), String.valueOf(v1._1.split("\\|")[4]),
								String.valueOf(v1._1.split("\\|")[5]), String.valueOf(v1._1.split("\\|")[6]),
								String.valueOf(v1._1.split("\\|")[7]), dev_count);

					}

				});

				List<StructField> asList = Arrays.asList(
						DataTypes.createStructField("log_date", DataTypes.StringType, true),
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

				String url = "jdbc:mysql://172.17.133.119:3306/adSys_user?useUnicode=true&characterEncoding=utf-8";
				String table = "devices_stats";
				Properties connectionProperties = new Properties();
				connectionProperties.put("user", "root");
				connectionProperties.put("password", "123");
				connectionProperties.put("driver", "com.mysql.jdbc.Driver");
				// 每天的数据都append，因为每天的数据都对应一个hive表
				createDataFrame.write().mode(SaveMode.Append).jdbc(url, table, connectionProperties);
			}

		});

	}

	private static void mediaMasterStats(SparkSession sparkSession, JavaDStream<String> filterLogInfoCache) {
		// TODO Auto-generated method stub

		JavaPairDStream<String, Long> mapToPair = filterLogInfoCache
				.mapToPair(new PairFunction<String, String, Long>() {

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

		JavaPairDStream<String, Long> reduceByKey = mapToPair.reduceByKey(new Function2<Long, Long, Long>() {

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

		reduceByKey.foreachRDD(new VoidFunction<JavaPairRDD<String, Long>>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public void call(JavaPairRDD<String, Long> t) throws Exception {
				// TODO Auto-generated method stub

				JavaRDD<Row> map = t.map(new Function<Tuple2<String, Long>, Row>() {

					/**
					 * 
					 */
					private static final long serialVersionUID = 1L;

					@Override
					public Row call(Tuple2<String, Long> v1) throws Exception {
						long expose_count = v1._2;
						return RowFactory.create(String.valueOf(v1._1.split("\\|")[0]),
								String.valueOf(v1._1.split("\\|")[1]), String.valueOf(v1._1.split("\\|")[2]),
								String.valueOf(v1._1.split("\\|")[3]), expose_count);
					}

				});

				List<StructField> asList = Arrays.asList(
						DataTypes.createStructField("log_date", DataTypes.StringType, true),
						DataTypes.createStructField("log_hour", DataTypes.StringType, true),
						DataTypes.createStructField("media_industry", DataTypes.StringType, true),
						DataTypes.createStructField("media_name", DataTypes.StringType, true),
						DataTypes.createStructField("expose_count", DataTypes.LongType, true));

				StructType schema = DataTypes.createStructType(asList);
				Dataset<Row> createDataFrame = sparkSession.createDataFrame(map, schema);
				String url = "jdbc:mysql://172.17.133.119:3306/adSys_user?useUnicode=true&characterEncoding=utf-8";
				String table = "medowner_adexp_stats";
				Properties connectionProperties = new Properties();
				connectionProperties.put("user", "root");
				connectionProperties.put("password", "123");
				connectionProperties.put("driver", "com.mysql.jdbc.Driver");
				// 每天的数据都append，因为每天的数据都对应一个hive表
				createDataFrame.write().mode(SaveMode.Append).jdbc(url, table, connectionProperties);

			}

		});

	}

	private static void devInfo(SparkSession sparkSession, JavaDStream<String> basicLogInfo) {
		// TODO Auto-generated method stub

		JavaDStream<String> filter = basicLogInfo.filter(new Function<String, Boolean>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Boolean call(String v1) throws Exception {
				// TODO Auto-generated method stub
				JSONObject jsonObject = JSON.parseObject(v1);
				String log_time = jsonObject.getString("time");
				String log_date = jsonObject.getString("date");
				String log_hour = jsonObject.getString("hour");
				String device_fingerprint = jsonObject.getString("device_fingerprint");
				String action = jsonObject.getString("action");
				return (log_date != null && !"".equals(log_date.trim()) && log_hour != null
						&& !"".equals(log_hour.trim()) && device_fingerprint != null
						&& !"".equals(device_fingerprint.trim()) && action != null && !"".equals(action.trim())
						&& log_time != null && !"".equals(log_time.trim()));
			}

		});

		JavaDStream<String> map = filter.map(new Function<String, String>() {

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
				System.out.println("设备详细信息log-----------→" + logInfo);
				return logInfo;
			}

		});

		JavaDStream<String> transform = map.transform(new Function<JavaRDD<String>, JavaRDD<String>>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public JavaRDD<String> call(JavaRDD<String> v1) throws Exception {
				// TODO Auto-generated method stub

				JavaRDD<String> distinctDeviceInfo = v1.distinct();
				return distinctDeviceInfo;
			}

		});

		transform.foreachRDD(new VoidFunction<JavaRDD<String>>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public void call(JavaRDD<String> t) throws Exception {
				// TODO Auto-generated method stub
				JavaRDD<Row> devInfoRow = t.map(new Function<String, Row>() {

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

				List<StructField> asList = Arrays.asList(
						DataTypes.createStructField("log_date", DataTypes.StringType, true),
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
				Dataset<Row> createDataFrame = sparkSession.createDataFrame(devInfoRow, schema);
				String url = "jdbc:mysql://172.17.133.119:3306/adSys_user?useUnicode=true&characterEncoding=utf-8";
				String table = "device_detail_info";
				Properties connectionProperties = new Properties();
				connectionProperties.put("user", "root");
				connectionProperties.put("password", "123");
				connectionProperties.put("driver", "com.mysql.jdbc.Driver");
				// 每天的数据都append，因为每天的数据都对应一个hive表
				createDataFrame.write().mode(SaveMode.Append).jdbc(url, table, connectionProperties);
			}
		});
	}
}
