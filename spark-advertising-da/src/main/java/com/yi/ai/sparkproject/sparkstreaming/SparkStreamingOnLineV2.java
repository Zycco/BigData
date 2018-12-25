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

import kafka.serializer.StringDecoder;
import scala.Tuple2;

public class SparkStreamingOnLineV2 {

	/**
	 * @param args
	 * @throws InterruptedException
	 * 
	 *             曝光统计和投放统计SparkStreaming的第二个版本
	 * 
	 *             和第一个版本比较
	 * 
	 *             第一个版本是用jdbc的方式写入mysql
	 * 
	 *             第二个版本是用sparksql的方式写入到mysql
	 * 
	 *             V2是上线版本
	 */
	public static void main(String[] args) throws InterruptedException {

		// 创建sparksession对象
		SparkSession sparkSession = SparkSession.builder().appName("StreamingRealTimeLogOnLine").getOrCreate();

		// 通过sparksession对象获取JavaSparkContext
		JavaSparkContext jsc = new JavaSparkContext(sparkSession.sparkContext());

		// 通过JavaSparkContext获取JavaStreamingContext
		JavaStreamingContext jssc = new JavaStreamingContext(jsc, Durations.seconds(60));

		jssc.checkpoint("hdfs://172.17.133.111:8020//adsysstreaming_checkpoint");

		// 首先，要创建一份kafka参数map
		Map<String, String> kafkaParams = new HashMap<String, String>();
		kafkaParams.put("metadata.broker.list", "172.17.133.111:9092,172.17.133.112:9092,172.17.133.113:9092");

		// 然后，要创建一个set，里面放入，你要读取的topic
		Set<String> topics = new HashSet<String>();
		topics.add("advSysLog0625");

		// 创建最基础的输入DStream
		JavaPairInputDStream<String, String> adRealTimeLogDStream = KafkaUtils.createDirectStream(jssc, String.class,
				String.class, StringDecoder.class, StringDecoder.class, kafkaParams, topics);

		JavaDStream<String> basicLogInfo = SparkStreamingOnLineV2.getBasicLogInfo(adRealTimeLogDStream);
		// 广告曝光统计小时为粒度
		SparkStreamingOnLineV2.getOneHourExpose(sparkSession, basicLogInfo);
		// 广告投放统计
		SparkStreamingOnLineV2.getOneHourServing(sparkSession, basicLogInfo);

		jssc.start();
		jssc.awaitTermination();
		jssc.close();

	}

	@SuppressWarnings("unused")
	private static JavaDStream<String> getBasicLogInfo(JavaPairInputDStream<String, String> adRealTimeLogDStream) {

		JavaDStream<String> map = adRealTimeLogDStream.map(new Function<Tuple2<String, String>, String>() {

			private static final long serialVersionUID = 1L;

			@Override
			public String call(Tuple2<String, String> v1) throws Exception {
				String adlogInfo = v1._2;
				System.out.println("接收到的log信息为" + "--------------" + adlogInfo);
				return adlogInfo;
			}
		});

		return map;

	}

	private static void getOneHourExpose(SparkSession sparkSession, JavaDStream<String> map) {

		JavaPairDStream<String, Long> mapToPair = map.mapToPair(new PairFunction<String, String, Long>() {

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

				String logInfo = log_date + "|" + log_hour + "|" + media_id + "|" + video_resource_id + "|" + video_name
						+ "|" + scene_id + "|" + style_id;

				System.out.println(logInfo);
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

				return v1 + v2;
			}
		});

		JavaDStream<Row> transform = reduceByKey.transform(new Function<JavaPairRDD<String, Long>, JavaRDD<Row>>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public JavaRDD<Row> call(JavaPairRDD<String, Long> v1) throws Exception {

				JavaRDD<Row> exposeRow = v1.map(new Function<Tuple2<String, Long>, Row>() {

					/**
					 * 
					 */
					private static final long serialVersionUID = 1L;

					@Override
					public Row call(Tuple2<String, Long> v1) throws Exception {
						long count = v1._2;
						long click_count = (long) Math.floor(count / 100000000);
						long expose_count = (long) (count % 100000000);
						return RowFactory.create(String.valueOf(v1._1.split("\\|")[0]),
								String.valueOf(v1._1.split("\\|")[1]), String.valueOf(v1._1.split("\\|")[2]),
								String.valueOf(v1._1.split("\\|")[3]), String.valueOf(v1._1.split("\\|")[4]),
								String.valueOf(v1._1.split("\\|")[5]), String.valueOf(v1._1.split("\\|")[6]),
								click_count, expose_count);
					}
				});

				return exposeRow;

			}

		});

		transform.foreachRDD(new VoidFunction<JavaRDD<Row>>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public void call(JavaRDD<Row> t) throws Exception {
				List<StructField> asList = Arrays.asList(
						DataTypes.createStructField("log_date", DataTypes.StringType, true),
						DataTypes.createStructField("log_hour", DataTypes.StringType, true),
						DataTypes.createStructField("media_id", DataTypes.StringType, true),
						DataTypes.createStructField("video_resource_id", DataTypes.StringType, true),
						DataTypes.createStructField("video_name", DataTypes.StringType, true),
						DataTypes.createStructField("scene_id", DataTypes.StringType, true),
						DataTypes.createStructField("style_id", DataTypes.StringType, true),
						DataTypes.createStructField("click_count", DataTypes.LongType, true),
						DataTypes.createStructField("expose_count", DataTypes.LongType, true));

				StructType schema = DataTypes.createStructType(asList);

				Dataset<Row> createDataFrame = sparkSession.createDataFrame(t, schema);

				String url = "jdbc:mysql://172.17.133.113:3306/adsys?useUnicode=true&characterEncoding=utf-8";
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

	private static void getOneHourServing(SparkSession sparkSession, JavaDStream<String> map) {

		JavaPairDStream<String, Long> mapToPair = map.mapToPair(new PairFunction<String, String, Long>() {

			/**
			 * 
			 */
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
				System.out.println(Info);

				return new Tuple2<String, Long>(Info, sum);

			}
		});

		JavaPairDStream<String, Long> reduceByKey = mapToPair.reduceByKey(new Function2<Long, Long, Long>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Long call(Long v1, Long v2) throws Exception {

				return v1 + v2;
			}
		});

		JavaDStream<Row> transform = reduceByKey.transform(new Function<JavaPairRDD<String, Long>, JavaRDD<Row>>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public JavaRDD<Row> call(JavaPairRDD<String, Long> v1) throws Exception {

				JavaRDD<Row> adSerVRow = v1.map(new Function<Tuple2<String, Long>, Row>() {

					/**
					 * 
					 */
					private static final long serialVersionUID = 1L;

					@Override
					public Row call(Tuple2<String, Long> v1) throws Exception {
						long count = v1._2;
						long click_count = (long) Math.floor(count / 100000000);
						long expose_count = (long) (count % 100000000);
						return RowFactory.create(String.valueOf(v1._1.split("\\|")[0]),
								String.valueOf(v1._1.split("\\|")[1]), String.valueOf(v1._1.split("\\|")[2]),
								String.valueOf(v1._1.split("\\|")[3]), String.valueOf(v1._1.split("\\|")[4]),
								click_count, expose_count);

					}

				});

				return adSerVRow;

			}

		});

		transform.foreachRDD(new VoidFunction<JavaRDD<Row>>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public void call(JavaRDD<Row> t) throws Exception {

				List<StructField> asList = Arrays.asList(
						DataTypes.createStructField("log_date", DataTypes.StringType, true),
						DataTypes.createStructField("log_hour", DataTypes.StringType, true),
						DataTypes.createStructField("ad_id", DataTypes.StringType, true),
						DataTypes.createStructField("ad_name", DataTypes.StringType, true),
						DataTypes.createStructField("advertiser_id", DataTypes.StringType, true),
						DataTypes.createStructField("click_count", DataTypes.LongType, true),
						DataTypes.createStructField("expose_count", DataTypes.LongType, true));
				StructType schema = DataTypes.createStructType(asList);
				Dataset<Row> createDataFrame = sparkSession.createDataFrame(t, schema);
				String url = "jdbc:mysql://172.17.133.113:3306/adsys?useUnicode=true&characterEncoding=utf-8";
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

}
