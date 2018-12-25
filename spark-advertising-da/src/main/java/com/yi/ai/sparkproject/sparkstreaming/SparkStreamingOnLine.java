package com.yi.ai.sparkproject.sparkstreaming;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import kafka.serializer.StringDecoder;
import scala.Tuple2;

public class SparkStreamingOnLine {

	public static void main(String[] args) throws InterruptedException {

		SparkConf conf = new SparkConf().setAppName("StreamingRealTimeLogOnLine");
		JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(60));

		// 首先，要创建一份kafka参数map
		Map<String, String> kafkaParams = new HashMap<String, String>();
		kafkaParams.put("metadata.broker.list", "172.17.133.111:9092,172.17.133.112:9092,172.17.133.113:9092");

		// 然后，要创建一个set，里面放入，你要读取的topic
		Set<String> topics = new HashSet<String>();
		topics.add("advSysLog0625");

		// 创建最基础的输入DStream
		JavaPairInputDStream<String, String> adRealTimeLogDStream = KafkaUtils.createDirectStream(jssc, String.class,
				String.class, StringDecoder.class, StringDecoder.class, kafkaParams, topics);

		JavaDStream<String> map = adRealTimeLogDStream.map(new Function<Tuple2<String, String>, String>() {

			private static final long serialVersionUID = 1L;

			@Override
			public String call(Tuple2<String, String> v1) throws Exception {
				String adlogInfo = v1._2;
				System.out.println("接收到的log信息为" + "--------------" + adlogInfo);
				return adlogInfo;
			}
		});

		// 广告曝光统计粒度：小时
		SparkStreamingOnLine.getOneHourExpose(map);
		SparkStreamingOnLine.getOneHourServing(map);

		jssc.start();
		jssc.awaitTermination();
		jssc.close();

	}

	private static void getOneHourExpose(JavaDStream<String> map) {

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

		reduceByKey.foreachRDD(new VoidFunction<JavaPairRDD<String, Long>>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public void call(JavaPairRDD<String, Long> t) throws Exception {

				t.foreachPartition(new VoidFunction<Iterator<Tuple2<String, Long>>>() {

					/**
					 * 
					 */
					private static final long serialVersionUID = 1L;

					@Override
					public void call(Iterator<Tuple2<String, Long>> t) throws Exception {
						/*// 给每个partition获取连接
						Connection conn = ConnectionPool.getConnection();
						while (t.hasNext()) {
							Tuple2<String, Long> next = t.next();
							String saveInfo = next._1;
							String[] splitInfo = saveInfo.split("\\|");
							Long count = next._2;
							long click_count = (long) Math.floor(count / 100000000);
							long expose_count = (long) (count % 100000000);
							String sql = "insert into onehouradexposure values ('" + splitInfo[0] + "','" + splitInfo[1]
									+ "','" + splitInfo[2] + "','" + splitInfo[3] + "','" + splitInfo[4] + "','"
									+ splitInfo[5] + "','" + splitInfo[6] + "','" + click_count + "','" + expose_count
									+ "')";
							Statement stmt = conn.createStatement();
							stmt.executeUpdate(sql);
						}
						ConnectionPool.returnConnection(conn);*/

					}

				});

			}
		});

	}

	private static void getOneHourServing(JavaDStream<String> map) {

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

		reduceByKey.foreachRDD(new VoidFunction<JavaPairRDD<String, Long>>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public void call(JavaPairRDD<String, Long> t) throws Exception {

				t.foreachPartition(new VoidFunction<Iterator<Tuple2<String, Long>>>() {

					private static final long serialVersionUID = 1L;

					@Override
					public void call(Iterator<Tuple2<String, Long>> t) throws Exception {
						// TODO Auto-generated method stub

						/*// 给每个partition获取连接
						Connection conn = ConnectionPool.getConnection();
						
						while (t.hasNext()) {
							Tuple2<String, Long> next = t.next();
							Long count = next._2;
							long click_count = (long) Math.floor(count / 100000000);
							long expose_count = (long) (count % 100000000);
							String[] splitInfo = next._1.split("\\|");
							String sql = "insert into onehouradserving values ('" + splitInfo[0] + "','" + splitInfo[1]
									+ "','" + splitInfo[2] + "','" + splitInfo[3] + "','" + splitInfo[4] + "','"
									+ click_count + "','" + expose_count + "')";
							Statement stmt = conn.createStatement();
							stmt.executeUpdate(sql);
						}
						ConnectionPool.returnConnection(conn);*/
					}

				});

			}

		});

	}

}
