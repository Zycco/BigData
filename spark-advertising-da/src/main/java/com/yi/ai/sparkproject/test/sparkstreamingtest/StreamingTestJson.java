package com.yi.ai.sparkproject.test.sparkstreamingtest;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
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
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

import kafka.serializer.StringDecoder;
import scala.Tuple2;

public class StreamingTestJson {

	public static void main(String[] args) throws InterruptedException {

		// 创建sparksession对象
		SparkSession sparkSession = SparkSession.builder().appName("testjson").getOrCreate();

		// 通过sparksession对象获取JavaSparkContext
		JavaSparkContext jsc = new JavaSparkContext(sparkSession.sparkContext());

		// 通过JavaSparkContext获取JavaStreamingContext
		JavaStreamingContext jssc = new JavaStreamingContext(jsc, Durations.seconds(60));

		// 首先，要创建一份kafka参数map
		Map<String, String> kafkaParams = new HashMap<String, String>();
		kafkaParams.put("metadata.broker.list", "172.17.133.115:9092,172.17.133.116:9092,172.17.133.118:9092");

		// 然后，要创建一个set，里面放入，你要读取的topic
		Set<String> topics = new HashSet<String>();
		topics.add("adSysLog");

		// 创建最基础的输入DStream
		JavaPairInputDStream<String, String> adRealTimeLogDStream = KafkaUtils.createDirectStream(jssc, String.class,
				String.class, StringDecoder.class, StringDecoder.class, kafkaParams, topics);

		JavaDStream<String> map = adRealTimeLogDStream.map(new Function<Tuple2<String, String>, String>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public String call(Tuple2<String, String> v1) throws Exception {
				String loginfo = v1._2;
				JSONObject jsonObject = JSON.parseObject(loginfo);
				String name = jsonObject.getString("name");
				String age = jsonObject.getString("age");
				String sex = jsonObject.getString("sex");

				return name + "," + age + "," + sex;
			}
		});

		map.foreachRDD(new VoidFunction<JavaRDD<String>>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public void call(JavaRDD<String> t) throws Exception {

				JavaRDD<Row> infoRow = t.map(new Function<String, Row>() {

					/**
					 * 
					 */
					private static final long serialVersionUID = 1L;

					@Override
					public Row call(String v1) throws Exception {
						// TODO Auto-generated method stub
						return RowFactory.create(String.valueOf(v1.split(",")[0]), String.valueOf(v1.split(",")[1]),
								String.valueOf(v1.split(",")[2]));

					}
				});

				List<StructField> asList = Arrays.asList(
						DataTypes.createStructField("name", DataTypes.StringType, true),
						DataTypes.createStructField("age", DataTypes.StringType, true),
						DataTypes.createStructField("sex", DataTypes.StringType, true));

				StructType schema = DataTypes.createStructType(asList);

				Dataset<Row> createDataFrame = sparkSession.createDataFrame(infoRow, schema);

				String url = "jdbc:mysql://172.17.133.119:3306/testdatabase?useUnicode=true&characterEncoding=utf-8";
				String table = "testjson";
				Properties connectionProperties = new Properties();
				connectionProperties.put("user", "root");
				connectionProperties.put("password", "123");
				connectionProperties.put("driver", "com.mysql.jdbc.Driver");
				// 每天的数据都append，因为每天的数据都对应一个hive表
				createDataFrame.write().mode(SaveMode.Append).jdbc(url, table, connectionProperties);

			}
		});

		jssc.start();
		jssc.awaitTermination();
		jssc.close();

	}
}
