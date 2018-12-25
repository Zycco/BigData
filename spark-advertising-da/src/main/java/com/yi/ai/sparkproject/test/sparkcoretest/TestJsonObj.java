package com.yi.ai.sparkproject.test.sparkcoretest;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.SparkSession;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

public class TestJsonObj {

	public static void main(String[] args) {

		SparkSession sparkSession = SparkSession.builder().appName("testjson").master("local")
				.config("spark.sql.warehouse.dir", "D:\\Spark\\spark-warehouse").getOrCreate();

		// 获取基础的数据 本地测试和Spark on hive 两个版本

		JavaSparkContext jsc = new JavaSparkContext(sparkSession.sparkContext());

		JavaRDD<String> mokeFile = jsc.textFile("./mokejson.txt");

		JavaRDD<String> map = mokeFile.map(new Function<String, String>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public String call(String v1) throws Exception {
				JSONObject jsonObject = JSON.parseObject(v1);
				String name = jsonObject.getString("action");
				String age = jsonObject.getString("api_version");
				String sex = jsonObject.getString("sdk_version");
				String info = name + "--" + age + "--" + sex;
				return info;
			}

		});

		map.foreach(new VoidFunction<String>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public void call(String t) throws Exception {
				System.out.println(t);
			}
		});

	}
}
