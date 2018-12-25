package com.yi.ai.sparkproject.test.sparkcoretest;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.SparkSession;

import com.yi.ai.sparkproject.constant.Constants;
import com.yi.ai.sparkproject.dao.OneHourAdExposeDao;
import com.yi.ai.sparkproject.dao.OneHourAdServingDao;
import com.yi.ai.sparkproject.dao.factory.DaoFactory;
import com.yi.ai.sparkproject.domain.OneHourAdExposurePojo;
import com.yi.ai.sparkproject.domain.OneHourAdServingPojo;

import scala.Tuple2;

/**
 * @author May
 * 
 *         本地版本测试 广告曝光统计 广告投放统计
 * 
 *         加上了工厂模式 以及分层
 *
 */
public class AdSparkLocal {
	public static void main(String[] args) {

		SparkSession sparkSession = SparkSession.builder().appName("AdSysStaAnalysis").master(Constants.SPARK_LOCAL)
				.config("spark.sql.warehouse.dir", "D:\\Spark\\spark-warehouse").getOrCreate();

		// 获取基础的数据 本地测试和Spark on hive 两个版本
		JavaRDD<String> basicLogInfoLocal = AdSparkLocal.getBasicLogInfoLocal(sparkSession);

		// 广告曝光统计
		AdSparkLocal.getOneHourExpose(basicLogInfoLocal);

		// 广告投放统计
		AdSparkLocal.getOneHourServing(basicLogInfoLocal);
	}

	/**
	 * @param sparkSession
	 * @return
	 * 
	 * 
	 * 		本地方式获取基础的rdd
	 */
	private static JavaRDD<String> getBasicLogInfoLocal(SparkSession sparkSession) {

		@SuppressWarnings("resource")
		JavaSparkContext jsc = new JavaSparkContext(sparkSession.sparkContext());

		JavaRDD<String> mokeFile = jsc.textFile("./mokedata.txt");
		return mokeFile;
	}

	/**
	 * @param basicLogInfoLocal
	 * 
	 *            广告曝光统计
	 */
	private static void getOneHourExpose(JavaRDD<String> basicLogInfoLocal) {
		// TODO Auto-generated method stub

		JavaPairRDD<String, Long> logInfoMapToPair = basicLogInfoLocal
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

		logInfoReduceByKey.foreach(new VoidFunction<Tuple2<String, Long>>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public void call(Tuple2<String, Long> v1) throws Exception {

				long count = v1._2;
				long click_count = (long) Math.floor(count / 100000000);
				long expose_count = (long) (count % 100000000);
				OneHourAdExposurePojo oneHourAdExposurePojo = new OneHourAdExposurePojo(
						String.valueOf(v1._1.split("\\|")[0]), String.valueOf(v1._1.split("\\|")[1]),
						String.valueOf(v1._1.split("\\|")[2]), String.valueOf(v1._1.split("\\|")[3]),
						String.valueOf(v1._1.split("\\|")[4]), String.valueOf(v1._1.split("\\|")[5]),
						String.valueOf(v1._1.split("\\|")[6]), click_count, expose_count);
				OneHourAdExposeDao oneHourAdExposeDao = DaoFactory.getOneHourAdExposeDao();
				oneHourAdExposeDao.insertOneHourExposeDa(oneHourAdExposurePojo);

			}
		});

	}

	/**
	 * @param basicLogInfoLocal
	 * 
	 *            广告投放统计
	 */
	private static void getOneHourServing(JavaRDD<String> basicLogInfoLocal) {
		JavaPairRDD<String, Long> oneHourAdservingPair = basicLogInfoLocal
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
		oneHourAdservingreduceByKey.foreach(new VoidFunction<Tuple2<String, Long>>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public void call(Tuple2<String, Long> v1) throws Exception {
				long count = v1._2;
				long click_count = (long) Math.floor(count / 100000000);
				long expose_count = (long) (count % 100000000);
				OneHourAdServingPojo oneHourAdServingPojo = new OneHourAdServingPojo(
						String.valueOf(v1._1.split("\\|")[0]), String.valueOf(v1._1.split("\\|")[1]),
						String.valueOf(v1._1.split("\\|")[2]), String.valueOf(v1._1.split("\\|")[3]),
						String.valueOf(v1._1.split("\\|")[4]), click_count, expose_count);
				OneHourAdServingDao oneHourAdServing = DaoFactory.getOneHourAdServing();
				oneHourAdServing.insertOneHourAdServing(oneHourAdServingPojo);

			}
		});

	}

}
