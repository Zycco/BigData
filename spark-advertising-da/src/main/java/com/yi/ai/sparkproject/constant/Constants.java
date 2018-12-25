package com.yi.ai.sparkproject.constant;

/**
 * 常量接口
 * 
 * @author Administrator
 *
 */
public interface Constants {

	/**
	 * 项目配置相关的常量
	 */

	String JDBC_DRIVER = "jdbc.driver";
	String JDBC_DATASOURCE_SIZE = "jdbc.datasource.size";
	String JDBC_URL = "jdbc.url";
	String JDBC_USER = "jdbc.user";
	String JDBC_PASSWORD = "jdbc.password";
	String JDBC_URL_PROD = "jdbc.url.prod";
	String JDBC_USER_PROD = "jdbc.user.prod";
	String JDBC_PASSWORD_PROD = "jdbc.password.prod";
	String SPARK_LOCAL = "local";
	String KAFKA_METADATA_BROKER_LIST = "kafka.metadata.broker.list";
	String KAFKA_TOPICS = "kafka.topics";

	/**
	 * Spark作业相关的常量
	 */
	String SPARK_APP_NAME_TEST_ON_Line = "HuiMaiDateDAOnline";

}
