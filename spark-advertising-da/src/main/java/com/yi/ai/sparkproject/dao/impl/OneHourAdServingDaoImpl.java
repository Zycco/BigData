package com.yi.ai.sparkproject.dao.impl;

import com.yi.ai.sparkproject.dao.OneHourAdServingDao;
import com.yi.ai.sparkproject.domain.OneHourAdServingPojo;
import com.yi.ai.sparkproject.jdbc.JDBCHelper;

public class OneHourAdServingDaoImpl implements OneHourAdServingDao {
	JDBCHelper jdbcHelper = JDBCHelper.getInstance();

	@Override
	public void insertOneHourAdServing(OneHourAdServingPojo oneHourAdServingPojo) {
		// TODO Auto-generated method stub

		String sql = "INSERT INTO onehouradserving VALUES(?,?,?,?,?,?,?)";

		Object[] params = new Object[] { oneHourAdServingPojo.getLog_date(), oneHourAdServingPojo.getLog_hour(),
				oneHourAdServingPojo.getAd_id(), oneHourAdServingPojo.getAd_name(),
				oneHourAdServingPojo.getAdvertiser_id(), oneHourAdServingPojo.getClick_count(),
				oneHourAdServingPojo.getExpose_count() };

		jdbcHelper.executeUpdate(sql, params);

	}

}
