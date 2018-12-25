package com.yi.ai.sparkproject.dao.impl;

import com.yi.ai.sparkproject.dao.OneHourAdExposeDao;
import com.yi.ai.sparkproject.domain.OneHourAdExposurePojo;
import com.yi.ai.sparkproject.jdbc.JDBCHelper;

public class OneHourAdExposeDaoImpl implements OneHourAdExposeDao {
	JDBCHelper jdbcHelper = JDBCHelper.getInstance();

	@Override
	public void insertOneHourExposeDa(OneHourAdExposurePojo oneHourAdExposurePojo) {
		String sql = "INSERT INTO onehouradexposure VALUES(?,?,?,?,?,?,?,?,?)";

		Object[] params = new Object[] { oneHourAdExposurePojo.getLog_date(), oneHourAdExposurePojo.getLog_hour(),
				oneHourAdExposurePojo.getMedia_id(), oneHourAdExposurePojo.getVideo_resource_id(),
				oneHourAdExposurePojo.getVideo_name(), oneHourAdExposurePojo.getScene_id(),
				oneHourAdExposurePojo.getStyle_id(), oneHourAdExposurePojo.getClick_count(),
				oneHourAdExposurePojo.getExpose_count() };

		jdbcHelper.executeUpdate(sql, params);

	}

}
