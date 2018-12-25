package com.yi.ai.sparkproject.dao.factory;

import com.yi.ai.sparkproject.dao.OneHourAdExposeDao;
import com.yi.ai.sparkproject.dao.OneHourAdServingDao;
import com.yi.ai.sparkproject.dao.impl.OneHourAdExposeDaoImpl;
import com.yi.ai.sparkproject.dao.impl.OneHourAdServingDaoImpl;

public class DaoFactory {
	public static OneHourAdExposeDao getOneHourAdExposeDao() {
		return new OneHourAdExposeDaoImpl();
	}

	public static OneHourAdServingDao getOneHourAdServing() {
		return new OneHourAdServingDaoImpl();
	}

}
