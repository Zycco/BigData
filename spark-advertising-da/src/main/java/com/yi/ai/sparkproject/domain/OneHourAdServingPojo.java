package com.yi.ai.sparkproject.domain;

import java.io.Serializable;

public class OneHourAdServingPojo implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	private String log_date;
	private String log_hour;
	private String ad_id;
	private String ad_name;
	private String advertiser_id;
	private long click_count;
	private long expose_count;

	public OneHourAdServingPojo() {
		super();
	}

	public OneHourAdServingPojo(String log_date, String log_hour, String ad_id, String ad_name, String advertiser_id,
			long click_count, long expose_count) {
		super();
		this.log_date = log_date;
		this.log_hour = log_hour;
		this.ad_id = ad_id;
		this.ad_name = ad_name;
		this.advertiser_id = advertiser_id;
		this.click_count = click_count;
		this.expose_count = expose_count;
	}

	public String getLog_date() {
		return log_date;
	}

	public void setLog_date(String log_date) {
		this.log_date = log_date;
	}

	public String getLog_hour() {
		return log_hour;
	}

	public void setLog_hour(String log_hour) {
		this.log_hour = log_hour;
	}

	public String getAd_id() {
		return ad_id;
	}

	public void setAd_id(String ad_id) {
		this.ad_id = ad_id;
	}

	public String getAd_name() {
		return ad_name;
	}

	public void setAd_name(String ad_name) {
		this.ad_name = ad_name;
	}

	public String getAdvertiser_id() {
		return advertiser_id;
	}

	public void setAdvertiser_id(String advertiser_id) {
		this.advertiser_id = advertiser_id;
	}

	public long getClick_count() {
		return click_count;
	}

	public void setClick_count(long click_count) {
		this.click_count = click_count;
	}

	public long getExpose_count() {
		return expose_count;
	}

	public void setExpose_count(long expose_count) {
		this.expose_count = expose_count;
	}

	public static long getSerialversionuid() {
		return serialVersionUID;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((ad_id == null) ? 0 : ad_id.hashCode());
		result = prime * result + ((ad_name == null) ? 0 : ad_name.hashCode());
		result = prime * result + ((advertiser_id == null) ? 0 : advertiser_id.hashCode());
		result = prime * result + (int) (click_count ^ (click_count >>> 32));
		result = prime * result + (int) (expose_count ^ (expose_count >>> 32));
		result = prime * result + ((log_date == null) ? 0 : log_date.hashCode());
		result = prime * result + ((log_hour == null) ? 0 : log_hour.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		OneHourAdServingPojo other = (OneHourAdServingPojo) obj;
		if (ad_id == null) {
			if (other.ad_id != null)
				return false;
		} else if (!ad_id.equals(other.ad_id))
			return false;
		if (ad_name == null) {
			if (other.ad_name != null)
				return false;
		} else if (!ad_name.equals(other.ad_name))
			return false;
		if (advertiser_id == null) {
			if (other.advertiser_id != null)
				return false;
		} else if (!advertiser_id.equals(other.advertiser_id))
			return false;
		if (click_count != other.click_count)
			return false;
		if (expose_count != other.expose_count)
			return false;
		if (log_date == null) {
			if (other.log_date != null)
				return false;
		} else if (!log_date.equals(other.log_date))
			return false;
		if (log_hour == null) {
			if (other.log_hour != null)
				return false;
		} else if (!log_hour.equals(other.log_hour))
			return false;
		return true;
	}

	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append("OneHourAdServingPojo [log_date=");
		builder.append(log_date);
		builder.append(", log_hour=");
		builder.append(log_hour);
		builder.append(", ad_id=");
		builder.append(ad_id);
		builder.append(", ad_name=");
		builder.append(ad_name);
		builder.append(", advertiser_id=");
		builder.append(advertiser_id);
		builder.append(", click_count=");
		builder.append(click_count);
		builder.append(", expose_count=");
		builder.append(expose_count);
		builder.append("]");
		return builder.toString();
	}

}
