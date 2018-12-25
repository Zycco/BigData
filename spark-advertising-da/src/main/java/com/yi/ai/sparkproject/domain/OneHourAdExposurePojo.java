package com.yi.ai.sparkproject.domain;

import java.io.Serializable;

public class OneHourAdExposurePojo implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	private String log_date;
	private String log_hour;
	private String media_id;
	private String video_resource_id;
	private String video_name;
	private String scene_id;
	private String style_id;
	private long click_count;
	private long expose_count;

	public OneHourAdExposurePojo() {
		super();
	}

	public OneHourAdExposurePojo(String log_date, String log_hour, String media_id, String video_resource_id,
			String video_name, String scene_id, String style_id, long click_count, long expose_count) {
		super();
		this.log_date = log_date;
		this.log_hour = log_hour;
		this.media_id = media_id;
		this.video_resource_id = video_resource_id;
		this.video_name = video_name;
		this.scene_id = scene_id;
		this.style_id = style_id;
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

	public String getMedia_id() {
		return media_id;
	}

	public void setMedia_id(String media_id) {
		this.media_id = media_id;
	}

	public String getVideo_resource_id() {
		return video_resource_id;
	}

	public void setVideo_resource_id(String video_resource_id) {
		this.video_resource_id = video_resource_id;
	}

	public String getVideo_name() {
		return video_name;
	}

	public void setVideo_name(String video_name) {
		this.video_name = video_name;
	}

	public String getScene_id() {
		return scene_id;
	}

	public void setScene_id(String scene_id) {
		this.scene_id = scene_id;
	}

	public String getStyle_id() {
		return style_id;
	}

	public void setStyle_id(String style_id) {
		this.style_id = style_id;
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
		result = prime * result + (int) (click_count ^ (click_count >>> 32));
		result = prime * result + (int) (expose_count ^ (expose_count >>> 32));
		result = prime * result + ((log_date == null) ? 0 : log_date.hashCode());
		result = prime * result + ((log_hour == null) ? 0 : log_hour.hashCode());
		result = prime * result + ((media_id == null) ? 0 : media_id.hashCode());
		result = prime * result + ((scene_id == null) ? 0 : scene_id.hashCode());
		result = prime * result + ((style_id == null) ? 0 : style_id.hashCode());
		result = prime * result + ((video_name == null) ? 0 : video_name.hashCode());
		result = prime * result + ((video_resource_id == null) ? 0 : video_resource_id.hashCode());
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
		OneHourAdExposurePojo other = (OneHourAdExposurePojo) obj;
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
		if (media_id == null) {
			if (other.media_id != null)
				return false;
		} else if (!media_id.equals(other.media_id))
			return false;
		if (scene_id == null) {
			if (other.scene_id != null)
				return false;
		} else if (!scene_id.equals(other.scene_id))
			return false;
		if (style_id == null) {
			if (other.style_id != null)
				return false;
		} else if (!style_id.equals(other.style_id))
			return false;
		if (video_name == null) {
			if (other.video_name != null)
				return false;
		} else if (!video_name.equals(other.video_name))
			return false;
		if (video_resource_id == null) {
			if (other.video_resource_id != null)
				return false;
		} else if (!video_resource_id.equals(other.video_resource_id))
			return false;
		return true;
	}

	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append("OneHourAdExposurePojo [log_date=");
		builder.append(log_date);
		builder.append(", log_hour=");
		builder.append(log_hour);
		builder.append(", media_id=");
		builder.append(media_id);
		builder.append(", video_resource_id=");
		builder.append(video_resource_id);
		builder.append(", video_name=");
		builder.append(video_name);
		builder.append(", scene_id=");
		builder.append(scene_id);
		builder.append(", style_id=");
		builder.append(style_id);
		builder.append(", click_count=");
		builder.append(click_count);
		builder.append(", expose_count=");
		builder.append(expose_count);
		builder.append("]");
		return builder.toString();
	}

}
