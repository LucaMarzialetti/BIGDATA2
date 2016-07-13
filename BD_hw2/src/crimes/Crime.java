package crimes;

import java.io.Serializable;

@SuppressWarnings("serial")
public class Crime implements Serializable {
	
	private String lsoa_code;
	private String lsoa_name;
	private String super_group;
	private String group;
	private String sub_group;
	private String lat;
	private String lon;
	private String location;
	private String month;
	private String crime_type;
	private String outcome;
	
	@Override
	public String toString() {
		return "Crime [lsoa_code=" + lsoa_code + ", lsoa_name=" + lsoa_name + ", super_group=" + super_group + ", group=" + group + ", sub_group="
				+ sub_group + ", lat=" + lat + ", lon=" + lon + ", location=" + location + ", month=" + month
				+ ", crime_type=" + crime_type + ", outcome=" + outcome + "]";
	}
	public String getLsoa_code() {
		return lsoa_code;
	}
	public String getLsoa_name() {
		return lsoa_name;
	}
	public String getSuper_group() {
		return super_group;
	}
	public String getGroup() {
		return group;
	}
	public String getSub_group() {
		return sub_group;
	}
	public String getLat() {
		return lat;
	}
	public String getLon() {
		return lon;
	}
	public String getLocation() {
		return location;
	}
	public String getMonth() {
		return month;
	}
	public String getCrime_type() {
		return crime_type;
	}
	public String getOutcome() {
		return outcome;
	}
	public void setLsoa_code(String lsoa_code) {
		this.lsoa_code = lsoa_code;
	}
	public void setLsoa_name(String lsoa_name) {
		this.lsoa_name = lsoa_name;
	}
	public void setSuper_group(String super_group) {
		this.super_group = super_group;
	}
	public void setGroup(String group) {
		this.group = group;
	}
	public void setSub_group(String sub_group) {
		this.sub_group = sub_group;
	}
	public void setLat(String lat) {
		this.lat = lat;
	}
	public void setLon(String lon) {
		this.lon = lon;
	}
	public void setLocation(String location) {
		this.location = location;
	}
	public void setMonth(String month) {
		this.month = month;
	}
	public void setCrime_type(String crime_type) {
		this.crime_type = crime_type;
	}
	public void setOutcome(String outcome) {
		this.outcome = outcome;
	}
	
}
