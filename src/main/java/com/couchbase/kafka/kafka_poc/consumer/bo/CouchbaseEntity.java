package com.couchbase.kafka.kafka_poc.consumer.bo;

import com.fasterxml.jackson.annotation.JsonProperty;

public class CouchbaseEntity {
	
	private String key;
	private String cas;
	@JsonProperty("value") 
	private UserProfile userProfile;
	public String getKey() {
		return key;
	}
	public void setKey(String key) {
		this.key = key;
	}
	public String getCas() {
		return cas;
	}
	public void setCas(String cas) {
		this.cas = cas;
	}
	public UserProfile getUserProfile() {
		return userProfile;
	}
	public void setUserProfile(UserProfile userProfile) {
		this.userProfile = userProfile;
	}
	
	

}
