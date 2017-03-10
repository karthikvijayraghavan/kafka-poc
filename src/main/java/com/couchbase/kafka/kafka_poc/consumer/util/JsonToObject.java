package com.couchbase.kafka.kafka_poc.consumer.util;

import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

import com.couchbase.kafka.kafka_poc.consumer.bo.CouchbaseEntity;
import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class JsonToObject {

	public static CouchbaseEntity convertJsonStringToObject(String jsonString) {
		ObjectMapper mapper = new ObjectMapper();
		CouchbaseEntity couchbaseEntity = null;
		try {

			// Convert JSON string to Object

			couchbaseEntity = mapper.readValue(jsonString, CouchbaseEntity.class);

			// Pretty print
			String prettyCouchbaseEntity = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(couchbaseEntity);
			System.out.println(prettyCouchbaseEntity);

		} catch (JsonGenerationException e) {
			e.printStackTrace();
		} catch (JsonMappingException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return couchbaseEntity;
	}

}
