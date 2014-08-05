package com.poc.standard.topology.test;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PersistancePreprationBolt {

	private static final Logger LOG = LoggerFactory
			.getLogger(PersistancePreprationBolt.class);

	public void execute(
			Map<String, Map<String, List<List<Integer>>>> aggregatedMap,
			ConcurrentMap<String, List<String>> totalMap) {

		
	
	}

	private String getDateUptoMinute(String field) {
		SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd hh:mm");
		String dateUptoMinute = null;
		try {
			dateUptoMinute = df.format(df.parse(field));
		} catch (ParseException e) {
		}
		return dateUptoMinute;
	}
}
