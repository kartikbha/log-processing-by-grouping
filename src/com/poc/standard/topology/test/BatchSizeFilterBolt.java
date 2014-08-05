package com.poc.standard.topology.test;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import backtype.storm.tuple.Values;

public class BatchSizeFilterBolt {
	
	HashMap<String, List<List<String>>> concurrentMap = new HashMap<String, List<List<String>>>();
	List<String> keyGeoPubTimeUpToMinuteList = new ArrayList<String>();
	List<String> keyGeoPubFullTimeList = new ArrayList<String>();
	List<String> holderList = new ArrayList<String>();
	Map<String, Map<String, List<List<String>>>> tmpholderForGeoPubTimeFreqMap = new HashMap<String, Map<String, List<List<String>>>>();
	Map<String, List<List<String>>> geoPubTimeHolderMap = null;
	String previousKey = null;
	String currentKey = null;
	int batchSize;

	
	public void execute(Values input){
		System.out.println(" BatchSizeFilterBolt... "+input);
		
		
		List<String> logRow = new ArrayList<String>();
		// geo
		logRow.add((String) input.get(1));
		// pub
		logRow.add((String) input.get(2));
		// website
		logRow.add((String) input.get(4));
		// bid
		logRow.add((String) input.get(5));
		// date
		logRow.add((String) input.get(7));
		String filterKeyTime = (String) input.get(7);

		// concurrentMap.put(input.getString(0), row);
		/*
		 * if(concurrentMap.size() == this.batchSize){ collector.emit(new
		 * Values(concurrentMap)); concurrentMap = new ConcurrentHashMap<String,
		 * List<String>>(); }
		 */

		String keyGeoPub = (String)input.get(1) + (String)input.get(2);
		String keyGeoPubFullTimePub = keyGeoPub + filterKeyTime;
		String keyGeoPubMinutePub = keyGeoPub
				+ getDateUptoMinute(filterKeyTime);

		// currentKey = keyGeoPubMinutePub;
		// previousKey

		if (keyGeoPubFullTimeList.contains(keyGeoPubFullTimePub)) {
			// here you are getting duplicate, count these duplicates.
			// this will become candidates of total impression

		} else {
			keyGeoPubFullTimeList.add(keyGeoPubFullTimePub);

			// you got key with slight differences in second.
			if (keyGeoPubFullTimeList.contains(keyGeoPubMinutePub)) {
				// These rows of logs can go for analysis together
				// club them in the single hash map.

				// list for time upto minutes
				// keyGeoPubTimeUpToMinuteList.add(keyGeoPubMinutePub);
            	geoPubTimeHolderMap = tmpholderForGeoPubTimeFreqMap
						.get(keyGeoPubMinutePub);
				// geoPubTimeFreqMap.
				List<List<String>> holderList = geoPubTimeHolderMap
						.get(keyGeoPubMinutePub);
				holderList.add(logRow);
				geoPubTimeHolderMap.put(keyGeoPubMinutePub, holderList);

				// you got brand new key
			} else {
				geoPubTimeHolderMap = new HashMap<String, List<List<String>>>();
				List<List<String>> holderList = new ArrayList<List<String>>();
				holderList.add(logRow);
				geoPubTimeHolderMap.put(keyGeoPubMinutePub, holderList);
				tmpholderForGeoPubTimeFreqMap.put(keyGeoPubMinutePub,geoPubTimeHolderMap);

			}

		}

		System.out.println(" tmpholderForGeoPubTimeFreqMap "+tmpholderForGeoPubTimeFreqMap);

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
