package com.poc.standard.topology.test;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

public class AnalysisLogicBolt {

	public void execute(ConcurrentHashMap<String, List<String>> mapForAnalysis) {

		Map <String,Map <String, List<List<Integer>>>> tmpholderForGeoPubTimeFreqMap = new HashMap<String, Map<String, List<List<Integer>>>>();
		Map<String, List<Integer>> tmpholderForIndexMap = new HashMap<String, List<Integer>>();
		
	    System.out.println(" mapForAnalysis " + mapForAnalysis);
        List<String> keyGeoPubTimeUpToMinuteList = new ArrayList<String>();
		List<String> keyGeoPubFullTimeList = new ArrayList<String>();
		Map<String, List<List<Integer>>> geoPubTimeFreqMap = null;
		List<Integer> indexList = null;
	    
		for (Entry<String, List<String>> entry : mapForAnalysis.entrySet()) {
			List<String> logRow = entry.getValue();
			// key are geo,pub,time upto minute.
			String index = entry.getKey();
        	String keyGeoPub = logRow.get(0) + logRow.get(1);
			String keyGeoPubFullTimePub = keyGeoPub + logRow.get(4);
			String keyGeoPubMinutePub = keyGeoPub
					+ getDateUptoMinute(logRow.get(4));

			// this is just a duplicate log impression, count it frequency but
			// don't use in calculation
			if (keyGeoPubFullTimeList.contains(keyGeoPubFullTimePub)) {
				
				System.out.println("keyGeoPubFullTimeList ..... "+keyGeoPubFullTimeList);
				// this i should count to tell total impression received.
				geoPubTimeFreqMap = tmpholderForGeoPubTimeFreqMap.get(keyGeoPubMinutePub);
				List<List<Integer>> holderList = geoPubTimeFreqMap.get(keyGeoPubMinutePub);
				int total = holderList.get(1).get(0)+new Integer(1);
				holderList.get(1).remove(0);
			    holderList.get(1).add(0,total);
				System.out.println("total holderList ...... "+holderList);
				geoPubTimeFreqMap.put(keyGeoPubMinutePub, holderList);
				//geoPubTimeFreqMap.put(keyGeoPubMinutePub,geoPubTimeFreqMap.get(keyGeoPubMinutePub).get(1).add(newImpressionCount));
		
     	  } else {
		   
				keyGeoPubFullTimeList.add(keyGeoPubFullTimePub);
				// prepare Frequencies, indexes Map
				if (!keyGeoPubTimeUpToMinuteList.contains(keyGeoPubMinutePub)) {
					indexList = new ArrayList<Integer>();
					keyGeoPubTimeUpToMinuteList.add(keyGeoPubMinutePub);
					
					geoPubTimeFreqMap = new HashMap<String,List<List<Integer>>>();
					tmpholderForGeoPubTimeFreqMap.put(keyGeoPubMinutePub,
							geoPubTimeFreqMap);
					tmpholderForIndexMap.put(keyGeoPubMinutePub,indexList);
					
				} else {
					geoPubTimeFreqMap = tmpholderForGeoPubTimeFreqMap.get(keyGeoPubMinutePub);
					indexList = tmpholderForIndexMap.get(keyGeoPubMinutePub);
					
				}
				
				
				collectFrequencyBasedOnKey(keyGeoPubMinutePub, geoPubTimeFreqMap,index,indexList);
				
				/*
				 * 
				 * Map<String,List<List<String>>>>
				 * 
				 * { WApub12014-08-04 12:30 = [[1,2,3,4], [20]}, 
				 * 
				 * 
				 * {
				 * WApub52014-08-04 12:30 = [[49, 46], [35]},
				 * 
				 *  
				 *  { WApub52014-08-04 12:30 = [[4,5,6,7],[32]]}
				 * 
				 */

			}

			// System.out.println(" geoPubTimeFreqMap " + geoPubTimeFreqMap);
			// System.out.println(" geoPubTimeIndexMap " + geoPubTimeIndexMap);

			//List<Map<String, List<Integer>>> values = new ArrayList<Map<String, List<Integer>>>();
		    //values.addAll(geoPubTimeFreqMap);
			//aggregateMap.put(keyGeoPubMinutePub, values);

		}

		System.out.println(" aggregateMap " + tmpholderForGeoPubTimeFreqMap);
	}

	

	private void collectFrequencyBasedOnKey(String keyGeoPubTime,
			Map<String, List<List<Integer>>> geoPubTimeFreqMap, String index, List<Integer> indexList) {
	/*
		 * Map<String,List<List<String>>>> 
		 * 
		 * { WApub12014-08-04 12:30 = [ [1,2,3,4], [20] ]}, 
		 * 
		 * { WApub52014-08-04 12:30 = [[49, 46], [35]}, 
		 * 
		 * {WApub52014-08-04 12:30 = [[4,5,6,7],[32]]}
		 * 
		 */

	  if (geoPubTimeFreqMap.containsKey(keyGeoPubTime)) {
			List<List<Integer>> holderList = geoPubTimeFreqMap.get(keyGeoPubTime);
			holderList.get(0).add(new Integer(Integer.parseInt(index)));
			
			// add up with old value
			List<Integer> total = new ArrayList<Integer>();
			int totalSum = holderList.get(1).get(0)+new Integer(1);
			System.out.println("total sum all the time. "+totalSum);
			holderList.get(1).remove(0);
		    holderList.get(1).add(0,totalSum);
			System.out.println(" unique holderList ...... "+holderList);
			geoPubTimeFreqMap.put(keyGeoPubTime, holderList);
       } else {
    	     // first time
    	    // list of indexes to maintain
			List<Integer> indexes = new ArrayList<Integer>();
			indexes.add(new Integer(Integer.parseInt(index)));
			// total
			List<Integer> total = new ArrayList<Integer>();
			total.add(new Integer(1));
			// add to holder list
			List<List<Integer>> holderList = new ArrayList<List<Integer>>();
			holderList.add(indexes);
			holderList.add(total);
			geoPubTimeFreqMap.put(keyGeoPubTime, holderList);
	   }


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
