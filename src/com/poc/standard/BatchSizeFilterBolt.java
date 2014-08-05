package com.poc.standard;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class BatchSizeFilterBolt extends BaseBasicBolt {

	private static final Logger LOG = LoggerFactory
			.getLogger(BatchSizeFilterBolt.class);

	List<String> keyGeoPubTimeUpToMinuteList = null;
	List<String> keyGeoPubFullTimeList = null;
	Map<String, Map<String, List<List<String>>>> tmpholderForGeoPubTimeFreqMap = null;
	Map<String, List<List<String>>> geoPubTimeHolderMap = null;
	String previousKey = null;
	String currentKey = null;
	int batchSize;
	Integer id;
	String name;

	public BatchSizeFilterBolt(int batchSize) {
		this.batchSize = batchSize;

	}

	@Override
	public void prepare(Map stormConf, TopologyContext context) {
		keyGeoPubTimeUpToMinuteList = new ArrayList<String>();
		keyGeoPubFullTimeList = new ArrayList<String>();
		tmpholderForGeoPubTimeFreqMap = new HashMap<String, Map<String, List<List<String>>>>();
		
		this.name = context.getThisComponentId();
		this.id = context.getThisTaskId();
	}

	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
	//	System.out.println("-- execute ["+name+"-"+id+"] --");

		//System.out.println(" BatchSizeFilterBolt... " + input);
	    
		//System.out.println(" keyGeoPubTimeUpToMinuteList... " + keyGeoPubTimeUpToMinuteList);
		//System.out.println(" in the start keyGeoPubFullTimeList... " + keyGeoPubFullTimeList);
		//System.out.println(" in the start tmpholderForGeoPubTimeFreqMap... " + tmpholderForGeoPubTimeFreqMap);
	    
	    processEmit(collector);
		
		List<String> logRow = new ArrayList<String>();
		// geo
		logRow.add(input.getString(1));
		// pub
		logRow.add(input.getString(2));
		// website
		logRow.add(input.getString(4));
		// bid
		logRow.add(input.getString(5));
		// date
		logRow.add(input.getString(7));
		String filterKeyTime = input.getString(7);

		// concurrentMap.put(input.getString(0), row);
		/*
		 * if(concurrentMap.size() == this.batchSize){ collector.emit(new
		 * Values(concurrentMap)); concurrentMap = new ConcurrentHashMap<String,
		 * List<String>>(); }
		 */

		String keyGeoPub = input.getString(1) + "~"+input.getString(2);
		String keyGeoPubFullTimePub = keyGeoPub +"~"+ filterKeyTime;
		String keyGeoPubMinutePub = keyGeoPub+"~"+ getDateUptoMinute(filterKeyTime);

		
	
		if (keyGeoPubFullTimeList.size() > 0 && keyGeoPubFullTimeList.contains(keyGeoPubFullTimePub)) {
			// here you are getting duplicate, count these duplicates.
			// this will become candidates of total impression
			//System.out.println(" duplicate... " + keyGeoPubFullTimePub);
			geoPubTimeHolderMap = tmpholderForGeoPubTimeFreqMap.get(keyGeoPubMinutePub);
			List<List<String>> holderList = geoPubTimeHolderMap.get(keyGeoPubMinutePub+"impression");
			//System.out.println(" holderList... " + holderList);
			int total = Integer.parseInt(holderList.get(0).get(0))+1;
			//System.out.println(" total... " + total);
			holderList.get(0).remove(0);
		    holderList.get(0).add(0,(new Integer(total)).toString());
			//System.out.println("total holderList ...... "+holderList);
		    geoPubTimeHolderMap.put(keyGeoPubMinutePub+"impression", holderList);

		} else {

			// you got key with slight differences in second.
			if (keyGeoPubFullTimeList.size() > 0 && CheckStartWithInList(keyGeoPubFullTimeList, keyGeoPubMinutePub)) {

				//System.out.println(" you got key with slight difference in ts "
					//	+ keyGeoPubMinutePub);

				//System.out.println(" geoPubTimeHolderMap "
					//	+ geoPubTimeHolderMap);

				// These rows of logs can go for analysis together
				// club them in the single hash map.

				geoPubTimeHolderMap = tmpholderForGeoPubTimeFreqMap
						.get(keyGeoPubMinutePub);
				// geoPubTimeFreqMap.
				List<List<String>> holderList = geoPubTimeHolderMap
						.get(keyGeoPubMinutePub);
				holderList.add(logRow);
				geoPubTimeHolderMap.put(keyGeoPubMinutePub, holderList);
				// impressions...
				
				List<List<String>> holderList1 = geoPubTimeHolderMap.get(keyGeoPubMinutePub+"impression");
				//System.out.println(" holderList1 "
					//	+ holderList1);

				int total = Integer.parseInt(holderList1.get(0).get(0))+1;
				holderList1.get(0).remove(0);
			    holderList1.get(0).add(0,(new Integer(total)).toString());
				//System.out.println("total holderList ...... "+holderList);
			    geoPubTimeHolderMap.put(keyGeoPubMinutePub+"impression", holderList1);

				
				tmpholderForGeoPubTimeFreqMap.put(keyGeoPubMinutePub,
						geoPubTimeHolderMap);

			} else { // you got brand new key

		     	geoPubTimeHolderMap = new HashMap<String, List<List<String>>>();
				List<List<String>> holderList = new ArrayList<List<String>>();
				holderList.add(logRow);
				geoPubTimeHolderMap.put(keyGeoPubMinutePub, holderList);
			

				// impression
				List<List<String>> holderList1 = new ArrayList<List<String>>();
				List<String> impression = new ArrayList<String>();
				impression.add(0,new Integer(1).toString());
				holderList1.add(impression);
			    geoPubTimeHolderMap.put(keyGeoPubMinutePub+"impression", holderList1);
			    
				tmpholderForGeoPubTimeFreqMap.put(keyGeoPubMinutePub,
						geoPubTimeHolderMap);
			}
			
			// common activites.
			// list for time upto minutes
			if(	!keyGeoPubTimeUpToMinuteList.contains(keyGeoPubMinutePub)) {
			     keyGeoPubTimeUpToMinuteList.add(keyGeoPubMinutePub);
			}
			// full tim
			keyGeoPubFullTimeList.add(keyGeoPubFullTimePub);

		}

		//System.out.println(" tmpholderForGeoPubTimeFreqMap "
			//	+ tmpholderForGeoPubTimeFreqMap);
		
		//System.out.println(" tmpholderForGeoPubTimeFreqMap  size () "
			//	+ tmpholderForGeoPubTimeFreqMap.size());
	}

	private void processEmit(BasicOutputCollector collector) {
		// TODO Auto-generated method stub
	   SimpleDateFormat DATE_FORMAT = new SimpleDateFormat(
				"yyyy-MM-dd hh:mm:ss aa");
       String fullTime = DATE_FORMAT.format(System.currentTimeMillis());
		
       String currentTimeUpToMinute = getDateUptoMinute(fullTime);
		// check all key in this map, keyGeoPubTimeUpToMinuteList
       for(int i=0;i<keyGeoPubTimeUpToMinuteList.size();i++) {
    	   
    	   String timeToCheck =  keyGeoPubTimeUpToMinuteList.get(i).split("~")[2];
    	   
    	   long diff = getDateUptoMinuteByDate(currentTimeUpToMinute).getTime() - getDateUptoMinuteByDate(timeToCheck).getTime();
    	   long diffMinutes = diff / (60 * 1000) % 60;
			if(diffMinutes > 1) {
    		   
				
				//List<String> keyGeoPubTimeUpToMinuteList = null;
				//List<String> keyGeoPubFullTimeList = null;
				//Map<String, Map<String, List<List<String>>>> tmpholderForGeoPubTimeFreqMap = null;
				String minuteKey = keyGeoPubTimeUpToMinuteList.get(i);
				System.out.println("send these keys to next bolt "+minuteKey);
				Values val = new Values();
		     	val.add(tmpholderForGeoPubTimeFreqMap.get(minuteKey));
				
		     	//System.out.println(" before ....keyGeoPubTimeUpToMinuteList... " + keyGeoPubTimeUpToMinuteList);
				//System.out.println(" before .. keyGeoPubFullTimeList... " + keyGeoPubFullTimeList);
				//System.out.println(" before .. tmpholderForGeoPubTimeFreqMap... " + tmpholderForGeoPubTimeFreqMap);
				
		     	collector.emit(val);
		
		     	tmpholderForGeoPubTimeFreqMap.remove(minuteKey);
				List<String> indexesToRemove = prepareStartWithInList(keyGeoPubFullTimeList,minuteKey);
				
				for(int in=0; in < indexesToRemove.size();in++) {
					keyGeoPubFullTimeList.remove(0);
				}
		
				// remove this last
			    keyGeoPubTimeUpToMinuteList.remove(i);
				System.out.println(" after ....keyGeoPubTimeUpToMinuteList... " + keyGeoPubTimeUpToMinuteList);
				System.out.println(" after .. keyGeoPubFullTimeList... " + keyGeoPubFullTimeList);
				System.out.println(" after .. tmpholderForGeoPubTimeFreqMap... " + tmpholderForGeoPubTimeFreqMap);
			    
			  
						
				}
    	   
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

	private String getDateUptoMinuteFirstDigit(String field) {
		SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd hh:m");
		String dateUptoMinute = null;
		try {
			dateUptoMinute = df.format(df.parse(field));
		} catch (ParseException e) {
		}
		return dateUptoMinute;
	}

	private Date getDateUptoMinuteByDate(String field) {
		SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd hh:mm");
		Date dateUptoMinute = null;
		try {
			dateUptoMinute = df.parse(field);
		} catch (ParseException e) {
		}
		return dateUptoMinute;
	}
	private boolean CheckStartWithInList(List<String> keyGeoPubFullTimeList,
			String keyGeoPubMinutePub) {
	 if(keyGeoPubFullTimeList.size() > 0) {
		for (int i = 0; i < keyGeoPubFullTimeList.size(); i++) {
			if (keyGeoPubFullTimeList.get(i).startsWith(keyGeoPubMinutePub)) {
				return true;
			} 
		}
	  }	
		return false;
	}

	
	private List<String> prepareStartWithInList(List<String> keyGeoPubFullTimeList,
			String keyGeoPubMinutePub) {

		List<String> indexToRemove = new ArrayList<String>();
		if(keyGeoPubFullTimeList.size() > 0) {
		for (int i = 0; i < keyGeoPubFullTimeList.size(); i++) {
			if (keyGeoPubFullTimeList.get(i).startsWith(keyGeoPubMinutePub)) {
				indexToRemove.add(new Integer(i).toString());
			} 
		}
	  }	
		return indexToRemove;
	}
	
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("batch"));
	}

}
