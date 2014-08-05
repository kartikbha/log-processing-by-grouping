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

	//List<String> keyGeoPubTimeUpToMinuteList = null;
	List<String> keyGeoPubFullTimeList = null;
	Map<String, Map<String, List<List<String>>>> tmpholderForGeoPubTimeFreqMap = null;
	Map<String, List<List<String>>> geoPubTimeHolderMap = null;
	String previousKey = null;
	String currentKey = null;
	int batchSize;
	Integer id;
	String name;

	

	@Override
	public void prepare(Map stormConf, TopologyContext context) {
		//keyGeoPubTimeUpToMinuteList = new ArrayList<String>();
		keyGeoPubFullTimeList = new ArrayList<String>();
		tmpholderForGeoPubTimeFreqMap = new HashMap<String, Map<String, List<List<String>>>>();
	
		this.name = context.getThisComponentId();
		this.id = context.getThisTaskId();
	}

	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
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
		String keyGeoPub = input.getString(1) + "~"+input.getString(2);
		String keyGeoPubFullTimePub = keyGeoPub +"~"+ filterKeyTime;
		String keyGeoPubMinutePub = keyGeoPub+"~"+ getDateUptoMinute(filterKeyTime);
		if (keyGeoPubFullTimeList.size() > 0 && keyGeoPubFullTimeList.contains(keyGeoPubFullTimePub) && tmpholderForGeoPubTimeFreqMap.containsKey(keyGeoPubMinutePub)) {
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
			if (keyGeoPubFullTimeList.size() > 0 && CheckStartWithInList(keyGeoPubFullTimeList, keyGeoPubMinutePub) && tmpholderForGeoPubTimeFreqMap.containsKey(keyGeoPubMinutePub)) {
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
			keyGeoPubFullTimeList.add(keyGeoPubFullTimePub);
        }
	}

	private void processEmit(BasicOutputCollector collector) {
		// TODO Auto-generated method stub
	   SimpleDateFormat DATE_FORMAT = new SimpleDateFormat(
				"yyyy-MM-dd hh:mm:ss aa");
       String currentTime = DATE_FORMAT.format(System.currentTimeMillis());
       for(int i=0;i<keyGeoPubFullTimeList.size();i++) {
       	   String[] geoPubFullTimeKey =  keyGeoPubFullTimeList.get(i).split("~");
    	   String geo = geoPubFullTimeKey[0];
    	   String pub = geoPubFullTimeKey[1];
    	   String fullTime = geoPubFullTimeKey[2];
      	   long diff = getFullTimeByDate(currentTime).getTime() - getFullTimeByDate(fullTime).getTime();
    	   long diffSeconds = diff / 1000 % 60;
    	   String geoPubMinuteKey = geo+"~"+pub+"~"+getDateUptoMinute(fullTime);
   	      if(diffSeconds > 10 && tmpholderForGeoPubTimeFreqMap.containsKey(geoPubMinuteKey)) {
     		 	Values val = new Values();
		     	val.add(tmpholderForGeoPubTimeFreqMap.get(geoPubMinuteKey));
     	       	// emit
		     	collector.emit(val);
				tmpholderForGeoPubTimeFreqMap.remove(geoPubMinuteKey);
				for(int index=0;index<prepareStartWithInList(keyGeoPubFullTimeList,geoPubMinuteKey).size();index++) {
        			   List<String> currentIndexes = prepareStartWithInList(keyGeoPubFullTimeList,geoPubMinuteKey);
					  // System.out.println("currentIndex  ..."+currentIndexes);
					   keyGeoPubFullTimeList.remove(currentIndexes.get(0));
		    	}
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
	private Date getFullTimeByDate(String field) {
		SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss aa");
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
