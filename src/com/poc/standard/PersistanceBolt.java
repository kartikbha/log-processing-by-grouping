package com.poc.standard;

import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.Mongo;

public class PersistanceBolt extends BaseRichBolt {
	
	private static final Logger LOG = LoggerFactory
			.getLogger(PersistanceBolt.class);
	
	String MONDO_DB_HOST = "localhost";
	String MONDO_DB = "test2";
	String MONDO_DB_COLLECTION = "testcollection2";
	int MONDO_DB_PORT = 27017;
	

	public void execute(Tuple input, BasicOutputCollector collector) {

	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		
	}

	DB db;
    public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		
		Mongo mongo = null;
		try {
			mongo = new Mongo(MONDO_DB_HOST, MONDO_DB_PORT);
		} catch (UnknownHostException e) {
			LOG.error("not able to connect mongo db "+e);
		}
		db = mongo.getDB(MONDO_DB);
	
	}

	@Override
	public void execute(Tuple input) {
		Map<String,List<String>> recordToSave =  (Map<String, List<String>>) input.getValueByField("recordToPersist");
		for (Entry<String, List<String>> entry : recordToSave.entrySet()) {
			//System.out.println(" row  "+entry.getValue());
		 }
		System.out.println("recordToSave      "+recordToSave);
		LOG.info(" recordToSave "+recordToSave);
		System.out.println("recordToSave size "+recordToSave.size());
		DBCollection collection = db.getCollection(MONDO_DB_COLLECTION);
		collection.insert(new BasicDBObject(recordToSave));
		
	}

	
	
}
