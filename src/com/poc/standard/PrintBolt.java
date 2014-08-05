package com.poc.standard;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

public class PrintBolt extends BaseBasicBolt {

	ConcurrentMap<Integer, List<String>> concurrentMap = new ConcurrentHashMap<Integer, List<String>>();

	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		// TODO Auto-generated method stub

		System.out.println(" input... " + input);

		System.out.println(" 0 " + input.getString(0));
		System.out.println(" 1 " + input.getString(1));
		System.out.println(" 2 " + input.getString(2));
		System.out.println(" 3 " + input.getString(3));
		System.out.println(" 4 " + input.getString(4));

	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub

		declarer.declare(new Fields("batch"));

	}

}
