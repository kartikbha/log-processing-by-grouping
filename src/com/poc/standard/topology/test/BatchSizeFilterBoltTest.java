package com.poc.standard.topology.test;

import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class BatchSizeFilterBoltTest {

	public static void main(String[] args) {
		// TODO Auto-generated method stub

		//[4, WA, pub1, adv10, www.a12.com, 0.001, 1214, 2014-08-05 11:09:18 AM]
		
		
		BatchSizeFilterBolt batchSizeFilter = new BatchSizeFilterBolt();
		
		Values val = new Values();
		val.add(1);
		val.add("WA");
		val.add("pub1");
		val.add("adv10");
		val.add("www.a12.com");
		val.add("0.001");
		val.add("1214");
		val.add("2014-08-05 11:09:18 AM");
		
		batchSizeFilter.execute(val);
		
		Values val1 = new Values();
		val1.add(1);
		val1.add("WA");
		val1.add("pub1");
		val1.add("adv10");
		val1.add("www.a12.com");
		val1.add("0.001");
		val1.add("1214");
		val1.add("2014-08-05 11:09:19 AM");
		
		batchSizeFilter.execute(val1);
		
	}

}
