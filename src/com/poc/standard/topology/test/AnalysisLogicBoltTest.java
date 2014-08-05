package com.poc.standard.topology.test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

public class AnalysisLogicBoltTest {

	public static void main(String args[]) {

		/*
		 * 1=[ NY, pub5, www.a12.com, 0.001, 2014-08-03 04:54:22 PM], 7=[NY,
		 * pub1, www.abc.com, 0.001, 2014-08-03 04:54:24 PM], 8=[NY, pub1,
		 * www.abc.com, 0.005, 2014-08-03 04:54:24 PM], 5=[NY, pub5,
		 * www.abc.com, 0.005, 2014-08-03 04:54:23 PM], 6=[NY, pub5,
		 * www.abc.com, 0.001, 2014-08-03 04:54:24 PM], 3=[WA, pub1,
		 * www.a12.com, 0.001, 2014-08-03 04:54:23 PM], 10=[NY, pub1,
		 * www.a12.com, 0.007, 2014-08-03 04:54:25 PM], 4=[WA, pub1,
		 * www.abc.com, 0.005, 2014-08-03 04:54:23 PM], 9=[WA, pub1,
		 * www.abc.com, 0.001, 2014-08-03 04:54:25 PM], 2=[NC, pub3,
		 * www.a12.com, 0.005, 2014-08-03 04:54:23 PM]}
		 */

		ConcurrentHashMap<String, List<String>> mapForAnalysis = new ConcurrentHashMap<String, List<String>>();

		List<String> attr = new ArrayList<String>(Arrays.asList("NY", "pub5",
				"www.a12.com", "0.001", "2014-08-03 04:54:22 PM"));
		mapForAnalysis.put("1", attr);

		attr = new ArrayList<String>(Arrays.asList("NC", "pub3", "www.a12.com",
				"0.005", "2014-08-03 04:54:23 PM"));
		mapForAnalysis.put("2", attr);

		attr = new ArrayList<String>(Arrays.asList("WA", "pub1", "www.a12.com",
				"0.001", "2014-08-03 04:54:23 PM"));
		mapForAnalysis.put("3", attr);

		attr = new ArrayList<String>(Arrays.asList("WA", "pub1", "www.abc.com",
				"0.005", "2014-08-03 04:54:23 PM"));
		mapForAnalysis.put("4", attr);

		attr = new ArrayList<String>(Arrays.asList("NY", "pub5", "www.abc.com",
				"0.005", "2014-08-03 04:54:23 PM"));
		mapForAnalysis.put("5", attr);

		attr = new ArrayList<String>(Arrays.asList("NY", "pub5", "www.abc.com",
				"0.001", "2014-08-03 04:54:24 PM"));
		mapForAnalysis.put("6", attr);

		attr = new ArrayList<String>(Arrays.asList("NY", "pub5", "www.a12.com",
				"0.001", "2014-08-03 04:54:22 PM"));
		mapForAnalysis.put("7", attr);

		attr = new ArrayList<String>(Arrays.asList("NY", "pub5", "www.a12.com",
				"0.001", "2014-08-03 04:54:22 PM"));
		mapForAnalysis.put("8", attr);

		attr = new ArrayList<String>(Arrays.asList("NY", "pub5", "www.a12.com",
				"0.001", "2014-08-03 04:54:22 PM"));
		mapForAnalysis.put("9", attr);

		attr = new ArrayList<String>(Arrays.asList("NY", "pub5", "www.a12.com",
				"0.001", "2014-08-03 04:54:22 PM"));
		mapForAnalysis.put("10", attr);

		System.out.println("mapForAnalysis1 size  " + mapForAnalysis.size());

		AnalysisLogicBolt call = new AnalysisLogicBolt();
		call.execute(mapForAnalysis);


		// ////////////////////

		ConcurrentHashMap<String, List<String>> mapForAnalysis1 = new ConcurrentHashMap<String, List<String>>();

		/**
		List<String> attr1 = new ArrayList<String>(Arrays.asList("NY", "pub5",
				"www.a12.com", "0.001", "2014-08-03 04:54:22 PM"));
		mapForAnalysis1.put("1", attr1);

	    attr1 = new ArrayList<String>(Arrays.asList("NY", "pub5",
				"www.a12.com", "0.002", "2014-08-03 04:54:22 PM"));
		mapForAnalysis1.put("2", attr1);

	
		attr1 = new ArrayList<String>(Arrays.asList("NY", "pub5",
				"www.a12.com", "0.003", "2014-08-03 04:54:21 PM"));
		mapForAnalysis1.put("3", attr1);


		attr1 = new ArrayList<String>(Arrays.asList("NY", "pub5",
				"www.a12.com", "0.003", "2014-08-03 04:54:21 PM"));
		mapForAnalysis1.put("4", attr1);
		
	
		attr1 = new ArrayList<String>(Arrays.asList("NY", "pub3",
				"www.a12.com", "0.001", "2014-08-03 04:56:23 PM"));
		mapForAnalysis1.put("5", attr1);

		
		attr1 = new ArrayList<String>(Arrays.asList("NY", "pub5",
				"www.a12.com", "0.001", "2014-08-03 04:55:21 PM"));
		mapForAnalysis1.put("6", attr1);

     	attr1 = new ArrayList<String>(Arrays.asList("NY", "pub5",
				"www.a12.com", "0.001", "2014-08-03 04:54:22 PM"));
		mapForAnalysis1.put("7", attr1);

	    attr1 = new ArrayList<String>(Arrays.asList("WA", "pub1", "www.abc.com",
				"0.005", "2014-08-03 04:54:23 PM"));
		mapForAnalysis1.put("8", attr1);
		
		**/
		System.out.println("mapForAnalysis1 size  " + mapForAnalysis1.size());

		//AnalysisLogicBolt call = new AnalysisLogicBolt();
		//call.execute(mapForAnalysis1);

	}
}
