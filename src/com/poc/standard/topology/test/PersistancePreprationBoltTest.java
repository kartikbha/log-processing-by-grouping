package com.poc.standard.topology.test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class PersistancePreprationBoltTest {

	public static void main(String args[]) {

		
		 /*
		 mapForAnalysis {
				 1=[NC, pub2, www.qwe.com, 0.002, 2014-08-04 09:46:02 PM], 
				 5=[OH, pub2, www.qwe.com, 0.002, 2014-08-04 09:46:03 PM], 
				 3=[NC, pub2, www.qwe.com, 0.002, 2014-08-04 09:46:02 PM], 
				 4=[NC, pub2, www.a12.com, 0.002, 2014-08-04 09:46:02 PM], 
				 2=[NC, pub2, www.qwe.com, 0.002, 2014-08-04 09:46:02 PM]}
			 */
		
		/**
		 * 
		 * filteredMap {NY~pub1~2014-08-05 03:59impression=[[22]], 
		 * 
		 * NY~pub1~2014-08-05 03:59=[[NY, pub1, www.abc.com, 0.002, 2014-08-05 03:59:43 PM], 
		 * [NY, pub1, www.a12.com, 0.001, 2014-08-05 03:59:44 PM], [NY, pub1, www.abc.com, 0.001, 2014-08-05 03:59:45 PM], 
		 * [NY, pub1, www.qwe.com, 0.001, 2014-08-05 03:59:46 PM], [NY, pub1, www.abc.com, 0.006, 2014-08-05 03:59:56 PM], 
		 * [NY, pub1, www.xyz.com, 0.005, 2014-08-05 03:59:57 PM], [NY, pub1, www.abc.com, 0.001, 2014-08-05 03:59:58 PM],
		 *  [NY, pub1, www.abc.com, 0.001, 2014-08-05 03:59:59 PM]]}
		 *  
		 *  
		 * 
		 */
		ConcurrentMap<String, List<String>> totalMap  = new ConcurrentHashMap<String, List<String>>();
		//1
		List<String> attr = new ArrayList<String>(Arrays.asList("NC", "pub2", "www.qwe.com", "0.002", "2014-08-04 09:46:02 PM"));
		totalMap.put("1",attr);
		// 2 
		attr = new ArrayList<String>(Arrays.asList("NC", "pub2", "www.qwe.com", "0.002", "2014-08-04 09:46:02 PM"));
		totalMap.put("2",attr);
		
		// 3 
		attr = new ArrayList<String>(Arrays.asList("NC", "pub2", "www.qwe.com", "0.002", "2014-08-04 09:46:02 PM"));
		totalMap.put("3",attr);
		
		// 4 
		attr = new ArrayList<String>(Arrays.asList("NC", "pub2", "www.a12.com", "0.002", "2014-08-04 09:46:02 PM"));
		totalMap.put("4",attr);
		
		// 5 
		attr = new ArrayList<String>(Arrays.asList("OH", "pub2", "www.qwe.com", "0.002", "2014-08-04 09:46:03 PM"));
		totalMap.put("5",attr);
		
	
		System.out.println("totalMap..."+totalMap);
		
		
   /*

		 aggregateMap {
		 
		 OHpub22014-08-04 09:46={OHpub22014-08-04 09:46=[[5], [1]]}, 
				 
		 NCpub22014-08-04 09:46={NCpub22014-08-04 09:46=[[1], [4]]}
		 
		 }

		*/
		// row 1
		List<Integer> indexes = new ArrayList<Integer>();
		indexes.add(5);
		
		List<Integer> totalImpression = new ArrayList<Integer>();
		totalImpression.add(1);
		
		List<List<Integer>> holder = new ArrayList<List<Integer>>();
		holder.add(indexes);
		holder.add(totalImpression);
		
		Map<String,List<List<Integer>>> keyValue = new HashMap<String,List<List<Integer>>>();
		keyValue.put("OHpub22014-08-04 09:46",holder);
		
	
		// row 2
		//  NCpub22014-08-04 09:46={NCpub22014-08-04 09:46=[[1], [4]]}
		List<Integer> indexes1 = new ArrayList<Integer>();
		indexes1.add(1);
		
		List<Integer> totalImpression1 = new ArrayList<Integer>();
		totalImpression1.add(4);
		
		List<List<Integer>> holder1 = new ArrayList<List<Integer>>();
		holder1.add(indexes1);
		holder1.add(totalImpression1);
		
		Map<String,List<List<Integer>>> keyValue1 = new HashMap<String,List<List<Integer>>>();
		keyValue1.put("NCpub22014-08-04 09:46",holder1);
	
		Map<String,Map<String,List<List<Integer>>>> all = new HashMap<String,Map<String,List<List<Integer>>>>();
		all.put("OHpub22014-08-04 09:46",keyValue);
		all.put("NCpub22014-08-04 09:46",keyValue1);
		
		System.out.println("all..."+all);
		
		
		PersistancePreprationBolt persistanceBolt = new PersistancePreprationBolt();
		persistanceBolt.execute(all,totalMap);
	}
}
