package com.poc.standard;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class PersistancePreprationBolt extends BaseBasicBolt {

	private static final Logger LOG = LoggerFactory
			.getLogger(PersistancePreprationBolt.class);

	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {

		Map<String, List<List<String>>> filteredMap = (Map<String, List<List<String>>>) input
				.getValueByField("batch");

		
		System.out.println(" filteredMap ......filteredMap "+filteredMap);
		
		Map<String, List<String>> totalRecords = new HashMap<String, List<String>>();
		float avgBid = 0;
		String date = "";
		String pub = "";
		String geo = "";
		int keyIndex = 0;

		for (Entry<String, List<List<String>>> entry : filteredMap.entrySet()) {

			List<String> rowTOSaveNoSQL = new ArrayList<String>();

			String key = entry.getKey();
			System.out.println(" key .........."+key);
			
			
			if (!key.endsWith("impression")) {

				List<List<String>> logRows = entry.getValue();
				for (List<String> logRow : logRows) {

					avgBid = avgBid + Float.parseFloat(logRow.get(3));
					date = logRow.get(4);
					pub = logRow.get(1);
					geo = logRow.get(0);

				}

				// date 1
				rowTOSaveNoSQL.add(getDateUptoMinute(date));
				// pub 2
				rowTOSaveNoSQL.add(pub);
				// geo 3
				rowTOSaveNoSQL.add(geo);
				// System.out.println(" before doing avg avgBid  "+avgBid+" freq "+(float)freq.get(0).intValue());
				avgBid = avgBid / (float) logRows.size();
				// System.out.println(" avgBid  "+avgBid);
				// avg bid 4
				rowTOSaveNoSQL.add(Float.toString(avgBid));
				// total impression 5
				// rowTOSaveNoSQL.add(Integer.toString(totalImpressions.get(0)));
				// uniques 6
				rowTOSaveNoSQL.add(Integer.toString(logRows.size()));
				

			} else {
				// if (!key.endsWith("impression"))
				List<List<String>> logRows = entry.getValue();
				for (List<String> logRow : logRows) {
					rowTOSaveNoSQL.add(logRow.get(0));
					
				}

			}
			keyIndex++;
			totalRecords.put(new Integer(keyIndex).toString(),
					rowTOSaveNoSQL);
		}

		System.out.println(" totalRecords  " + totalRecords);
		collector.emit(new Values(totalRecords));
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

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("recordToPersist"));
	}

}
