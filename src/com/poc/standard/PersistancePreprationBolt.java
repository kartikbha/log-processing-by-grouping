package com.poc.standard;

import java.text.ParseException;
import java.text.SimpleDateFormat;
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

		System.out.println(" filteredMap... " + filteredMap);
		Map<String, String> totalRecord = new HashMap<String, String>();
		float avgBid = 0;
		String date = "";
		String pub = "";
		String geo = "";
		for (Entry<String, List<List<String>>> entry : filteredMap.entrySet()) {
			String key = entry.getKey();
			if (!key.endsWith("impression")) {
				List<List<String>> logRows = entry.getValue();
				for (List<String> logRow : logRows) {
					avgBid = avgBid + Float.parseFloat(logRow.get(3));
					date = logRow.get(4);
					pub = logRow.get(1);
					geo = logRow.get(0);

				}
				// date,publisher,geo,imps,uniques,avgBids
				// date 1
				totalRecord.put("date", getDateUptoMinute(date));
				// pub 2
				totalRecord.put("publisher", pub);
				// geo 3
				totalRecord.put("geo", geo);
				avgBid = avgBid / (float) logRows.size();
				// avg bid 4
				totalRecord.put("avgBids", new Float(avgBid).toString());
				// uniques 6
				totalRecord.put("uniques",
						new Integer(logRows.size()).toString());

			} else {
				// if (!key.endsWith("impression"))
				List<List<String>> logRows = entry.getValue();
				for (List<String> logRow : logRows) {
					totalRecord.put("imps", logRow.get(0));
				}
			}
		}

		collector.emit(new Values(totalRecord));
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
