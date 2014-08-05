package com.poc.standard.topology.local;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.poc.standard.AnalysisLogicBolt;
import com.poc.standard.BatchSizeFilterBolt;
import com.poc.standard.LogGenerationSpout;
import com.poc.standard.PersistanceBolt;
import com.poc.standard.PersistancePreprationBolt;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

public class ChimperLogAnalysisTopology {

	private static final Logger logger = LoggerFactory
			.getLogger(ChimperLogAnalysisTopology.class);

	public static void main(String[] args) {

		try {

			TopologyBuilder builder = new TopologyBuilder();
			// Topology configuration
			Config conf = new Config();
			conf.setNumAckers(10);
			conf.setMessageTimeoutSecs(300);
			conf.setMaxSpoutPending(100);
			conf.setDebug(false);
			conf.setMaxTaskParallelism(10);
			
			int batchSize = 10;
			builder.setSpout("LogGenerationSpout", new LogGenerationSpout(batchSize), 1);

			builder.setBolt("BatchSizeFilterBolt", new BatchSizeFilterBolt(batchSize),
					2).setNumTasks(2).fieldsGrouping("LogGenerationSpout",new Fields("dateUpToMinute"));

			builder.setBolt("PersistancePrepreationBolt", new PersistancePreprationBolt(), 1)
				.setNumTasks(1).shuffleGrouping("BatchSizeFilterBolt");
			
			builder.setBolt("PersistanceBolt", new PersistanceBolt(), 1)
			.setNumTasks(1).shuffleGrouping("PersistancePrepreationBolt");
		
			
			if (args != null && args.length > 0) {
				// Submit topology
				StormSubmitter.submitTopology("logProcessing", conf,
						builder.createTopology());

			} else {
				LocalCluster cluster = new LocalCluster();
				cluster.submitTopology("logProcessing", conf,
						builder.createTopology());
			}
		} catch (AlreadyAliveException | InvalidTopologyException e) {
			logger.error("RequestException", e);
		}
	}

}
