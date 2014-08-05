package com.poc.standard.topology.local;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;

import com.poc.standard.BatchSizeFilterBolt;
import com.poc.standard.LogGenerationSpout;
import com.poc.standard.PrintBolt;

public class SpoutCheck {

	private static final Logger logger = LoggerFactory
			.getLogger(SpoutCheck.class);

	public static void main(String[] args) {

		try {

			TopologyBuilder builder = new TopologyBuilder();
			// Topology configuration
			Config conf = new Config();
			conf.setNumAckers(10);
			conf.setMessageTimeoutSecs(300);
			conf.setMaxSpoutPending(100);
			conf.setDebug(false);
			conf.setMaxTaskParallelism(3);
			builder.setSpout("LogGenerationSpout", new LogGenerationSpout(10), 1);

			builder.setBolt("PrintBolt", new PrintBolt(),
					1).setNumTasks(1).shuffleGrouping("LogGenerationSpout");
			
			
			if (args != null && args.length > 0) {
				// Submit topology
				StormSubmitter.submitTopology(args[0], conf,
						builder.createTopology());

			} else {
				LocalCluster cluster = new LocalCluster();
				cluster.submitTopology("spoutTestTpology", conf,
						builder.createTopology());
			}
		} catch (AlreadyAliveException | InvalidTopologyException e) {
			logger.error("RequestException", e);
		}
	}

}
