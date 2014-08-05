package com.poc.standard;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Map;
import java.util.Random;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class LogGenerationSpout extends BaseRichSpout {

	private static final Logger LOG = LoggerFactory
			.getLogger(LogGenerationSpout.class);
	
	private SpoutOutputCollector collector;
	private int batchSize;
	public LogGenerationSpout()  {
		this(5);
	}

	public LogGenerationSpout(int batchSize)  {
		this.batchSize = batchSize;
	}
	public final static String[] GEO = { "NY", "NC", "OH", "PA", "WA", "MA",
		"KY", "IL" };
	
	//public final static String[] GEO = { "NY", "NY", "NY", "NY", "NY", "NY",
		//"NY", "NY" };
	public final static String[] PUBLISHER = { "pub1", "pub2", "pub3", "pub4",
		"pub5" };
	
	//public final static String[] PUBLISHER = { "pub1", "pub1", "pub1", "pub1",
	//"pub1" };
	
    public final static String[] ADVERTISER = { "adv10", "adv20", "adv30",
			"adv40", "adv50" };
    public final static String[] WEBSITE = { "www.abc.com", "www.qwe.com",
			"www.xyz.com", "www.123.com", "www.a12.com" };
    public final static String[] BID = { "0.001", "0.002", "0.006", "0.007",
			"0.005" };
    public final static String[] COOKIE = { "1214", "3453", "3432", "1256",
			"1267" };
    private double[] activityDistribution;
	private Random randomGenerator;
    private long logId = 0;

	@Override
	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		this.collector = collector;
		this.randomGenerator = new Random();
		// will be on website.
		this.activityDistribution = getProbabilityDistribution(WEBSITE.length,
				randomGenerator);
	}

	@Override
	public void nextTuple() {
	    for(int i = 0; i <= batchSize; i++) {
	    	 Values value = getNextLogs();
	         //LOG.info(" dispatched batch... "+value);
	    	
			this.collector.emit(value);
		}
	    //LOG.info(" dispatched batch... ");
		//System.out.println("dispatched batch...");
	    try {
			Thread.sleep(6000);
		} catch (InterruptedException e) {
		}
	    
	}

	private Values getNextLogs()  {
		
		try {
			Thread.sleep(randInt(1, 10) * 20);
		} catch (InterruptedException e) {
		}
		Values val = new Values();
		val.add(++logId + "");
		String geo = GEO[randomIndex(activityDistribution, randomGenerator)];
		val.add(geo);
		String pub = PUBLISHER[randomIndex(activityDistribution, randomGenerator)];
		val.add(pub);
		val.add(ADVERTISER[randomIndex(activityDistribution, randomGenerator)]);
		val.add(WEBSITE[randomIndex(activityDistribution, randomGenerator)]);
		val.add(BID[randomIndex(activityDistribution, randomGenerator)]);
		val.add(COOKIE[randomIndex(activityDistribution, randomGenerator)]);
		String fullTime = DATE_FORMAT.format(System.currentTimeMillis());
		
		val.add(fullTime);
		val.add(geo+pub+getDateUptoMinute(fullTime));
		
		return val;
	}
	
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("id", "geo", "pub", "adv", "website",
				"bid", "cookie", "date","dateUpToMinute"));
	}

	// --- Helper methods --- //
	// SimpleDateFormat is not thread safe!
	private SimpleDateFormat DATE_FORMAT = new SimpleDateFormat(
			"yyyy-MM-dd hh:mm:ss aa");

	public static int randInt(int min, int max) {
    	Random rand = new Random();
		int randomNum = rand.nextInt((max - min) + 1) + min;
		return randomNum;
	}

	private static double[] getProbabilityDistribution(int n,
			Random randomGenerator) {
		double a[] = new double[n];
		double s = 0.0d;
		for (int i = 0; i < n; i++) {
			a[i] = 1.0d - randomGenerator.nextDouble();
			a[i] = -1 * Math.log(a[i]);
			s += a[i];
		}
		for (int i = 0; i < n; i++) {
			a[i] /= s;
		}
		return a;
	}

	private static int randomIndex(double[] distribution, Random randomGenerator) {
		double rnd = randomGenerator.nextDouble();
		double accum = 0;
		int index = 0;
		for (; index < distribution.length && accum < rnd; index++, accum += distribution[index - 1])
			;
		return index - 1;
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


}
