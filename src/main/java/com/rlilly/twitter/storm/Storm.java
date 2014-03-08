package com.rlilly.twitter.storm;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.log4j.Logger;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.Utils;

import com.rlilly.twitter.storm.bolts.MessageBolt;
import com.rlilly.twitter.storm.bolts.NamedEntityBolt;
import com.rlilly.twitter.storm.config.ConfigMapper;
import com.rlilly.twitter.storm.spouts.TweetSpout;
import com.rlilly.twitter.storm.twitterstream.Processor;
import com.rlilly.twitter.storm.twitterstream.StreamingClient;
import com.twitter.hbc.core.event.Event;

public class Storm 
{
private static Logger _logger = Logger.getLogger(Storm.class);
	
	private LinkedBlockingQueue<String> _msgQueue;
	private LinkedBlockingQueue<Event> _eventQueue;
	
	private ExecutorService _executorService;
	
	private TopologyBuilder _builder;
	
	public Storm() {
		new ConfigMapper();
		
		this.initQueues();
		
		this.initThreads();
		
		this.defineTopology();
		
		this.initCluster();
	}
	
	private void initQueues() {
		_logger.info("Initializing Queues");
		
		this._msgQueue = new LinkedBlockingQueue<String>(100000);
        this._eventQueue = new LinkedBlockingQueue<Event>(1000);
	}
	
	private void initThreads() {
		_logger.info("Starting Threads");
		
		this._executorService = Executors.newFixedThreadPool(1);
		
		this._executorService.execute(new StreamingClient(this._msgQueue, this._eventQueue));
		
		//this._executorService.execute(new Processor(this._msgQueue));
	}
	
	private void defineTopology() {
		_logger.info("Defining Topology");
		_builder = new TopologyBuilder();
		_builder.setSpout("tweets", new TweetSpout(this._msgQueue), 1);
		_builder.setBolt("message", new MessageBolt(), 3)
			.shuffleGrouping("tweets");
		_builder.setBolt("namedentity", new NamedEntityBolt(), 5)
			.shuffleGrouping("message");
	}
	
	private void initCluster() {
		_logger.info("Initializing Cluster");
		
		Config conf = new Config();
		//conf.setDebug(true);
		conf.setNumWorkers(10);
		
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("test", conf, _builder.createTopology());
		Utils.sleep(60000);
		cluster.killTopology("test");
		cluster.shutdown();
		
		_logger.info("Shutting down");
		System.exit(0);
	}
	
    public static void main( String[] args )
    {
        System.out.println( "YOLO!" );
        
        new Storm();
    }
}
