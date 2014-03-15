package com.rlilly.twitter.storm;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

import twitter4j.Status;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.Utils;

import com.rlilly.twitter.storm.bolts.MessageBolt;
import com.rlilly.twitter.storm.bolts.NamedEntityBolt;
import com.rlilly.twitter.storm.config.ConfigMapper;
import com.rlilly.twitter.storm.spouts.TweetSpout;
import com.rlilly.twitter.storm.stats.Stats;
import com.rlilly.twitter.storm.twitterstream.StreamingClient;
import com.twitter.hbc.core.event.Event;

public class Storm 
{
private static Logger _logger = Logger.getLogger(Storm.class);
	
	private LinkedBlockingQueue<String> _msgQueue;
	private LinkedBlockingQueue<Event> _eventQueue;
	private LinkedBlockingQueue<Status> _tweetQueue;
	
	private ExecutorService _executorService;
	
	private TopologyBuilder _builder;
	
	private LocalCluster cluster;
	
	public Storm() {
		new ConfigMapper();
		
		this.initQueues();
		
		this.initThreads();
		
		this.defineTopology();
		
		this.initCluster();
		
		this.initStats();
	}
	
	private void initQueues() {
		_logger.info("Initializing Queues");
		
		this._msgQueue = new LinkedBlockingQueue<String>(100000);
        this._eventQueue = new LinkedBlockingQueue<Event>(1000);
        this._tweetQueue = new LinkedBlockingQueue<Status>(100000);
	}
	
	private void initThreads() {
		_logger.info("Starting Threads");
		
		this._executorService = Executors.newFixedThreadPool(1);
		
		this._executorService.execute(new StreamingClient(this._msgQueue, this._eventQueue, this._tweetQueue));
	}
	
	private void defineTopology() {
		_logger.info("Defining Topology");
		_builder = new TopologyBuilder();
		_builder.setSpout("tweets", new TweetSpout(this._tweetQueue), 1);
		_builder.setBolt("message", new MessageBolt(), 1)
			.shuffleGrouping("tweets");
		//_builder.setBolt("namedentity", new NamedEntityBolt(), 5)
		//	.shuffleGrouping("message");
	}
	
	private void initCluster() {
		_logger.info("Initializing Cluster");
		
		Config conf = new Config();
		conf.setDebug(true);
		conf.setMaxSpoutPending(500);
		conf.setNumWorkers(10);
		
		this.cluster = new LocalCluster();
		cluster.submitTopology("test", conf, _builder.createTopology());
		/*Utils.sleep(60000);
		cluster.killTopology("test");
		cluster.shutdown();
		
		_logger.info("Shutting down");
		System.exit(0);*/
	}
	
	private void initStats() {
		ScheduledThreadPoolExecutor stats = new ScheduledThreadPoolExecutor(1);
		stats.scheduleAtFixedRate(new Stats(this._tweetQueue, this._msgQueue, this._eventQueue, this.cluster), 60000, 60000, TimeUnit.MILLISECONDS);
	}
	
    public static void main( String[] args )
    {
        System.out.println( "YOLO!" );
        
        new Storm();
    }
}
