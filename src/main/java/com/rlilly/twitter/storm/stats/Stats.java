package com.rlilly.twitter.storm.stats;

import java.util.concurrent.LinkedBlockingQueue;

import org.apache.log4j.Logger;

import twitter4j.Status;
import backtype.storm.LocalCluster;

import com.twitter.hbc.core.event.Event;

public class Stats implements Runnable {

	Logger _logger = Logger.getLogger(Stats.class);
	
	LinkedBlockingQueue<Status> _tweetQueue;
	LinkedBlockingQueue<String> _msgQueue;
	LinkedBlockingQueue<Event> _eventQueue;
	
	LocalCluster _cluster;
	
	public Stats(LinkedBlockingQueue<Status> tweetQueue, LinkedBlockingQueue<String> msgQueue, LinkedBlockingQueue<Event> eventQueue, LocalCluster cluster) {
		_tweetQueue = tweetQueue;
		_msgQueue = msgQueue;
		_eventQueue = eventQueue;
		_cluster = cluster;
	}
	
	private void printStats() {
		String msg = "Stats: ";
		if (this._tweetQueue.size() > 0) msg += "Tweet Queue: " + this._tweetQueue.size() + ", ";
		if (this._msgQueue.size() > 0) msg += "Message Queue: " + this._msgQueue.size() + ", ";
		if (this._eventQueue.size() > 0) msg += "Event Queue: " + this._eventQueue.size() + ", ";
		
		_logger.info(msg);
	}
	
	@Override
	public void run() {
		printStats();
		
	}

}
