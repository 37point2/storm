package com.rlilly.twitter.storm.spouts;

import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

import twitter4j.Status;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class TweetSpout implements IRichSpout {
	
	LinkedBlockingQueue<Status> _tweetQueue;
	SpoutOutputCollector _collector;
	
	public TweetSpout(LinkedBlockingQueue<Status> tweetQueue) {
		_tweetQueue = tweetQueue;
	}
	
	@Override
	public void ack(Object msgId) {
		System.out.println("Acked " + msgId);
	}

	@Override
	public void activate() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void close() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void deactivate() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void fail(Object msgId) {
		System.out.println("Failed " + msgId);
	}

	@Override
	public void nextTuple() {
		try {
			Status status = _tweetQueue.take();
			Object msgId = status.getId();
			_collector.emit(new Values(status.getText(), status.getUser().getScreenName()), msgId);
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		}
		
	}

	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		_collector = collector;
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("tweet", "user"));
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

}
