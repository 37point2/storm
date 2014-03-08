package com.rlilly.twitter.storm.spouts;

import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class TweetSpout implements IRichSpout {
	
	LinkedBlockingQueue<String> _msgQueue;
	SpoutOutputCollector _collector;
	
	public TweetSpout(LinkedBlockingQueue<String> msgQueue) {
		_msgQueue = msgQueue;
	}
	
	@Override
	public void ack(Object arg0) {
		// TODO Auto-generated method stub
		
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
	public void fail(Object arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void nextTuple() {
		try {
			String msg = _msgQueue.take();
			_collector.emit(new Values(msg));
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		
	}

	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		_collector = collector;
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("tweet"));
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

}
