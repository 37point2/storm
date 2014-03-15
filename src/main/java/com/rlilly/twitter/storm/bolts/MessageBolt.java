package com.rlilly.twitter.storm.bolts;

import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.IBasicBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class MessageBolt implements IBasicBolt{

	private static final long serialVersionUID = -2245986100070368445L;
	OutputCollector _collector;
	
	@Override
	public void cleanup() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("message"));
		
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

	@Override
	public void execute(Tuple tuple, BasicOutputCollector collector) {
		collector.emit(new Values(tuple.getString(0)));
		
	}

	@Override
	public void prepare(Map conf, TopologyContext context) {
		// TODO Auto-generated method stub
		
	}

}
