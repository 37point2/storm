package com.rlilly.twitter.storm.bolts;

import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.IBasicBolt;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

public class NamedEntityBolt implements IBasicBolt {
	

	private static final long serialVersionUID = 1L;

	@Override
	public void cleanup() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void execute(Tuple tuple, BasicOutputCollector collector) {
		System.out.println(tuple.getString(0));
	}

	@Override
	public void prepare(Map conf, TopologyContext context) {
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

}
