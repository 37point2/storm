package com.rlilly.twitter.storm.twitterstream;

import java.util.concurrent.LinkedBlockingQueue;

import org.apache.log4j.Logger;

public class Processor implements Runnable{

private static Logger _logger = Logger.getLogger(Processor.class);
	
	private LinkedBlockingQueue<String> _msgQueue;
	
	public Processor(LinkedBlockingQueue<String> _msgQueue) {
		this._msgQueue = _msgQueue;
	}
	
	public void run() {
		while(true) {
			try {
				String msg = this._msgQueue.take();
				_logger.info(msg);
			} catch (InterruptedException e) {
				_logger.error(e);
			}
		}
	}
}
