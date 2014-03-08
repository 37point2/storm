package com.rlilly.twitter.storm.twitterstream;

import java.util.concurrent.LinkedBlockingQueue;

import org.apache.log4j.Logger;

import com.rlilly.twitter.storm.config.BaseConfig;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesSampleEndpoint;
import com.twitter.hbc.core.endpoint.StreamingEndpoint;
import com.twitter.hbc.core.event.Event;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;

public class StreamingClient implements Runnable {
	
	private static Logger _logger = Logger.getLogger(StreamingClient.class);
	
	private LinkedBlockingQueue<String> _msgQueue;
	private LinkedBlockingQueue<Event> _eventQueue;

	public StreamingClient(LinkedBlockingQueue<String> _msgQueue, LinkedBlockingQueue<Event> _eventQueue) {
		this._msgQueue = _msgQueue;
		this._eventQueue = _eventQueue;
	}
	
	public void run() {
		_logger.info("Starting StreamingClient");
		
		Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        StreamingEndpoint endpoint = new StatusesSampleEndpoint();
        
        Authentication hosebirdAuth = new OAuth1(
        		BaseConfig.apiKey,
        		BaseConfig.apiSecret,
        		BaseConfig.accessToken,
        		BaseConfig.accessTokenSecret);
        
        ClientBuilder builder = new ClientBuilder()
        	.name("Hosebird-Client-01")
        	.hosts(hosebirdHosts)
        	.authentication(hosebirdAuth)
        	.endpoint(endpoint)
        	.processor(new StringDelimitedProcessor(this._msgQueue))
        	.eventMessageQueue(this._eventQueue);
        
        Client hosebirdClient = builder.build();
        
        hosebirdClient.connect();
        
        _logger.info("StreamingClient Connected");
	}
}
