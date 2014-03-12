package com.rlilly.twitter.storm.twitterstream;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.log4j.Logger;

import twitter4j.Status;
import twitter4j.StatusListener;

import com.google.common.collect.Lists;
import com.rlilly.twitter.storm.config.BaseConfig;
import com.rlilly.twitter.storm.twitterstream.Listener;
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
import com.twitter.hbc.twitter4j.v3.Twitter4jStatusClient;

public class StreamingClient implements Runnable {
	
	private static Logger _logger = Logger.getLogger(StreamingClient.class);
	
	private LinkedBlockingQueue<String> _msgQueue;
	private LinkedBlockingQueue<Event> _eventQueue;
	private LinkedBlockingQueue<Status> _tweetQueue;

	public StreamingClient(LinkedBlockingQueue<String> msgQueue, LinkedBlockingQueue<Event> eventQueue, LinkedBlockingQueue<Status> tweetQueue) {
		this._msgQueue = msgQueue;
		this._eventQueue = eventQueue;
		this._tweetQueue = tweetQueue;
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
        
        StatusListener listener = new Listener(_tweetQueue);
        
        int numThreads = 4;
        ExecutorService service = Executors.newFixedThreadPool(numThreads);
        
        Twitter4jStatusClient t4jClient = new Twitter4jStatusClient(
        		hosebirdClient, this._msgQueue, Lists.newArrayList(listener), service);
        
        t4jClient.connect();
        for (int threads = 0; threads < numThreads; threads++) {
        	t4jClient.process();
        }
        
        _logger.info("StreamingClient Connected");
	}
}
