package com.rlilly.twitter.storm.twitterstream;

import java.util.concurrent.LinkedBlockingQueue;

import org.apache.log4j.Logger;

import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.twitter.hbc.twitter4j.v3.handler.StatusStreamHandler;
import com.twitter.hbc.twitter4j.v3.message.DisconnectMessage;
import com.twitter.hbc.twitter4j.v3.message.StallWarningMessage;

public class Listener implements StatusStreamHandler{

	Logger _logger = Logger.getLogger(Listener.class);
	ObjectMapper mapper = new ObjectMapper();
	
	LinkedBlockingQueue<Status> _tweetQueue;

	public Listener(LinkedBlockingQueue<Status> tweetQueue) {
		_tweetQueue = tweetQueue;
	}

	@Override
	public void onDeletionNotice(StatusDeletionNotice msg) {
		try {
			_logger.debug("Status delete: " + mapper.writeValueAsString(msg));
		} catch (JsonProcessingException e) {
			_logger.error(e);
		}
	}

	@Override
	public void onScrubGeo(long longitude, long latitude) {
		_logger.info(longitude + "," + latitude);
	}

	@Override
	public void onStallWarning(StallWarning msg) {
		try {
			_logger.warn(mapper.writeValueAsString(msg));
		} catch (JsonProcessingException e) {
			_logger.error(e);
		}
	}

	@Override
	public void onStatus(Status status) {
		try {
			this._tweetQueue.put(status);
		} catch (InterruptedException e) {
			_logger.error(e);
		}
	}

	@Override
	public void onTrackLimitationNotice(int num) {
		_logger.warn("Track limit: " + num);
	}

	@Override
	public void onException(Exception msg) {
		_logger.error(msg);
	}

	@Override
	public void onDisconnectMessage(DisconnectMessage msg) {
		try {
			_logger.info(mapper.writeValueAsString(msg));
		} catch (JsonProcessingException e) {
			_logger.error(e);
		}
	}

	@Override
	public void onStallWarningMessage(StallWarningMessage msg) {
		try {
			_logger.warn(mapper.writeValueAsString(msg));
		} catch (JsonProcessingException e) {
			_logger.error(e);
		}
	}

	@Override
	public void onUnknownMessageType(String msg) {
		_logger.warn("Unknown message type: " + msg);
	}
	
}
