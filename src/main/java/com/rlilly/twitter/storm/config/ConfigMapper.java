package com.rlilly.twitter.storm.config;

import java.io.File;
import java.io.IOException;

import org.apache.log4j.Logger;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class ConfigMapper {

	private static Logger _logger = Logger.getLogger(ConfigMapper.class);
	
	public ConfigMapper() {
		try {
			final ObjectMapper mapper = new ObjectMapper();
			final StormConfig stormConfig = mapper.readValue(new File(Base.FILE_CONFIG), StormConfig.class);
			
			//Twitter
			BaseConfig.apiKey = stormConfig.twitter.apiKey;
			BaseConfig.apiSecret = stormConfig.twitter.apiSecret;
			BaseConfig.accessToken = stormConfig.twitter.accessToken;
			BaseConfig.accessTokenSecret = stormConfig.twitter.accessTokenSecret;
			
			_logger.info("Success");
			
		} catch (JsonParseException e) {
			_logger.error(e);
			_logger.fatal("Malformed configuration file: " + Base.FILE_CONFIG);
			System.exit(1);
		} catch (JsonMappingException e) {
			_logger.error(e);
		} catch (IOException e) {
			_logger.error(e);
		}
	}
	
}
