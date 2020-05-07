package com.sample.cassandra;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class Consumer {

    private final Logger logger = LoggerFactory.getLogger(Consumer.class);

    @KafkaListener(topics = "jsonTopic")
    public void consumeJson(String data) throws IOException {
        logger.info("Received response = " + data);
    }
    
    @KafkaListener(topics = "xmlTopic")
    public void consumeXml(String data) throws IOException {
        logger.info("Received response = " + data);
    }
    
    @KafkaListener(topics = "movieTopic")
    public void consumeMovies(String movieName) throws IOException {
    	logger.info("Received response = " + movieName);
    }
    
    @KafkaListener(topics = "directorTopic")
    public void consumeDir(String dirName) throws IOException {
    	logger.info("Received response = " + dirName);
    }
}
