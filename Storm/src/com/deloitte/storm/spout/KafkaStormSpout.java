package com.deloitte.storm.spout;

import java.util.Map;

import com.google.common.collect.ImmutableList;

import storm.kafka.BrokerHosts;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;

import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;

public class KafkaStormSpout extends BaseRichSpout {
	  SpoutOutputCollector _collector;
	  private static int STORM_KAFKA_FROM_READ_FROM_START = -2;
	    private static int STORM_KAFKA_FROM_READ_FROM_CURRENT_OFFSET = -1;
	  private static int readFromMode = STORM_KAFKA_FROM_READ_FROM_START;
	  private static String TOPIC_NAME = "test";


	@Override
	public void nextTuple() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void open(Map arg0, TopologyContext arg1, SpoutOutputCollector arg2) {
		// TODO Auto-generated method stub
		
		//
		
		BrokerHosts brokerHosts = new ZkHosts("localhost:2181");
		   SpoutConfig kafkaConfig = new SpoutConfig(brokerHosts, TOPIC_NAME, "", "storm");
	       
	        kafkaConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
		
		KafkaSpout kafkaSpout = new KafkaSpout(kafkaConfig);
		
		

	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer arg0) {
		// TODO Auto-generated method stub
		
	}

}
