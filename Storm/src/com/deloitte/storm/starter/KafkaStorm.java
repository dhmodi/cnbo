package com.deloitte.storm.starter;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.TopologyBuilder;

import com.deloitte.util.ApplicationConstants;
import com.deloitte.storm.bolt.LogValidatorBolt;
import com.deloitte.storm.bolt.RecordExtractorBolt;


public class KafkaStorm {
	public static void main(String[] args) throws InterruptedException, AlreadyAliveException, InvalidTopologyException, FileNotFoundException, IOException {
		Properties appProperties = new Properties();
		appProperties.load(new FileInputStream(ApplicationConstants.APP_RECOURCE));
		TopologyBuilder builder = new TopologyBuilder();
		ZkHosts zkHosts = new ZkHosts(appProperties.getProperty(ApplicationConstants.ZK_HOST));
		SpoutConfig spoutConfig = new SpoutConfig(zkHosts, appProperties.getProperty(ApplicationConstants.ZK_TOPIC), 
				appProperties.getProperty(ApplicationConstants.ZK_ROOT), 
				appProperties.getProperty(ApplicationConstants.ZK_ID));
		spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
		KafkaSpout spoutconf = new KafkaSpout(spoutConfig);
		spoutConfig.forceFromStart = true;
		builder.setSpout(ApplicationConstants.KAFKA_SPOUT, spoutconf);
		builder.setBolt(ApplicationConstants.LOG_VALIDATOR, new LogValidatorBolt()).shuffleGrouping(ApplicationConstants.KAFKA_SPOUT);
		builder.setBolt(ApplicationConstants.REC_EXTRACTOR, new RecordExtractorBolt()).shuffleGrouping(ApplicationConstants.LOG_VALIDATOR);
		Config config = new Config();
		config.setDebug(true);
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("KafkaStorm", config, builder.createTopology());
//		Thread.sleep(10000);
//		cluster.shutdown();
	}
}
