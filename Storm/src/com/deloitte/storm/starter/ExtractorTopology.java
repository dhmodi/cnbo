package com.deloitte.storm.starter;

import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.Properties;

import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.TopologyBuilder;

import com.deloitte.storm.bolt.HdfsBolt;
import com.deloitte.storm.bolt.LogValidatorBolt;
import com.deloitte.storm.bolt.RecordExtractorBolt;
import com.deloitte.storm.bolt.format.DefaultFileNameFormat;
import com.deloitte.storm.bolt.format.DelimitedRecordFormat;
import com.deloitte.storm.bolt.format.FileNameFormat;
import com.deloitte.storm.bolt.format.RecordFormat;
import com.deloitte.storm.bolt.rotation.FileRotationPolicy;
import com.deloitte.storm.bolt.rotation.FileSizeRotationPolicy;
import com.deloitte.storm.bolt.rotation.FileSizeRotationPolicy.Units;
import com.deloitte.storm.bolt.sync.CountSyncPolicy;
import com.deloitte.storm.bolt.sync.SyncPolicy;
import com.deloitte.util.ApplicationConstants;

/**
 * This topology demonstrates Storm's stream groupings and multilang
 * capabilities.
 */
public class ExtractorTopology {

	public static void main(String[] args) throws Exception {
		
		//Reading application configs from properties file
		Properties appProperties = new Properties();
		appProperties.load(new FileInputStream(ApplicationConstants.APP_RECOURCE));
		
		//Configuring KafkaSpout with a Kafka topic and instantiating it 
		ZkHosts zkHosts = new ZkHosts(appProperties.getProperty(ApplicationConstants.ZK_HOST));
		SpoutConfig spoutConfig = new SpoutConfig(
				zkHosts, 
				appProperties.getProperty(ApplicationConstants.ZK_TOPIC), 
				appProperties.getProperty(ApplicationConstants.ZK_ROOT), 
				appProperties.getProperty(ApplicationConstants.ZK_ID));
		spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
		spoutConfig.forceFromStart = true;	//Forcefully starting from the beginning of the kafka topic 
		KafkaSpout kafkaSpout = new KafkaSpout(spoutConfig);
		
		//Configuring HdfsBolt
		ArrayList<String> columnNames = new ArrayList<String>();
		columnNames.add("ipaddress");
		columnNames.add("username");
		columnNames.add("eventdate");
		columnNames.add("eventtime");
		columnNames.add("visitedurl");
		String tableName = "Raw_event";

		// use "|" instead of "," for field delimiter
		RecordFormat format = new DelimitedRecordFormat()
		        .withFieldDelimiter(appProperties.getProperty(ApplicationConstants.REC_FMT_DELIM));
		// sync the file system after every 1k tuples
		SyncPolicy syncPolicy = new CountSyncPolicy(
				Integer.parseInt(appProperties.getProperty(ApplicationConstants.SYNC_POL_COUNT)));
		// rotate files when they reach 5MB
		FileRotationPolicy rotationPolicy = new FileSizeRotationPolicy(
				Float.parseFloat(appProperties.getProperty(ApplicationConstants.FILE_SIZE_ROT_POL)), 
				Units.MB);
		FileNameFormat fileNameFormat = new DefaultFileNameFormat()
		        .withPath(appProperties.getProperty(ApplicationConstants.FILE_NAME_FORMAT));
		HdfsBolt hdfsBolt = new HdfsBolt()
		        .withFsUrl(appProperties.getProperty(ApplicationConstants.HDFS_URL))
		        .withFileNameFormat(fileNameFormat)
		        .withRecordFormat(format)
		        .withRotationPolicy(rotationPolicy)
		        .withSyncPolicy(syncPolicy);

		//Configuring and instantiating TopologyBuilder
		TopologyBuilder topologyBuilder = new TopologyBuilder();
		topologyBuilder.setSpout(ApplicationConstants.KAFKA_SPOUT, kafkaSpout);
		
		topologyBuilder.setBolt(ApplicationConstants.LOG_VALIDATOR, new LogValidatorBolt()).shuffleGrouping(ApplicationConstants.KAFKA_SPOUT);
		topologyBuilder.setBolt(ApplicationConstants.REC_EXTRACTOR, new RecordExtractorBolt()).shuffleGrouping(ApplicationConstants.LOG_VALIDATOR);

		topologyBuilder.setBolt(ApplicationConstants.HDFS_BOLT, hdfsBolt).shuffleGrouping(ApplicationConstants.KAFKA_SPOUT);

		Config conf = new Config();
		if(appProperties.getProperty(ApplicationConstants.APP_MODE)
				.equalsIgnoreCase(ApplicationConstants.DEBUG)) {
			conf.setDebug(true);
		}

		if (args != null && args.length > 0) {
			conf.setNumWorkers(3);
			StormSubmitter.submitTopology(args[0], conf,
					topologyBuilder.createTopology());
		} else {
			conf.setMaxTaskParallelism(3);
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology(ApplicationConstants.TOPOLOGY_NAME, conf,
					topologyBuilder.createTopology());

			// Thread.sleep(10000);
			// cluster.shutdown();
		}
	}
}