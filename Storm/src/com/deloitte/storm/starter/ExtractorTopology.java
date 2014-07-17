package com.deloitte.storm.starter;

import java.util.ArrayList;
import java.util.List;

import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.TopologyBuilder;

import com.deloitte.storm.bolt.CleanEventBolt;
import com.deloitte.storm.rdbms.RDBMSDumperBolt;

/**
 * This topology demonstrates Storm's stream groupings and multilang
 * capabilities.
 */
public class ExtractorTopology {

	public static void main(String[] args) throws Exception {
		List<String> hosts = new ArrayList<String>();
		// BrokerHosts brokerHosts = new ZkHosts("localhost");
		SpoutConfig kafkaConf = new SpoutConfig(new ZkHosts("localhost",
				"/brokers"), "SimpleEvent", "/tmp", "1");
		// SpoutConfig kafkaConf = new SpoutConfig(brokerHosts, "test",
		// "/kafkastorm", "discovery");
		kafkaConf.scheme = new SchemeAsMultiScheme(new StringScheme());
		// kafkaConf.forceStartOffsetTime(-2);

		kafkaConf.zkServers = new ArrayList<String>() {
			{
				add("localhost");
			}
		};
		kafkaConf.zkPort = 2181;
		KafkaSpout kafkaSpout = new KafkaSpout(kafkaConf);
		kafkaConf.forceFromStart = true;
		
		
		ArrayList<String> columnNames = new ArrayList<String>();
		String tableName = "Raw_event";

		TopologyBuilder builder = new TopologyBuilder();

		// builder.setSpout("spout", new RandomSentenceSpout(), 10);
		builder.setSpout("spout", kafkaSpout, 10);

		// columnNames.add("event_id");
		columnNames.add("ipaddress");
		columnNames.add("username");
		columnNames.add("eventdate");
		columnNames.add("eventtime");
		columnNames.add("visitedurl");

		
		
		
		// add dumper bolt to the builder

		builder.setBolt("split", new CleanEventBolt(), 20).shuffleGrouping(
				"spout");
		// builder.setBolt("count", new WordCount(), 12).fieldsGrouping("split",
		// new Fields("word"));

		// add dumper bolt to the builder
		RDBMSDumperBolt dumperBolt = new RDBMSDumperBolt(tableName, columnNames);
		builder.setBolt("dumperBolt", dumperBolt, 20).noneGrouping("split");

		Config conf = new Config();
		//conf.setDebug(true);

		if (args != null && args.length > 0) {
			conf.setNumWorkers(3);

			StormSubmitter.submitTopology(args[0], conf,
					builder.createTopology());
		} else {
			conf.setMaxTaskParallelism(3);

			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology("hbase-word-count", conf,
					builder.createTopology());

			// Thread.sleep(10000);

			// cluster.shutdown();
		}
	}
}