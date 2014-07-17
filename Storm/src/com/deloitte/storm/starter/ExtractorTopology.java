package com.deloitte.storm.starter;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;
import com.deloitte.storm.rdbms.RDBMSDumperBolt;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

/**
 * This topology demonstrates Storm's stream groupings and multilang capabilities.
 */
public class ExtractorTopology {
  public static class CleanEvent extends BaseBasicBolt {

	  private String ipAddress;
	  private String userName;

	  private String eventDate;
	  private String eventTime;
	  private String visitedURL;
	  private int count = 0;
	  public void execute(Tuple tuple, BasicOutputCollector collector) {
		  //System.out.println(tuple.getString(0));
		 for(String field :tuple.getString(0).split(" "))
		 {
			 ++count;
			 field = field.replaceAll("\\[", "");
			 field = field.replaceAll("\\]", "");
			 field = field.replaceAll("\"", "");
		    switch(count){
		    	case 1: ipAddress = field;
//		    	System.out.println(ipAddress);
		    	break;
		    	case 3: userName = field;
//		    	System.out.println(userName);
		    	break;
		    	case 4: 
		    		String[] tempDate = field.split(":", 2);
		    		//Date Formatting
		eventDate = tempDate[0] ;
		System.out.println(eventDate);
		    		//eventDate = new SimpleDateFormat("dd/MMM/yyyy").format(tempDate[0]);
				
		    		
		    		eventTime = tempDate[1];
		    		
//		    		System.out.println(eventDate + " " + eventTime);
		    	
		    		break;
		    	case 7:
		    		visitedURL = field;
//		    		System.out.println(visitedURL);
		    		break;
		    	
		    		default:
		    			break;
		    }
		 }
			 collector.emit(new Values(ipAddress, userName, eventDate, eventTime, visitedURL));
		 
		  }


    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
      declarer.declare(new Fields("ipAddress", "userName", "eventDate", "eventTime", "visitedURL"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
      return null;
    }


  }

  public static void main(String[] args) throws Exception {

		List<String> hosts = new ArrayList<String>();
        //BrokerHosts brokerHosts = new ZkHosts("localhost");
        SpoutConfig kafkaConf = new SpoutConfig(new ZkHosts("localhost","/brokers"), "mytopic", "/tmp", "discovery");
        //SpoutConfig kafkaConf = new SpoutConfig(brokerHosts, "test", "/kafkastorm", "discovery");
        kafkaConf.scheme = new SchemeAsMultiScheme(new StringScheme());
//        kafkaConf.forceStartOffsetTime(-2);  
        
        kafkaConf.zkServers = new ArrayList<String>() {{  
            add("localhost");   
          }};  
          kafkaConf.zkPort = 2181;  
KafkaSpout kafkaSpout = new KafkaSpout(kafkaConf);
	  ArrayList<String> columnNames = new ArrayList<String>();
      String tableName = "Raw_event";   
      kafkaConf.forceFromStart = true;
      
    TopologyBuilder builder = new TopologyBuilder();

//    builder.setSpout("spout", new RandomSentenceSpout(), 10);
    builder.setSpout("spout", kafkaSpout, 10);
   

    //columnNames.add("event_id");
    columnNames.add("ipaddress");
    columnNames.add("username");
    columnNames.add("eventdate");
    columnNames.add("eventtime");
    columnNames.add("visitedurl");
  
    // add dumper bolt to the builder
    
    builder.setBolt("split", new CleanEvent(), 20).shuffleGrouping("spout");
//    builder.setBolt("count", new WordCount(), 12).fieldsGrouping("split", new Fields("word"));

 // add dumper bolt to the builder
    RDBMSDumperBolt dumperBolt = new RDBMSDumperBolt(tableName,columnNames);
    builder.setBolt("dumperBolt", dumperBolt, 20).noneGrouping("split");
    
    
    
    
    
    Config conf = new Config();
    conf.setDebug(true);

    if (args != null && args.length > 0) {
      conf.setNumWorkers(3);

      StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
    }
    else {
      conf.setMaxTaskParallelism(3);

      LocalCluster cluster = new LocalCluster();
      cluster.submitTopology("hbase-word-count", conf, builder.createTopology());

      //Thread.sleep(10000);

     // cluster.shutdown();
    }
  }
}