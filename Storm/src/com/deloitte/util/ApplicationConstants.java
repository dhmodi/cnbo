package com.deloitte.util;

public interface ApplicationConstants {
	
	//Generic constants
	public static final String APP_RECOURCE = "/home/cloudera/workspace/CNBO/src/main/resources/ApplicationResources.properties";
	
	//Constants for Producer 
	public static final String INPUT_FILE_NAME = "producer.file.name";	
	public static final String BROKER_LIST = "producer.broker.list";	
	public static final String SERIALIZER_TYPE = "producer.serializer.type";
	public static final String NUM_OF_ACK = "producer.ack.request";
	public static final String TOPIC_TO_WRITE = "producer.topic.name";
	public static final String METADATA_BRKR_LIST = "metadata.broker.list";
	public static final String SERLZR_CLASS = "serializer.class";
	public static final String REQ_RQRD_ACKS = "request.required.acks";
	
	//Constants for Consumer
	public static final String ZK_HOST = "zookeeper.host";
	public static final String ZK_TOPIC = "zookeeper.topic";
	public static final String ZK_ROOT = "zookeeper.root";
	public static final String ZK_ID = "zookeeper.id";
	
	//Constants for Topology
	public static final String KAFKA_SPOUT = "kafka_spout";
	public static final String LOG_VALIDATOR = "log_validator";
	public static final String REC_EXTRACTOR = "rec_extractor";
	public static final String HDFS_BOLT = "hdfs_bolt";
	public static final String TOPOLOGY_NAME = "hbase-word-count";
	//Constants for DB
	public static final String DB_DRIVER_CLASS = "db.driver.classname";
	public static final String DB_USER_NAME = "db.username";
	public static final String DB_PASSWORD = "db.password";
	public static final String DB_CONNECTION_URL = "db.connection.url";
	
	//Constants for HDFS
	public static final String REC_FMT_DELIM = "hdfs.record.format.delimeter";
	public static final String SYNC_POL_COUNT = "hdfs.sync.policy.count";
	public static final String FILE_SIZE_ROT_POL = "hdfs.file.size.rotation.policy";
	public static final String FILE_NAME_FORMAT = "hdfs.file.name.format";
	public static final String HDFS_URL = "hdfs.url";
	
	//Constants for Application Config
	public static final String APP_MODE = "app.mode";
	public static final String DEBUG = "DEBUG";
	
	//Constants for different fields of log file
	public static final String INDEX_IPADDRESS = "log.cont.gr.IPADDRESS";
	public static final String INDEX_USERNAME = "log.cont.gr.USERNAME";
	public static final String INDEX_VISITDATE = "log.cont.gr.VISITDATE";
	public static final String INDEX_PAGEURL = "log.cont.gr.PAGEURL";
	
}
