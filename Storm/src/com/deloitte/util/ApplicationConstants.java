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
	
	//Constants for DB
	public static final String DB_DRIVER_CLASS = "db.driver.classname";
	public static final String DB_USER_NAME = "db.username";
	public static final String DB_PASSWORD = "db.password";
	public static final String DB_CONNECTION_URL = "db.connection.url";
}
