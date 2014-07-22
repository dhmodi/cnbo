package com.deloitte.kafka.producer;


import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import java.util.*;
import java.io.*;

import com.deloitte.kafka.util.ProducerConstant;


public class KafkaProducer  {

	public static void main(String[] args) {
		String propertiesPath = args[0];

		Process p;
		Properties props = new Properties();
		String line = "";

		String msg = "";
		String num = "1";

		Properties properties = new Properties();
		try {
			properties.load(new FileInputStream(propertiesPath));
		} catch (IOException e) {
			e.printStackTrace();
		}

		String Command = "tail -f "
				+ properties
						.getProperty(ProducerConstant.INPUT_FILE_NAME);
		String Broker_List = properties
				.getProperty(ProducerConstant.BROKER_LIST);
		//System.out.println(Broker_List);
		String Serializer_type = properties
				.getProperty(ProducerConstant.SERIALIZER_TYPE);
		String Num_Of_Ack = properties
				.getProperty(ProducerConstant.NUM_OF_ACK);
		String Topic_To_Write = properties
				.getProperty(ProducerConstant.TOPIC_TO_WRITE);
		props.put("metadata.broker.list", Broker_List);
		props.put("serializer.class", Serializer_type);
		// props.put("partitioner.class", "example.producer.SimplePartitioner");
		props.put("request.required.acks", Num_Of_Ack);

		ProducerConfig config = new ProducerConfig(props);
		Producer<String, String> producer = new Producer<String, String>(config);

		try {
			p = Runtime.getRuntime().exec(Command);
			// p.waitFor();
			BufferedReader reader = new BufferedReader(new InputStreamReader(
					p.getInputStream()));

			while ((line = reader.readLine()) != null) {

				msg = line + "\n";
				KeyedMessage<String, String> data = new KeyedMessage<String, String>(
						Topic_To_Write, num, msg);
				//System.out.println("Dhaval");
				producer.send(data);

			}
			producer.close();

		} catch (Exception e) {
			e.printStackTrace();
		}

	}
}


