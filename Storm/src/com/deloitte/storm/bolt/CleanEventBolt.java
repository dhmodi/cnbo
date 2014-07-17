package com.deloitte.storm.bolt;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Map;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class CleanEventBolt extends BaseBasicBolt {

	private String ipAddress;
	private String userName;

	private String eventDate;
	private String eventTime;
	private String visitedURL;
	private int count = 0;

	public void execute(Tuple tuple, BasicOutputCollector collector) {
		// System.out.println(tuple.getString(0));
//		for (String field : tuple.getString(0).split(" ")) {
//			++count;
//			field = field.replaceAll("\\[", "");
//			field = field.replaceAll("\\]", "");
//			field = field.replaceAll("\"", "");
//			switch (count) {
//			case 1:
//				ipAddress = field;
//				// System.out.println(ipAddress);
//				break;
//			case 3:
//				userName = field;
//				// System.out.println(userName);
//				break;
//			case 4:
//				String[] tempDate = field.split(":", 2);
//				// Date Formatting
//				eventDate = tempDate[0];
//				//System.out.println(eventDate);
//				// eventDate = new
//				// SimpleDateFormat("dd/MMM/yyyy").format(tempDate[0]);
//
//				eventTime = tempDate[1];
//
//				// System.out.println(eventDate + " " + eventTime);
//
//				break;
//			case 7:
//				visitedURL = field;
//				// System.out.println(visitedURL);
//				break;
//
//			default:
//				break;
//			}
//		}
		
		
		
		String[] field = tuple.getString(0).split(" ");
		ipAddress = field[0].replaceAll("\\[", "").replaceAll("\\]", "").replaceAll("\"", "");
//		System.out.println(ipAddress);
		userName = field[2].replaceAll("\\[", "").replaceAll("\\]", "").replaceAll("\"", "");
//		System.out.println(userName);
		String[] tempDate = field[3].replaceAll("\\[", "").replaceAll("\\]", "").replaceAll("\"", "").split(":", 2);
//		System.out.println(field[3]);
		eventDate = tempDate[0];
//		System.out.println(eventDate);
		// eventDate = new
		// SimpleDateFormat("dd/MMM/yyyy").format(tempDate[0]);

		eventTime = tempDate[1];
		visitedURL = field[6].replaceAll("\\[", "").replaceAll("\\]", "").replaceAll("\"", "");
//		System.out.println(visitedURL);
		collector.emit(new Values(ipAddress, userName, eventDate, eventTime,
				visitedURL));

	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("ipAddress", "userName", "eventDate",
				"eventTime", "visitedURL"));
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

}
