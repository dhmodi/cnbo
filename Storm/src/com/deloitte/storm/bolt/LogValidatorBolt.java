package com.deloitte.storm.bolt;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class LogValidatorBolt implements IRichBolt {
	private static final long serialVersionUID = 1L;
	private OutputCollector collector;
	private Pattern logPattern;
	private List<String> logs = new ArrayList<String>();
	
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		this.collector = collector;
	}

	public void execute(Tuple input) {
		String log = input.getString(0);
		StringBuilder logFormat = new StringBuilder();
		logFormat.append("(\\s*\\d+\\.\\d+.\\d+.\\d+\\s*)");
		logFormat.append("(\\s*-\\s+-\\s*)");
		logFormat.append("(\\s*\\[\\d+/\\w+/\\d+:\\d+:\\d+:\\d+\\s+-\\d+\\]\\s*)");
		logFormat.append("(\\s*\"\\w+\\s*)");
		logFormat.append("(\\s*[\\w\\d/\\.]+\\s*)");
		logFormat.append("(\\s*[\\w\\d\\./]+\")");
		logFormat.append("(\\s*\\d+\\s+)");
		logFormat.append("(\\s*\\d+\\s+)");
		logFormat.append("(\\s*\".*\"\\s*)");
		logFormat.append("(\\s*\"[.\\w\\W\\d\\D]*\"\\s*)");
		logPattern = Pattern.compile(logFormat.toString());
		Matcher matcher = logPattern.matcher(log);
		if(matcher.matches()) {
			logs.add("One match found!");
			collector.emit(new Values(log));
		} else {
			System.out.println("No match found!");
		}
		collector.ack(input);
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("log"));
	}

	public void cleanup() {
	}

	public Map<String, Object> getComponentConfiguration() {
		return null;
	}
}