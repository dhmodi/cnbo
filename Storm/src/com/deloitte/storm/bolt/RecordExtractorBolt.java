package com.deloitte.storm.bolt;

import java.beans.PropertyVetoException;
import java.io.FileInputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

import com.deloitte.datasource.DataSource;
import com.deloitte.util.ApplicationConstants;

public class RecordExtractorBolt implements IRichBolt {
	private static final long serialVersionUID = 1L;
	private OutputCollector collector;
	private Pattern logPattern;
	private Connection connection;
	private DataSource dataSource;
	private PreparedStatement preparedStatement;
	private int recCounter = 0;
	
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		this.collector = collector;
		try {
			Properties appProperties = new Properties();
			appProperties.load(new FileInputStream(ApplicationConstants.APP_RECOURCE));
			dataSource = DataSource.getInstance(appProperties);
			connection = dataSource.getConnection();
			preparedStatement = connection.prepareStatement("insert into INTERPRETER_15MIN(IPADDRESS, USERNAME, VISITDATE, PAGEURL) values(?, ?, TO_TIMESTAMP_TZ (?, 'DD/MON/YYYY:HH24:MI:SS TZH:TZM'), ?)");
		} catch (SQLException e1) {
			e1.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (PropertyVetoException e) {
			e.printStackTrace();
		}
		StringBuilder logFormat = new StringBuilder();
		logFormat.append("(\\s*\\d+\\.\\d+.\\d+.\\d+\\s*)");
		logFormat.append("(\\s*-\\s+)");
		logFormat.append("(\\s*-\\s+)");
		logFormat.append("(\\s*\\[\\d+/\\w+/\\d+:\\d+:\\d+:\\d+\\s+-\\d+\\]\\s*)");
		logFormat.append("(\\s*\"\\w+\\s*)");
		logFormat.append("(\\s*[\\w\\d/\\.]+\\s*)");
		logFormat.append("(\\s*[\\w\\d\\./]+\")");
		logFormat.append("(\\s*\\d+\\s+)");
		logFormat.append("(\\s*\\d+\\s+)");
		logFormat.append("(\\s*\".*\"\\s*)");
		logFormat.append("(\\s*\"[.\\w\\W\\d\\D]*\"\\s*)");
		logPattern = Pattern.compile(logFormat.toString());
	}

	public void execute(Tuple input) {
		try {
			String log = input.getString(0);
			Matcher matcher = logPattern.matcher(log);
			Map<String, String> map = new HashMap<String, String>();
			while (matcher.find()) {
				preparedStatement.setString(1, matcher.group(1).trim());
				preparedStatement.setString(2, matcher.group(3).trim());
				String dateTime = matcher.group(4).trim();
				dateTime = matcher.group(4).trim().replace("[", "").replace("]", "");
				preparedStatement.setString(3, dateTime);
				preparedStatement.setString(4, matcher.group(6).trim());
			}
			preparedStatement.addBatch();
			if(++recCounter % 1000 == 0) {
				preparedStatement.executeBatch();
		    }
		} catch (SQLException e) {
			e.printStackTrace();
		}
		collector.ack(input);
	}

	public void cleanup() {
		try {
			preparedStatement.executeBatch();	//insert remaining records
//			preparedStatement.close();
//			connection.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
//		declarer.declare(new Fields("Interpreter15Min"));
	}

	public Map<String, Object> getComponentConfiguration() {
		return null;
	}
	
}