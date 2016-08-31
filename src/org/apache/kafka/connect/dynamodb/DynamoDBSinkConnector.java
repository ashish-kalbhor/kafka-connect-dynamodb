package org.apache.kafka.connect.dynamodb;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkConnector;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Sink Kafka Connector for AWS DynamoDB.
 * 
 * @author Ashish Kalbhor (ashish.kalbhor@gmail.com)
 *
 */
public class DynamoDBSinkConnector extends SinkConnector 
{
	// Properties to be configured in dynamodb.properties file
	
	// Table Name
	public static final String DYNAMODB_TABLE_NAME = "dynamodb.table";
	// Region Name
	public static final String DYNAMODB_REGION_NAME = "dynamodb.region";

	private String tableName;
	private String awsRegion;

	@Override
	public String version() 
	{
		return AppInfoParser.getVersion();
	}

	@Override
	public void start(Map<String, String> props) 
	{
		tableName = props.get(DYNAMODB_TABLE_NAME);
		awsRegion = props.get(DYNAMODB_REGION_NAME);
    
		if(tableName == null || tableName.isEmpty())
			throw new ConnectException("DynamoDB table name not configured.");
		
		if(awsRegion == null || awsRegion.isEmpty())
			throw new ConnectException("DynamoDB region not configured.");
    		
	}

	@Override
	public Class<? extends Task> taskClass() 
	{
		return DynamoDBSinkTask.class;
	}

	@Override
	public List<Map<String, String>> taskConfigs(int maxTasks) 
	{
		ArrayList<Map<String, String>> configs = new ArrayList<>();
		for (int i = 0; i < maxTasks; i++) 
		{
			Map<String, String> config = new HashMap<>();
			
			config.put(DYNAMODB_TABLE_NAME, tableName);
			config.put(DYNAMODB_REGION_NAME, awsRegion);
			configs.add(config);
		}
		return configs;
	}

	@Override
	public void stop() 
	{
		//not implemented
	}

	@Override
	public ConfigDef config() 
	{
		return null;
	}
}
