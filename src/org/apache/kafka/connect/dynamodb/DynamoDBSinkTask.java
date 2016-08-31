package org.apache.kafka.connect.dynamodb;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.DescribeTableRequest;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.amazonaws.services.dynamodbv2.model.TableDescription;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


/**
 * Sink Task that sets up DynamoDBClient for given table and injects each incoming value from Kafka
 * into the table.
 * <p> NOTE: The Table in DynamoDB should already be configured.
 * 
 * @author Ashish Kalbhor (ashish.kalbhor@gmail.com)
 *
 */
public class DynamoDBSinkTask extends SinkTask 
{
	private static final Logger log = LoggerFactory.getLogger(DynamoDBSinkTask.class);
	
	String tableName;
	String awsRegion;
	AmazonDynamoDBClient dynamoDBClient;
	List<AttributeDefinition> attributes;
	 
	  
	@Override
	public void start(Map<String, String> props) 
	{
		tableName = props.get(DynamoDBSinkConnector.DYNAMODB_TABLE_NAME);
		awsRegion = props.get(DynamoDBSinkConnector.DYNAMODB_REGION_NAME);
		
		dynamoDBClient = new AmazonDynamoDBClient(new ProfileCredentialsProvider());
		dynamoDBClient.configureRegion(Regions.fromName(awsRegion));
		
		DescribeTableRequest descRequest = new DescribeTableRequest().withTableName(tableName);
		TableDescription desc = dynamoDBClient.describeTable(descRequest).getTable();
		attributes = desc.getAttributeDefinitions();
		
		log.info("DynamoDB client has been initialized.");
	}
	
	@Override
	public void put(Collection<SinkRecord> records) 
	{
		for (SinkRecord record : records) 
		{
			String value = record.value().toString();
			
			PutItemRequest insertRequest = createPutRequest(value);
			
			// Skip the bad record.
			if(null != insertRequest)
			{
				dynamoDBClient.putItem(createPutRequest(value));
				log.info("Successfully inserted value: " + value);
			}
		}
	}
	  
	/**
	 * Given a value and dynamodb table, this method will create a PutItemRequest tuple.
	 * <p> NOTE: The values are assumed to be comma separated single record in Kafka.
	 *
	 * @param value
	 * @return PutItemRequest
	 */
	public PutItemRequest createPutRequest(String value)
	{
		Map<String, AttributeValue> item = new HashMap<>();
		int columnCount = 0;
		
		// Assuming values per attribute are comma separated.
		String[] attributeValues = value.split(",");
		
		if(attributeValues.length != attributes.size())
		{
			log.info("Incorrect mapping of attributes with incoming value: " + value);
			return null;
		}else
		{
			for(AttributeDefinition attr : attributes)
			{
				String attributeName = attr.getAttributeName();
				item.put(attributeName, new AttributeValue(attributeValues[columnCount++]));
			}
		}
				
		PutItemRequest putItemRqst = new PutItemRequest(tableName, item);
		return putItemRqst;
	}
	
 	@Override
 	public void stop() 
 	{
 	}
	
 	@Override
 	public String version() 
 	{
 		return new DynamoDBSinkConnector().version();
 	}

 	@Override
 	public void flush(Map<TopicPartition, OffsetAndMetadata> arg0) 
 	{
 		
 	}
  
}