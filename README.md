# kafka-connect-dynamodb
A Kafka Sink Connector that can dump messages from Kafka into AWS DynamoDB table.

# Build the Connector
You can build the connector using Maven lifecycle phases:
```
mvn clean
mvn package
```

# Sink Connector
When the connector is run as Sink, it retrieves messages from Kafka and writes them in DynamoDB table. 
The Sink Task reads the attribute description as defined for the table in AWS configuration.
Each message is then inserted as per the attributes.

## Sample Configuration
```ini
name=dynamodb-sink-connector
connector.class=org.apache.kafka.connect.dynamodb.DynamoDBSinkConnector
tasks.max=1
dynamodb.table=sometable
dynamodb.region=us-west-2
topics=test
```

* **name**: Name of the connector
* **connector.class**: Name of the class implementation of the connector
* **tasks.max**: Maximum no. of tasks to be created.
* **dynamodb.table**: AWS DynamoDB table name which is already created.
* **dynamodb.region**: AWS Region name (E.g. "us-west-2" for Oregon region).
* **topics**: Topic the connector should be listening to.

# AWS CLI Configuration
Make sure that AWS CLI is configured on the machine.
The connector uses ProfileCredentialsProvider that will load the default credentials configured for the machine.
