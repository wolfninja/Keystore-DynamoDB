package com.wolfninja.keystore.dynamodb;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.UUID;

import org.testng.annotations.AfterClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Factory;
import org.testng.annotations.Test;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.wolfninja.keystore.api.BaseKeyspaceTest;

@Test
public class DynamoDbKeyspaceIntegrationTest extends BaseKeyspaceTest {

	private final DynamoDbKeyspace keyspace;
	private Table table;

	@Factory(dataProvider = "dp")
	public DynamoDbKeyspaceIntegrationTest(final DynamoDbKeyspace keyspace, final Table table) {
		super(keyspace);
		this.keyspace = keyspace;
		this.table = table;
	}

	@DataProvider
	static Object[][] dp() throws FileNotFoundException, IllegalArgumentException, IOException {
		final String tableName = UUID.randomUUID().toString();
		System.out.println("Using table name : " + tableName);
		final Table table = DynamoDbAdapter.createNewTable(dynamoDB(), tableName, "it_keyspace", "it_key");
		final DynamoDbKeyspace ddk = new DynamoDbKeyspace("coolKeyspace", table, "it_keyspace", "it_key", "it_value",
				"it_version");
		return new Object[][] { { ddk, table } };
	}

	@AfterClass
	public void tearDown() {
		if (table != null) {
			table.delete();
		}
	}

	public static DynamoDB dynamoDB() throws FileNotFoundException, IllegalArgumentException, IOException {
		final AWSCredentials awsCredentials = new PropertiesCredentials(
				Thread.currentThread().getContextClassLoader().getResourceAsStream("aws.test.properties"));

		final AmazonDynamoDBClient client = new AmazonDynamoDBClient(awsCredentials);
		client.setRegion(Region.getRegion(Regions.US_WEST_1));

		final DynamoDB dynamoDB = new DynamoDB(client);
		return dynamoDB;
	}
}
