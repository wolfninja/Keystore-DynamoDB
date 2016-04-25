package com.wolfninja.keystore.dynamodb;

import org.easymock.EasyMock;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.amazonaws.services.dynamodbv2.document.Table;
import com.wolfninja.keystore.api.Keyspace;

public class DynamoDbAdapterTest {

	@Test
	public void getKeyspace() {
		final Table table = EasyMock.createMock(Table.class);
		
		final DynamoDbAdapter adapter = DynamoDbAdapter.create(table);
		final Keyspace actual = adapter.getKeyspace("myKeyspace");
		
		Assert.assertEquals(actual.getClass(), DynamoDbKeyspace.class);
	}
}
