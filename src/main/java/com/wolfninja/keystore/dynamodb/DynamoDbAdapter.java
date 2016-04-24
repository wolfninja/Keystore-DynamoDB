package com.wolfninja.keystore.dynamodb;

import java.util.Objects;
import java.util.Optional;

import javax.annotation.Nonnull;

import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.amazonaws.services.dynamodbv2.model.TableDescription;
import com.amazonaws.util.Throwables;
import com.wolfninja.keystore.api.KeyValueStoreAdapter;
import com.wolfninja.keystore.api.Keyspace;

public class DynamoDbAdapter implements KeyValueStoreAdapter {

	public static final String DEFAULT_ATTRIBUTE_KEYSPACE = "bucket";

	public static final String DEFAULT_ATTRIBUTE_KEY = "key";

	public static final String DEFAULT_ATTRIBUTE_VALUE = "value";

	public static final String DEFAULT_ATTRIBUTE_VERSION = "version";

	@Nonnull
	public static DynamoDbAdapter create(@Nonnull final Table table) {
		Objects.requireNonNull(table, "Table must not be null");
		return new DynamoDbAdapter(table, DEFAULT_ATTRIBUTE_KEYSPACE, DEFAULT_ATTRIBUTE_KEY, DEFAULT_ATTRIBUTE_VALUE,
				DEFAULT_ATTRIBUTE_VERSION);
	}

	@Nonnull
	public static DynamoDbAdapter create(@Nonnull final Table table, @Nonnull final String attributeNameKeyspace,
			@Nonnull final String attributeNameKey, @Nonnull final String attributeNameValue,
			@Nonnull final String attributeNameVersion) {
		Objects.requireNonNull(table, "Table must not be null");
		Objects.requireNonNull(attributeNameKeyspace, "AttributeNameKeyspace must not be null");
		Objects.requireNonNull(attributeNameKey, "AttributeNameKey must not be null");
		Objects.requireNonNull(attributeNameValue, "AttributeNameValue must not be null");
		Objects.requireNonNull(attributeNameVersion, "AttributeNameVersion must not be null");
		return new DynamoDbAdapter(table, attributeNameKeyspace, attributeNameKey, attributeNameValue,
				attributeNameVersion);
	}

	public static Table createNewTable(@Nonnull final DynamoDB dynamoDB, @Nonnull final String tableName) {
		Objects.requireNonNull(dynamoDB, "DynamoDB must not be null");
		Objects.requireNonNull(tableName, "TableName must not be null");
		return createNewTable(dynamoDB, tableName, DEFAULT_ATTRIBUTE_KEYSPACE, DEFAULT_ATTRIBUTE_KEY);
	}

	public static Table createNewTable(@Nonnull final DynamoDB dynamoDB, @Nonnull final String tableName,
			@Nonnull final String attributeNameKeyspace, @Nonnull final String attributeNameKey) {
		Objects.requireNonNull(dynamoDB, "DynamoDB must not be null");
		Objects.requireNonNull(tableName, "TableName must not be null");
		Objects.requireNonNull(attributeNameKeyspace, "AttributeNameKeyspace must not be null");
		Objects.requireNonNull(attributeNameKey, "AttributeNameKey must not be null");

		final CreateTableRequest request = new CreateTableRequest() //
				.withTableName(tableName) //
				.withAttributeDefinitions( //
						new AttributeDefinition(attributeNameKeyspace, ScalarAttributeType.S), //
						new AttributeDefinition(attributeNameKey, ScalarAttributeType.S) //
				) //
				.withKeySchema(//
						new KeySchemaElement(attributeNameKeyspace, KeyType.HASH), //
						new KeySchemaElement(attributeNameKey, KeyType.RANGE)) //
				.withProvisionedThroughput(new ProvisionedThroughput(2L, 1L));

		final Table table = dynamoDB.createTable(request);
		try {
			table.waitForActive();
		} catch (final InterruptedException e) {
			throw Throwables.failure(e);
		}
		return table;
	}

	public static Optional<Table> loadTable(final DynamoDB dynamoDB, final String tableName)
			throws InterruptedException {
		final Table table = dynamoDB.getTable(tableName);

		final TableDescription descr = table.waitForActiveOrDelete();
		if (descr != null) {
			return Optional.ofNullable(table);
		}
		return Optional.empty();
	}

	private final String attributeNameKeyspace;

	private final String attributeNameKey;

	private final String attributeNameValue;

	private final Table table;

	private final String attributeNameVersion;

	protected DynamoDbAdapter(@Nonnull final Table table, @Nonnull final String attributeNameKeyspace,
			@Nonnull final String attributeNameKey, @Nonnull final String attributeNameValue,
			@Nonnull final String attributeNameVersion) {
		Objects.requireNonNull(table, "Table must not be null");
		Objects.requireNonNull(attributeNameKeyspace, "AttributeNameKeyspace must not be null");
		Objects.requireNonNull(attributeNameKey, "AttributeNameKey must not be null");
		Objects.requireNonNull(attributeNameValue, "AttributeNameValue must not be null");
		Objects.requireNonNull(attributeNameVersion, "AttributeNameVersion must not be null");
		this.table = table;
		this.attributeNameKeyspace = attributeNameKeyspace;
		this.attributeNameKey = attributeNameKey;
		this.attributeNameValue = attributeNameValue;
		this.attributeNameVersion = attributeNameVersion;
	}

	public String getAttributeNameKey() {
		return attributeNameKey;
	}

	public String getAttributeNameKeyspace() {
		return attributeNameKeyspace;
	}

	public String getAttributeNameValue() {
		return attributeNameValue;
	}

	@Override
	@Nonnull
	public Keyspace getKeyspace(@Nonnull final String keyspaceName) {
		Objects.requireNonNull(keyspaceName, "KeyspaceName must not be null");
		return new DynamoDbKeyspace(keyspaceName, table, attributeNameKeyspace, attributeNameKey, attributeNameValue,
				attributeNameVersion);
	}

	public Table getTable() {
		return table;
	}
}
