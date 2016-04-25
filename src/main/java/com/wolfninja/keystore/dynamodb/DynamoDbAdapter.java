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

/**
 * DynamoDB implementation of the {@link KeyValueStoreAdapter}
 * 
 * @since 0.1
 */
public class DynamoDbAdapter implements KeyValueStoreAdapter {

	/**
	 * Default keyspace attribute name
	 * 
	 * @since 0.1
	 */
	public static final String DEFAULT_ATTRIBUTE_KEYSPACE = "keyspace";

	/**
	 * Default key attribute name
	 * 
	 * @since 0.1
	 */
	public static final String DEFAULT_ATTRIBUTE_KEY = "key";

	/**
	 * Default value attribute name
	 * 
	 * @since 0.1
	 */
	public static final String DEFAULT_ATTRIBUTE_VALUE = "value";

	/**
	 * Default version attribute name
	 * 
	 * @since 0.1
	 */
	public static final String DEFAULT_ATTRIBUTE_VERSION = "version";

	/**
	 * Create a new {@link DynamoDbAdapter} instance using an existing {@link Table} and default attribute name <br>
	 * <br>
	 * <b>Note</b>, using attribute names that differ from the default may result in unexpected behavior. Use
	 * {@link #create(Table, String, String, String, String)} for customizing attribute names
	 * 
	 * @param table
	 *            {@link Table} to use, not null
	 * @return new {@link DynamoDbAdapter} instance backed by the given table, not null
	 * @see #create(Table, String, String, String, String)
	 * @since 0.1
	 */
	@Nonnull
	public static DynamoDbAdapter create(@Nonnull final Table table) {
		Objects.requireNonNull(table, "Table must not be null");
		return new DynamoDbAdapter(table, DEFAULT_ATTRIBUTE_KEYSPACE, DEFAULT_ATTRIBUTE_KEY, DEFAULT_ATTRIBUTE_VALUE,
				DEFAULT_ATTRIBUTE_VERSION);
	}

	/**
	 * Create a new {@link DynamoDbAdapter}, backed by the given {@link Table} and using custom attribute names
	 * 
	 * @param table
	 *            {@link Table} to use, not null
	 * @param attributeNameKeyspace
	 *            Name of keyspace attribute, not null. Part of the primary key. String attribute.
	 * @param attributeNameKey
	 *            Name of the key attribute, not null. Part of the primary key. String attribute.
	 * @param attributeNameValue
	 *            Name of the value attribute, not null. String attribute.
	 * @param attributeNameVersion
	 *            Name of the version attribute, not null. Long attribute.
	 * @return new {@link DynamoDbAdapter} instance backed by the given table, not null
	 * @since 0.1
	 */
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

	/**
	 * Helper method to create a new DynamoDB {@link Table} using default attribute names
	 * <p>
	 * See the overloaded {@link #createNewTable(DynamoDB, String, String, String)} to specify attribute names
	 * <p>
	 * <b>Note, this method blocks until the table is created + active in AWS</b>
	 * </p>
	 * 
	 * @param dynamoDB
	 *            {@link DynamoDB} instance, configured with credentials and target AWS region, not null
	 * @param tableName
	 *            Name of table to create, not null.
	 * @return Created + active {@link Table} instance, not null
	 * @see DynamoDbAdapter#createNewTable(DynamoDB, String, String, String)
	 * @since 0.1
	 */
	@Nonnull
	public static Table createNewTable(@Nonnull final DynamoDB dynamoDB, @Nonnull final String tableName) {
		Objects.requireNonNull(dynamoDB, "DynamoDB must not be null");
		Objects.requireNonNull(tableName, "TableName must not be null");
		return createNewTable(dynamoDB, tableName, DEFAULT_ATTRIBUTE_KEYSPACE, DEFAULT_ATTRIBUTE_KEY);
	}

	/**
	 * Helper method to create a new DynamoDB {@link Table}, using custom attribute names
	 * <p>
	 * See the other {@link #createNewTable(DynamoDB, String)} method to use the default attribute names.
	 * </p>
	 * <p>
	 * <b>Note, this method blocks until the table is created + active in AWS</b>
	 * </p>
	 * 
	 * @param dynamoDB
	 *            {@link DynamoDB} instance, configured with credentials and target AWS region, not null
	 * @param tableName
	 *            Name of table to create, not null
	 * @param attributeNameKeyspace
	 *            Name of keyspace attribute, not null. Part of the primary key. String attribute.
	 * @param attributeNameKey
	 *            Name of the key attribute, not null. Part of the primary key. String attribute.
	 * @return Created + active {@link Table} instance, not null
	 * @see #createNewTable(DynamoDB, String)
	 * @since 0.1
	 */
	@Nonnull
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

	/**
	 * Load existing DynamoDB {@link Table}
	 * <p>
	 * <b>Note, this method blocks until the table is active</b>
	 * </p>
	 * 
	 * @param dynamoDB
	 *            {@link DynamoDB} instance, configured with credentials and target AWS region, not null
	 * @param tableName
	 *            Table name, not null
	 * @return Optional {@link Table} instance, {@link Optional#empty()} if no table with that name
	 * @throws InterruptedException
	 * @since 0.1
	 */
	@Nonnull
	public static Optional<Table> loadTable(@Nonnull final DynamoDB dynamoDB, @Nonnull final String tableName)
			throws InterruptedException {
		Objects.requireNonNull(dynamoDB, "DynamoDB must not be null");
		Objects.requireNonNull(tableName, "TableName must not be null");
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

	/**
	 * Constructor
	 * 
	 * @param table
	 * @param attributeNameKeyspace
	 * @param attributeNameKey
	 * @param attributeNameValue
	 * @param attributeNameVersion
	 * @since 0.1
	 */
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

	/**
	 * Get configured key attribute name
	 * 
	 * @return String Key attribute name
	 * @since 0.1
	 */
	@Nonnull
	public String getAttributeNameKey() {
		return attributeNameKey;
	}

	/**
	 * Get configured keyspace attribute name
	 * 
	 * @return String Keyspace attribute name
	 * @since 0.1
	 */
	@Nonnull
	public String getAttributeNameKeyspace() {
		return attributeNameKeyspace;
	}

	/**
	 * Get configured value attribute name
	 * 
	 * @return String Value attribute name
	 * @since 0.1
	 */
	@Nonnull
	public String getAttributeNameValue() {
		return attributeNameValue;
	}

	/**
	 * Get configured version attribute name
	 * 
	 * @return String Version attribute name
	 * @since 0.1
	 */
	@Nonnull
	public String getAttributeNameVersion() {
		return attributeNameVersion;
	}

	@Override
	@Nonnull
	public Keyspace getKeyspace(@Nonnull final String keyspaceName) {
		Objects.requireNonNull(keyspaceName, "KeyspaceName must not be null");
		return new DynamoDbKeyspace(keyspaceName, table, attributeNameKeyspace, attributeNameKey, attributeNameValue,
				attributeNameVersion);
	}

	/**
	 * Get the configured {@link Table} backing this adapter
	 * 
	 * @return {@link Table} instance
	 * @since 0.1
	 */
	@Nonnull
	public Table getTable() {
		return table;
	}
}
