package com.wolfninja.keystore.dynamodb;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import javax.annotation.Nonnull;

import com.amazonaws.services.dynamodbv2.document.AttributeUpdate;
import com.amazonaws.services.dynamodbv2.document.DeleteItemOutcome;
import com.amazonaws.services.dynamodbv2.document.Expected;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.PrimaryKey;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.UpdateItemOutcome;
import com.amazonaws.services.dynamodbv2.document.spec.DeleteItemSpec;
import com.amazonaws.services.dynamodbv2.document.spec.GetItemSpec;
import com.amazonaws.services.dynamodbv2.document.spec.UpdateItemSpec;
import com.amazonaws.services.dynamodbv2.model.ConditionalCheckFailedException;
import com.amazonaws.services.dynamodbv2.model.ReturnValue;
import com.wolfninja.keystore.api.KeyValue;
import com.wolfninja.keystore.api.Keyspace;

public class DynamoDbKeyspace implements Keyspace {

	private static final boolean CONSISTENT_READ = false;
	private final String keyspaceName;
	private final Table table;
	private final String attributeNameKeyspace;
	private final String attributeNameKey;
	private final String attributeNameValue;
	private final String attributeNameVersion;

	protected DynamoDbKeyspace(@Nonnull final String keyspaceName, @Nonnull final Table table,
			@Nonnull final String attributeNameKeyspace, @Nonnull final String attributeNameKey,
			@Nonnull final String attributeNameValue, @Nonnull final String attributeNameVersion) {
		Objects.requireNonNull(keyspaceName, "KeyspaceName must not be null");
		Objects.requireNonNull(table, "Table must not be null");
		Objects.requireNonNull(attributeNameKeyspace, "AttributeNameKeyspace must not be null");
		Objects.requireNonNull(attributeNameKey, "AttributeNameKey must not be null");
		Objects.requireNonNull(attributeNameValue, "AttributeNameValue must not be null");
		Objects.requireNonNull(attributeNameVersion, "AttributeNameVersion must not be null");

		this.keyspaceName = keyspaceName;
		this.table = table;
		this.attributeNameKeyspace = attributeNameKeyspace;
		this.attributeNameKey = attributeNameKey;
		this.attributeNameValue = attributeNameValue;
		this.attributeNameVersion = attributeNameVersion;
	}

	@Override
	public boolean add(final String key, final String value) {
		Objects.requireNonNull(key, "Key must not be null");
		Objects.requireNonNull(value, "Value must not be null");

		final Item item = buildItem(key, value);
		final Map<String, String> nameMap = new HashMap<>();
		nameMap.put("#b", attributeNameKeyspace);
		try {
			table.putItem(item, "attribute_not_exists(#b)", nameMap, null);
			return true;
		} catch (ConditionalCheckFailedException ex) {
			return false;
		}
	}

	private Item buildItem(final String key, final String value) {
		return new Item() //
				.withPrimaryKey(buildPrimaryKey(key)) //
				.withString(attributeNameValue, value) //
				.withLong(attributeNameVersion, value.hashCode());
	}

	private PrimaryKey buildPrimaryKey(final String key) {
		Objects.requireNonNull(key, "Key must not be null");
		return new PrimaryKey(attributeNameKeyspace, keyspaceName, attributeNameKey, key);
	}

	@Override
	public boolean checkAndSet(final String key, final String value, final long version) {
		Objects.requireNonNull(key, "Key must not be null");
		Objects.requireNonNull(value, "Value must not be null");

		final UpdateItemSpec spec = new UpdateItemSpec() //
				.withPrimaryKey(buildPrimaryKey(key)) //
				.withExpected(new Expected(attributeNameVersion).eq(version)) //
				.withAttributeUpdate( //
						new AttributeUpdate(attributeNameValue).put(value), //
						new AttributeUpdate(attributeNameVersion).put(value.hashCode()) //
		);

		try {
			table.updateItem(spec);
			return true;
		} catch (final ConditionalCheckFailedException e) {
			return false;
		}
	}

	@Override
	public boolean delete(final String key) {
		Objects.requireNonNull(key, "Key must not be null");
		final DeleteItemSpec spec = new DeleteItemSpec() //
				.withReturnValues(ReturnValue.ALL_OLD) //
				.withPrimaryKey(buildPrimaryKey(key));
		final DeleteItemOutcome outcome = table.deleteItem(spec);
		return outcome.getItem() != null;
	}

	@Override
	public boolean deletes(final String key, final long version) {
		Objects.requireNonNull(key, "Key must not be null");
		final DeleteItemSpec spec = new DeleteItemSpec() //
				.withReturnValues(ReturnValue.ALL_OLD) //
				.withPrimaryKey(buildPrimaryKey(key)) //
				.withExpected(new Expected(attributeNameVersion).eq(version));

		try {
			final DeleteItemOutcome outcome = table.deleteItem(spec);
			return outcome.getItem() != null;
		} catch (final ConditionalCheckFailedException ex) {
			return false;
		}
	}

	@Override
	public boolean exists(final String key) {
		Objects.requireNonNull(key, "Key must not be null");
		final GetItemSpec spec = new GetItemSpec() //
				.withPrimaryKey(buildPrimaryKey(key)) //
				.withAttributesToGet(attributeNameKey) //
				.withConsistentRead(CONSISTENT_READ); //
		return table.getItem(spec) != null;
	}

	@Override
	public Optional<String> get(final String key) {
		Objects.requireNonNull(key, "Key must not be null");
		final GetItemSpec spec = new GetItemSpec() //
				.withPrimaryKey(buildPrimaryKey(key)) //
				.withConsistentRead(CONSISTENT_READ); //

		final Item item = table.getItem(spec);
		if (item == null) {
			return Optional.empty();
		}
		return Optional.ofNullable(item.getString(attributeNameValue));
	}

	@Override
	public Optional<KeyValue> gets(final String key) {
		Objects.requireNonNull(key, "Key must not be null");
		final GetItemSpec spec = new GetItemSpec() //
				.withPrimaryKey(buildPrimaryKey(key)) //
				.withConsistentRead(CONSISTENT_READ); //

		final Item item = table.getItem(spec);
		if (item == null) {
			return Optional.empty();
		}

		return Optional
				.of(KeyValue.create(key, item.getString(attributeNameValue), item.getLong(attributeNameVersion)));
	}

	@Override
	public boolean replace(final String key, final String value) {
		Objects.requireNonNull(key, "Key must not be null");
		Objects.requireNonNull(value, "Value must not be null");

		final UpdateItemSpec spec = new UpdateItemSpec() //
				.withReturnValues(ReturnValue.ALL_OLD) //
				.withPrimaryKey(attributeNameKeyspace, keyspaceName, attributeNameKey, key) //
				.withAttributeUpdate( //
						new AttributeUpdate(attributeNameValue).put(value), //
						new AttributeUpdate(attributeNameVersion).put(value.hashCode()) //
				) //
				.withExpected(new Expected(attributeNameKey).exists());

		try {
			final UpdateItemOutcome outcome = table.updateItem(spec);
			final Item item = outcome.getItem();
			if (item == null) {
				return true;
			}
			return !Objects.equals(item.get(attributeNameValue), value);
		} catch (ConditionalCheckFailedException ex) {
			return false;
		}
	}

	@Override
	public boolean set(final String key, final String value) {
		Objects.requireNonNull(key, "Key must not be null");
		Objects.requireNonNull(value, "Value must not be null");

		final Item item = buildItem(key, value);
		table.putItem(item);
		return true;
	}

}
