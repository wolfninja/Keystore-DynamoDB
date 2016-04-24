package com.wolfninja.keystore.dynamodb;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import org.easymock.EasyMock;
import org.easymock.LogicalOperator;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.amazonaws.services.dynamodbv2.document.AttributeUpdate;
import com.amazonaws.services.dynamodbv2.document.DeleteItemOutcome;
import com.amazonaws.services.dynamodbv2.document.Expected;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.KeyAttribute;
import com.amazonaws.services.dynamodbv2.document.PutItemOutcome;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.UpdateItemOutcome;
import com.amazonaws.services.dynamodbv2.document.spec.DeleteItemSpec;
import com.amazonaws.services.dynamodbv2.document.spec.GetItemSpec;
import com.amazonaws.services.dynamodbv2.document.spec.UpdateItemSpec;
import com.amazonaws.services.dynamodbv2.model.ConditionalCheckFailedException;
import com.amazonaws.services.dynamodbv2.model.ReturnValue;
import com.wolfninja.keystore.api.KeyValue;

public class DynamoDbKeyspaceTest {

	public static Comparator<AttributeUpdate> attributeUpdateComparator() {
		return new Comparator<AttributeUpdate>() {

			@Override
			public int compare(AttributeUpdate o1, AttributeUpdate o2) {
				int i = o1.getAction().compareTo(o2.getAction());
				if (i != 0)
					return i;

				i = o1.getAttributeName().compareTo(o2.getAttributeName());
				if (i != 0)
					return i;

				i = collectionComparator(objectComparator()).compare(o1.getAttributeValues(), o2.getAttributeValues());
				if (i != 0)
					return i;

				return objectComparator().compare(o1.getValue(), o2.getValue());
			}
		};
	}

	public static <T> Comparator<Collection<T>> collectionComparator(final Comparator<T> comparator) {
		return new Comparator<Collection<T>>() {

			@Override
			public int compare(Collection<T> o1, Collection<T> o2) {
				if (o1 == null || o2 == null) {
					if (o1 == o2)
						return 0;
					return o1 == null ? -1 : 1;
				}
				if (o1.size() != o2.size())
					return Integer.compare(o1.size(), o2.size());

				final List<T> list1 = new ArrayList<>(o1);
				final List<T> list2 = new ArrayList<>(o2);
				for (int i = 0; i < o1.size(); i++) {
					int c = comparator.compare(list1.get(i), list2.get(i));
					if (c != 0)
						return c;
				}
				return 0;
			}
		};
	}

	public static <E extends Comparable<E>> Comparator<E> defaultComparator(final Class<E> clazz) {
		return new Comparator<E>() {

			@Override
			public int compare(E o1, E o2) {
				if (o1 == o2)
					return 0;
				if (o1 == null)
					return -1;
				if (o2 == null)
					return 1;
				if (o1.equals(o2))
					return 0;
				return o1.compareTo(o2);
			}
		};
	}

	public static Comparator<DeleteItemSpec> deleteItemSpecComparator() {
		return new Comparator<DeleteItemSpec>() {

			@Override
			public int compare(DeleteItemSpec o1, DeleteItemSpec o2) {
				int i = o1.getReturnValues().compareTo(o2.getReturnValues());
				if (i != 0)
					return i;

				i = keyAttributeCollectionComparator().compare(o1.getKeyComponents(), o2.getKeyComponents());
				if (i != 0)
					return i;

				return collectionComparator(expectedComparator()).compare(o1.getExpected(), o2.getExpected());
			}
		};
	}

	public static Comparator<Expected> expectedComparator() {
		return new Comparator<Expected>() {

			@Override
			public int compare(Expected o1, Expected o2) {
				int i = o1.getAttribute().compareTo(o2.getAttribute());
				if (i != 0)
					return i;

				i = o1.getComparisonOperator().compareTo(o2.getComparisonOperator());
				if (i != 0)
					return i;

				return Arrays.equals(o1.getValues(), o2.getValues()) ? 0 : -1;
			}
		};
	}

	private static Comparator<GetItemSpec> getItemSpecComparator() {
		return new Comparator<GetItemSpec>() {

			@Override
			public int compare(final GetItemSpec o1, final GetItemSpec o2) {
				if (!Objects.equals(o1.getAttributesToGet(), o2.getAttributesToGet()))
					return -1;
				if (!o1.isConsistentRead() != o2.isConsistentRead())
					return o1.isConsistentRead().compareTo(o2.isConsistentRead());
				return keyAttributeCollectionComparator().compare(o1.getKeyComponents(), o2.getKeyComponents());
			}
		};
	}

	public static Comparator<Collection<KeyAttribute>> keyAttributeCollectionComparator() {
		return new Comparator<Collection<KeyAttribute>>() {

			@Override
			public int compare(Collection<KeyAttribute> o1, Collection<KeyAttribute> o2) {
				if (o1.size() != o2.size())
					return Integer.compare(o1.size(), o2.size());

				final Comparator<KeyAttribute> comparator = keyAttributeComparator();
				final List<KeyAttribute> list1 = new ArrayList<>(o1);
				final List<KeyAttribute> list2 = new ArrayList<>(o2);
				for (int i = 0; i < o1.size(); i++) {
					int c = comparator.compare(list1.get(i), list2.get(i));
					if (c != 0)
						return c;
				}
				return 0;
			}
		};
	}

	public static Comparator<Collection<KeyAttribute>> keyAttributeCollectionComparator(
			final Comparator<KeyAttribute> comparator) {
		return collectionComparator(keyAttributeComparator());
	}

	public static Comparator<KeyAttribute> keyAttributeComparator() {
		return new Comparator<KeyAttribute>() {

			@Override
			public int compare(KeyAttribute o1, KeyAttribute o2) {
				int i = o1.getName().compareTo(o2.getName());
				if (i != 0)
					return i;

				return Integer.compare(o1.getValue().hashCode(), o2.getValue().hashCode());
			}
		};
	}

	public static Comparator<Object> objectComparator() {
		return new Comparator<Object>() {

			@Override
			public int compare(Object o1, Object o2) {
				if (o1 == o2)
					return 0;
				if (o1 == null)
					return -1;
				if (o2 == null)
					return 1;
				if (o1.equals(o2))
					return 0;

				return Integer.compare(o1.hashCode(), o2.hashCode());
			}
		};
	}

	private static Comparator<UpdateItemSpec> updateItemSpecComparator() {
		return new Comparator<UpdateItemSpec>() {

			@Override
			public int compare(UpdateItemSpec o1, UpdateItemSpec o2) {
				int i = collectionComparator(expectedComparator()).compare(o1.getExpected(), o2.getExpected());
				if (i != 0)
					return i;

				i = keyAttributeCollectionComparator().compare(o1.getKeyComponents(), o2.getKeyComponents());
				if (i != 0)
					return i;

				i = defaultComparator(String.class).compare(o1.getReturnValues(), o2.getReturnValues());
				if (i != 0)
					return i;

				return collectionComparator(attributeUpdateComparator()).compare(o1.getAttributeUpdate(),
						o2.getAttributeUpdate());
			}
		};
	}

	private DynamoDbKeyspace keyspace;

	private Table mockTable;

	@DataProvider
	Object[][] addDoesntAllowNullsData() {
		return new Object[][] { //
				{ null, "someValue" }, //
				{ "someKey", null }, //
				{ null, null } //
		};
	}

	@Test(dataProvider = "addDoesntAllowNullsData", expectedExceptions = NullPointerException.class)
	public void addDoesntAllowNullsTest(final String key, final String value) {
		keyspace.add(key, value);
		Assert.fail("Expected exception!");
	}

	@Test
	public void addTest() {
		final Item inputItem = new Item() //
				.withPrimaryKey("ut_attr_keyspace", "ut_keyspace", "ut_attr_key", "add_first") //
				.withString("ut_attr_val", "abed") //
				.withLong("ut_attr_version", "abed".hashCode());
		final Map<String, String> inputNameMap = new HashMap<>();
		inputNameMap.put("#b", "ut_attr_keyspace");

		EasyMock.expect(mockTable.putItem(inputItem, "attribute_not_exists(#b)", inputNameMap, null))
				.andReturn(EasyMock.createMock(PutItemOutcome.class));
		EasyMock.expect(mockTable.putItem(inputItem, "attribute_not_exists(#b)", inputNameMap, null))
				.andThrow(new ConditionalCheckFailedException("Already exists yo"));

		EasyMock.replay(mockTable);

		final boolean actual = keyspace.add("add_first", "abed");
		final boolean actual2 = keyspace.add("add_first", "abed");

		EasyMock.verify(mockTable);
		Assert.assertTrue(actual);
		Assert.assertFalse(actual2);
	}

	@DataProvider
	Object[][] checkAndSetDoesntAllowNullsData() {
		return new Object[][] { //
				{ null, "someValue", 1L }, //
				{ "someKey", null, 2L }, //
				{ null, null, 3L } //
		};
	}

	@Test(dataProvider = "checkAndSetDoesntAllowNullsData", expectedExceptions = NullPointerException.class)
	public void checkAndSetDoesntAllowNullsTest(final String key, final String value, final long version) {
		keyspace.checkAndSet(key, value, version);
		Assert.fail("Expected exception!");
	}

	@Test
	public void checkAndSetTest() {
		final long oldVersion = "troy".hashCode();
		final long newVersion = "chang".hashCode();
		final UpdateItemSpec inputSpec = new UpdateItemSpec() //
				.withPrimaryKey("ut_attr_keyspace", "ut_keyspace", "ut_attr_key", "cas_first") //
				.withExpected(new Expected("ut_attr_version").eq(oldVersion)) //
				.withAttributeUpdate( //
						new AttributeUpdate("ut_attr_val").put("chang"), //
						new AttributeUpdate("ut_attr_version").put(newVersion) //
		);

		final UpdateItemOutcome mockOutcome = EasyMock.createMock(UpdateItemOutcome.class);

		mockTable.updateItem(EasyMock.cmp(inputSpec, updateItemSpecComparator(), LogicalOperator.EQUAL));
		EasyMock.expectLastCall().andReturn(mockOutcome);

		EasyMock.replay(mockTable, mockOutcome);

		final boolean actual = keyspace.checkAndSet("cas_first", "chang", oldVersion);

		EasyMock.verify(mockTable, mockOutcome);
		Assert.assertTrue(actual);
	}

	@Test
	public void checkAndSetVersionMismatchTest() {
		final long oldVersion = "troy".hashCode();
		final long newVersion = "chang".hashCode();
		final UpdateItemSpec inputSpec = new UpdateItemSpec() //
				.withPrimaryKey("ut_attr_keyspace", "ut_keyspace", "ut_attr_key", "cas_first") //
				.withExpected(new Expected("ut_attr_version").eq(oldVersion)) //
				.withAttributeUpdate( //
						new AttributeUpdate("ut_attr_val").put("chang"), //
						new AttributeUpdate("ut_attr_version").put(newVersion) //
		);

		final UpdateItemOutcome mockOutcome = EasyMock.createMock(UpdateItemOutcome.class);
		mockOutcome.getItem();
		EasyMock.expectLastCall().andReturn(null);

		mockTable.updateItem(EasyMock.cmp(inputSpec, updateItemSpecComparator(), LogicalOperator.EQUAL));
		EasyMock.expectLastCall().andThrow(new ConditionalCheckFailedException("Mismatch!"));

		EasyMock.replay(mockTable);

		final boolean actual = keyspace.checkAndSet("cas_first", "chang", oldVersion);

		EasyMock.verify(mockTable);
		Assert.assertFalse(actual);
	}

	@Test(expectedExceptions = NullPointerException.class)
	public void deleteDoesntAllowNullKeyTest() {
		keyspace.delete(null);
		Assert.fail("Expected exception!");
	}

	@Test(expectedExceptions = NullPointerException.class)
	public void deletesDoesntAllowNullKeyTest() {
		keyspace.deletes(null, 23L);
		Assert.fail("Expected exception!");
	}

	@Test
	public void deletesKeyDoesntExistTest() {
		final long version = "annie".hashCode();
		final DeleteItemSpec inputSpec = new DeleteItemSpec().withReturnValues(ReturnValue.ALL_OLD) //
				.withPrimaryKey("ut_attr_keyspace", "ut_keyspace", "ut_attr_key", "deletes_second") //
				.withExpected(new Expected("ut_attr_version").eq(version));

		final DeleteItemOutcome deletedOutcome = EasyMock.createMock(DeleteItemOutcome.class);
		deletedOutcome.getItem();
		EasyMock.expectLastCall().andReturn(null);

		mockTable.deleteItem(EasyMock.cmp(inputSpec, deleteItemSpecComparator(), LogicalOperator.EQUAL));
		EasyMock.expectLastCall().andReturn(deletedOutcome);

		EasyMock.replay(mockTable);

		final boolean deletes = keyspace.deletes("deletes_second", version);

		EasyMock.verify(mockTable);
		Assert.assertEquals(deletes, false);
	}

	@Test
	public void deletesTest() {
		final long version = "annie".hashCode();
		final DeleteItemSpec inputSpec = new DeleteItemSpec().withReturnValues(ReturnValue.ALL_OLD) //
				.withPrimaryKey("ut_attr_keyspace", "ut_keyspace", "ut_attr_key", "deletes_first") //
				.withExpected(new Expected("ut_attr_version").eq(version));

		final DeleteItemOutcome deletedOutcome = EasyMock.createMock(DeleteItemOutcome.class);
		final Item item = EasyMock.createMock(Item.class);
		EasyMock.expect(deletedOutcome.getItem()).andReturn(item);

		mockTable.deleteItem(EasyMock.cmp(inputSpec, deleteItemSpecComparator(), LogicalOperator.EQUAL));
		EasyMock.expectLastCall().andReturn(deletedOutcome);

		EasyMock.replay(mockTable, deletedOutcome);

		final boolean deletes = keyspace.deletes("deletes_first", version);

		EasyMock.verify(mockTable, deletedOutcome);
		Assert.assertEquals(deletes, true);
	}

	@Test
	public void deletesVersionDoesntMatchTest() {
		final long version = "annie".hashCode();
		final DeleteItemSpec inputSpec = new DeleteItemSpec().withReturnValues(ReturnValue.ALL_OLD) //
				.withPrimaryKey("ut_attr_keyspace", "ut_keyspace", "ut_attr_key", "deletes_first") //
				.withExpected(new Expected("ut_attr_version").eq(version));

		final DeleteItemOutcome deletedOutcome = EasyMock.createMock(DeleteItemOutcome.class);

		mockTable.deleteItem(EasyMock.cmp(inputSpec, deleteItemSpecComparator(), LogicalOperator.EQUAL));
		EasyMock.expectLastCall().andThrow(new ConditionalCheckFailedException("mismatch"));

		EasyMock.replay(mockTable, deletedOutcome);

		final boolean deletes = keyspace.deletes("deletes_first", version);

		EasyMock.verify(mockTable, deletedOutcome);
		Assert.assertEquals(deletes, false);
	}

	@Test
	public void deleteTest() {
		final DeleteItemSpec inputSpec = new DeleteItemSpec() //
				.withReturnValues(ReturnValue.ALL_OLD) //
				.withPrimaryKey( //
						"ut_attr_keyspace", "ut_keyspace", //
						"ut_attr_key", "delete_first");

		final DeleteItemOutcome deletedOutcome = EasyMock.createMock(DeleteItemOutcome.class);
		final Item item = EasyMock.createMock(Item.class);
		EasyMock.expect(deletedOutcome.getItem()).andReturn(item);

		final DeleteItemOutcome emptyOutcome = EasyMock.createMock(DeleteItemOutcome.class);
		EasyMock.expect(emptyOutcome.getItem()).andReturn(null);

		mockTable.deleteItem(EasyMock.cmp(inputSpec, deleteItemSpecComparator(), LogicalOperator.EQUAL));
		EasyMock.expectLastCall() //
				.andReturn(deletedOutcome) //
				.andReturn(emptyOutcome); //

		EasyMock.replay(mockTable, emptyOutcome, deletedOutcome);

		final boolean actual = keyspace.delete("delete_first");
		final boolean actual2 = keyspace.delete("delete_first");

		EasyMock.verify(mockTable, emptyOutcome, deletedOutcome);
		Assert.assertTrue(actual);
		Assert.assertFalse(actual2);
	}

	@BeforeMethod
	public void DynamoDbKeyspace() {
		mockTable = EasyMock.createMock(Table.class);
		keyspace = new DynamoDbKeyspace("ut_keyspace", mockTable, "ut_attr_keyspace", "ut_attr_key", "ut_attr_val",
				"ut_attr_version");
	}

	@Test(expectedExceptions = NullPointerException.class)
	public void existsDoesntAllowNullKeyTest() {
		keyspace.exists(null);
		Assert.fail("Expected exception!");
	}

	@Test
	public void existsTest() {
		final GetItemSpec getSpec = new GetItemSpec() //
				.withAttributesToGet("ut_attr_key") //
				.withPrimaryKey( //
						"ut_attr_keyspace", "ut_keyspace", //
						"ut_attr_key", "exists_first") //
				.withConsistentRead(true);

		mockTable.getItem(EasyMock.cmp(getSpec, getItemSpecComparator(), LogicalOperator.EQUAL));
		EasyMock.expectLastCall() //
				.andReturn(EasyMock.createMock(Item.class)) //
				.andReturn(null); //

		EasyMock.replay(mockTable);

		final boolean actual = keyspace.exists("exists_first");
		final boolean actual2 = keyspace.exists("exists_first");

		EasyMock.verify(mockTable);
		Assert.assertTrue(actual);
		Assert.assertFalse(actual2);
	}

	@Test(expectedExceptions = NullPointerException.class)
	public void getDoesntAllowNullKeyTest() {
		keyspace.get(null);
		Assert.fail("Expected exception!");
	}

	@Test(expectedExceptions = NullPointerException.class)
	public void getsDoesntAllowNullKeyTest() {
		keyspace.gets(null);
		Assert.fail("Expected exception!");
	}

	@Test
	public void getsNoKeyTest() {
		final GetItemSpec getSpec = new GetItemSpec() //
				.withPrimaryKey( //
						"ut_attr_keyspace", "ut_keyspace", //
						"ut_attr_key", "gets_NONE") //
				.withConsistentRead(true);

		mockTable.getItem(EasyMock.cmp(getSpec, getItemSpecComparator(), LogicalOperator.EQUAL));
		EasyMock.expectLastCall().andReturn(null);

		EasyMock.replay(mockTable);

		final Optional<KeyValue> actual = keyspace.gets("gets_NONE");

		EasyMock.verify(mockTable);
		Assert.assertFalse(actual.isPresent());
	}

	@Test
	public void getsTest() {
		final GetItemSpec getSpec = new GetItemSpec() //
				.withPrimaryKey( //
						"ut_attr_keyspace", "ut_keyspace", //
						"ut_attr_key", "gets_first") //
				.withConsistentRead(true);

		final Item mockItem = EasyMock.createMock(Item.class);
		mockItem.getString("ut_attr_val");
		EasyMock.expectLastCall().andReturn("shirley");
		mockItem.getLong("ut_attr_version");
		EasyMock.expectLastCall().andReturn("shirley".hashCode());

		mockTable.getItem(EasyMock.cmp(getSpec, getItemSpecComparator(), LogicalOperator.EQUAL));
		EasyMock.expectLastCall().andReturn(mockItem);

		EasyMock.replay(mockTable, mockItem);

		final Optional<KeyValue> actual = keyspace.gets("gets_first");

		EasyMock.verify(mockTable, mockItem);
		Assert.assertEquals(actual.get().getKey(), "gets_first");
		Assert.assertEquals(actual.get().getValue(), "shirley");
		Assert.assertEquals(actual.get().getVersion(), "shirley".hashCode());
	}

	@Test
	public void getTest() {
		final GetItemSpec getSpec = new GetItemSpec() //
				.withPrimaryKey( //
						"ut_attr_keyspace", "ut_keyspace", //
						"ut_attr_key", "get_first") //
				.withConsistentRead(true);

		final Item mockItem = EasyMock.createMock(Item.class);
		mockItem.getString("ut_attr_val");
		EasyMock.expectLastCall().andReturn("pierce");

		mockTable.getItem(EasyMock.cmp(getSpec, getItemSpecComparator(), LogicalOperator.EQUAL));
		EasyMock.expectLastCall() //
				.andReturn(mockItem) //
				.andReturn(null); //

		EasyMock.replay(mockTable, mockItem);

		final Optional<String> actual = keyspace.get("get_first");
		final Optional<String> actual2 = keyspace.get("get_first");

		EasyMock.verify(mockTable, mockItem);
		Assert.assertEquals(actual, Optional.of("pierce"));
		Assert.assertFalse(actual2.isPresent());
	}

	@DataProvider
	Object[][] replaceDoesntAllowNullsData() {
		return new Object[][] { //
				{ null, "someValue" }, //
				{ "someKey", null }, //
				{ null, null } //
		};
	}

	@Test(dataProvider = "replaceDoesntAllowNullsData", expectedExceptions = NullPointerException.class)
	public void replaceDoesntAllowNullsTest(final String key, final String value) {
		keyspace.replace(key, value);
		Assert.fail("Expected exception!");
	}

	@Test
	public void replaceTest() {
		final long newVersion = "jeff".hashCode();

		final UpdateItemSpec inputSpec = new UpdateItemSpec() //
				.withReturnValues(ReturnValue.ALL_OLD) //
				.withPrimaryKey("ut_attr_keyspace", "ut_keyspace", "ut_attr_key", "replace_first") //
				.withAttributeUpdate( //
						new AttributeUpdate("ut_attr_val").put("jeff"), //
						new AttributeUpdate("ut_attr_version").put(newVersion) //
				) //
				.withExpected(new Expected("ut_attr_key").exists());

		final UpdateItemOutcome mockOutcome = EasyMock.createMock(UpdateItemOutcome.class);
		final Item mockItem = EasyMock.createMock(Item.class);
		EasyMock.expect(mockOutcome.getItem()).andReturn(mockItem).times(2);
		EasyMock.expect(mockItem.get("ut_attr_val")) //
				.andReturn("dean") //
				.andReturn("jeff");

		EasyMock.expect(
				mockTable.updateItem(EasyMock.cmp(inputSpec, updateItemSpecComparator(), LogicalOperator.EQUAL))) //
				.andReturn(mockOutcome).times(2) //
				.andThrow(new ConditionalCheckFailedException("Doesn't exist"));

		EasyMock.replay(mockTable, mockOutcome, mockItem);

		final boolean actual = keyspace.replace("replace_first", "jeff");
		final boolean actual2 = keyspace.replace("replace_first", "jeff");
		final boolean actual3 = keyspace.replace("replace_first", "jeff");

		EasyMock.verify(mockTable, mockOutcome, mockItem);
		Assert.assertTrue(actual);
		Assert.assertFalse(actual2);
		Assert.assertFalse(actual3);
	}

	@DataProvider
	Object[][] setDoesntAllowNullsData() {
		return new Object[][] { //
				{ null, "someValue" }, //
				{ "someKey", null }, //
				{ null, null } //
		};
	}

	@Test(dataProvider = "setDoesntAllowNullsData", expectedExceptions = NullPointerException.class)
	public void setDoesntAllowNullsTest(final String key, final String value) {
		keyspace.set(key, value);
		Assert.fail("Expected exception!");
	}

	@Test
	public void setTest() {
		final Item inputItem = new Item() //
				.withPrimaryKey("ut_attr_keyspace", "ut_keyspace", "ut_attr_key", "set_first") //
				.withString("ut_attr_val", "britta") //
				.withLong("ut_attr_version", "britta".hashCode());

		EasyMock.expect(mockTable.putItem(inputItem)).andReturn(EasyMock.createMock(PutItemOutcome.class));

		EasyMock.replay(mockTable);

		final boolean actual = keyspace.set("set_first", "britta");

		EasyMock.verify(mockTable);
		Assert.assertTrue(actual);
	}
}
