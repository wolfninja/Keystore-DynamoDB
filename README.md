# KeyStore-DynamoDB
DynamoDB client implementation of the [WolfNinja KeyStore abstraction API](http://github.com/wolfninja/KeyStore)

*Master branch:*
![Travis CI Build](https://img.shields.io/travis/wolfninja/KeyStore-DynamoDB.svg)
[![Code Coverage](https://img.shields.io/codecov/c/github/wolfninja/KeyStore-DynamoDB.svg)](https://codecov.io/github/wolfninja/KeyStore-DynamoDB)

*Develop branch:*
![Travis CI Build](https://img.shields.io/travis/wolfninja/KeyStore-DynamoDB/develop.svg)
[![Code Coverage](https://img.shields.io/codecov/c/github/wolfninja/KeyStore-DynamoDB/develop.svg)](https://codecov.io/github/wolfninja/KeyStore-DynamoDB?branch=develop)

## Description
DynamoDB implementation of the KeyStore API. This implementation uses the [Amazon AWS SDK Java](https://github.com/aws/aws-sdk-java) library.

## Latest version
The most recent release is KeyStore-dynamodb 0.1.0, released 2016-04-25. Targets [KeyStore API 0.1.0](https://github.com/wolfninja/KeyStore/tree/v0.1.0)

### Maven Central
Releases are available via Maven Central: [com.wolfninja.keystore:keystore-dynamodb:0.1.0](http://search.maven.org/#artifactdetails|com.wolfninja.keystore|keystore-dynamodb|0.1.0|bundle)

* Group ID: com.wolfninja.keystore
* Artifact ID: keystore-dynamodb

### Gradle

```
dependencies {
  compile 'com.wolfninja.keystore:keystore-dynamodb:0.1.0'
}
```

### Maven
```xml
    <dependency>
      <groupId>com.wolfninja.keystore</groupId>
      <artifactId>keystore-dynamodb</artifactId>
      <version>0.1.0</version>
    </dependency>
```

## Usage Example
```java
		// Add creds
		final AWSCredentials awsCredentials = new BasicAWSCredentials("AWS_KEY",
				"AWS_SECRET");

		// Create client, set region
		final AmazonDynamoDBClient client = new AmazonDynamoDBClient(awsCredentials);
		client.setRegion(Region.getRegion(Regions.US_WEST_1));

		// Create DynamoDB object
		final DynamoDB dynamoDB = new DynamoDB(client);

		// Create a new table
		final String tableName = "keystore";
		final Table table = DynamoDbAdapter.loadTable(dynamoDB, tableName)
				.orElseGet(() -> DynamoDbAdapter.createNewTable(dynamoDB, tableName));

		// New Store instance
		final DynamoDbAdapter adapter = DynamoDbAdapter.create(table);
		final KeyValueStore kvs = KeyValueStore.create(adapter);
		// Get keyspace
		final Keyspace keyspace = kvs.getKeyspace("main");

		// Add a KV pair to the keyspace
		{
			assert keyspace.set("testKey", "Hiya buddy");
			assert keyspace.set("otherKey", "Stored in a different key");
		}

		// Verify stored in DynamoDB
		{
			final Item item = table.getItem(new PrimaryKey(DynamoDbAdapter.DEFAULT_ATTRIBUTE_KEYSPACE, "main",
					DynamoDbAdapter.DEFAULT_ATTRIBUTE_KEY, "testKey"));
			assert item != null;

			final String value = item.getString(DynamoDbAdapter.DEFAULT_ATTRIBUTE_VALUE);
			assert value.equals("Hiya buddy");
		}

		// Replace value via a check and set
		{
			// Use gets to get value with version info
			final Optional<KeyValue> kv = keyspace.gets("otherKey");
			assert kv.isPresent();

			// Do cas operation using retrieved version
			assert keyspace.checkAndSet("otherKey", "Replaced value", kv.get().getVersion());
		}

		// Verify replaced version
		{
			final Item item = table.getItem(new PrimaryKey(DynamoDbAdapter.DEFAULT_ATTRIBUTE_KEYSPACE, "main",
					DynamoDbAdapter.DEFAULT_ATTRIBUTE_KEY, "otherKey"));
			assert item != null;

			final String value = item.getString(DynamoDbAdapter.DEFAULT_ATTRIBUTE_VALUE);
			assert value.equals("Replaced value");
		}
	}
```

## Configuration
- Requires passing in a configured AWS SDK _Table_ instance (see Usage example)

## Limitations
- API: This implementation supports the full API featureset
- Implementation:
  - Currently, all reads are configured to be Strongly Consistent

## Implementation Details
- Each Adapter instance corresponds to a separate DynamoDB table
- DynamoDB table attributes:
  - By default, we expect a table with the following 4 attributes:
    - keyspace (Partition key, String)
    - key (Sort key, String)
    - value (String)
    - version (Numeric)
  - Attribute names can be customized by specfying them in the overloaded _KeyspaceAdapter.create()_ method


## Versioning
- This project uses [Semantic Versioning](http://semver.org/) to make release versions predictable
- Versions consist of MAJOR.MINOR.PATCH
  - Different MAJOR versions are not guaranteed to be API compatible
  - Incrementing MINOR versions within the same MAJOR version contain additional functionality, with existing calls being compatible
  - Different PATCH versions withing the same MAJOR.MINOR version are completely API compatible
- MAJOR and MINOR versions are compatible with the same API version

## Branches
- *master* is the "stable" branch from which releases are built
- *develop* branch is used for active development, and merged into *master* at release time


## Changelog
See CHANGELOG.md for full history of release changes

## License
Licenced under the MIT License (see LICENSE.txt)
