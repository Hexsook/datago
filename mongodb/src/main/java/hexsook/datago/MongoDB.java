package hexsook.datago;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;
import hexsook.originext.HostPort;
import hexsook.originext.object.ObjectUtil;
import hexsook.originext.object.Strings;
import org.bson.Document;
import org.bson.UuidRepresentation;
import org.bson.conversions.Bson;

import java.util.*;
import java.util.regex.Pattern;

public class MongoDB {

    /**
     * Finds all documents that match the filter criteria.
     */
    public static FindIterable<Document> findDocument(MongoCollection<Document> collection, Document filter) {
        List<Bson> filters = new ArrayList<>();
        filter.keySet().forEach(key -> {
            Object value = filter.get(key);
            if (ObjectUtil.isObject(value, String.class)) {
                filters.add(Filters.regex(key, Pattern.compile("^" + Pattern.quote((String) value) + "$",
                        Pattern.CASE_INSENSITIVE)));
            } else {
                filters.add(Filters.eq(key, value));
            }
        });
        return collection.find(Filters.and(filters));
    }

    /**
     * Finds document by primary key.
     * @see MongoDB#findDocument(MongoCollection, Document)
     */
    public static Document findDocumentPrimary(MongoCollection<Document> collection, Object value) {
        return findDocument(collection, new Document("_id", value)).first();
    }

    /**
     * Finds out if there is a document that meets the filter conditions
     * @see MongoDB#findDocument(MongoCollection, Document)
     */
    public static boolean hasDocument(MongoCollection<Document> collection, Document query) {
        return findDocument(collection, query).first() != null;
    }

    /**
     * Finds document by primary key.
     * @see MongoDB#findDocumentPrimary(MongoCollection, Object)
     */
    public static boolean hasDocumentPrimary(MongoCollection<Document> collection, Object value) {
        return findDocumentPrimary(collection, value) != null;
    }

    /**
     * Updates the contents of all documents that match the filter criteria
     */
    public static void update(MongoCollection<Document> collection, Document filter, Document update) {
        Document updateCommand = new Document("$set", update);
        for (Document document : findDocument(collection, filter)) {
            collection.updateOne(document, updateCommand);
        }
    }

    /**
     * Updates document by primary key.
     */
    public static void updatePrimary(MongoCollection<Document> coll, Object value, Document update) {
        update(coll, new Document("_id", value), update);
    }

    /**
     * Inserts single document.
     */
    public static void insert(MongoCollection<Document> collection, Document doc) {
        collection.insertOne(doc);
    }

    /**
     * Inserts multiple documents.
     */
    public static void insert(MongoCollection<Document> collection, Document... docs) {
        collection.insertMany(Arrays.asList(docs));
    }

    /**
     * If a document with the same primary key does not exist in the collection, insert the document.
     * If it does exist, handle the differences.
     *
     * @see MongoDB#insert(MongoCollection, Document)
     * @see MongoDB#updatePrimary(MongoCollection, Object, Document)
     */
    public static void insertIfAbsent(MongoCollection<Document> collection, Document doc) {
        Document existingDoc = findDocumentPrimary(collection, doc.get("_id"));

        if (existingDoc == null) {
            insert(collection, doc);
        } else {
            for (String key : new ArrayList<>(doc.keySet())) {
                if (!existingDoc.containsKey(key)) {
                    updatePrimary(collection, doc.get("_id"), new Document(key, doc.get(key)));
                }
            }
            for (String key : new ArrayList<>(existingDoc.keySet())) {
                if (!doc.containsKey(key)) {
                    existingDoc.remove(key);
                }
            }

            updatePrimary(collection, doc.get("_id"), existingDoc);
        }
    }

    /**
     * Deletes all documents that match the filter criteria.
     */
    public static void delete(MongoCollection<Document> collection, Document filter) {
        if (!hasDocument(collection, filter)) {
            return;
        }
        for (Document document : findDocument(collection, filter)) {
            collection.deleteOne(document);
        }
    }

    /**
     * Deletes document by primary key.
     */
    public static void deletePrimary(MongoCollection<Document> collection, Object value) {
        delete(collection, new Document("_id", value));
    }

    /**
     * Replaces all documents matching the filter criteria with.
     */
    public static void replace(MongoCollection<Document> collection, Document filter, Document replacement) {
        for (Document document : findDocument(collection, filter)) {
            collection.replaceOne(document, replacement);
        }
    }

    /**
     * Replaces document by primary key.
     */
    public static void replacePrimary(MongoCollection<Document> collection, Object value, Document replacement) {
        replace(collection, new Document("_id", value), replacement);
    }

    /**
     * Serializes Document to Map.
     */
    public static Map<String, Object> serializeDocument(Document doc) {
        Map<String, Object> serialized = new HashMap<>();
        for (String key : doc.keySet()) {
            serialized.put(key, doc.get(key));
        }
        return serialized;
    }

    /**
     * Lists the values of a specified field in all documents in a collection.
     */
    public static <T> List<T> getFieldValues(MongoCollection<Document> collection, String field, Class<T> clazz) {
        FindIterable<Document> documents = collection.find().projection(new Document(field, 1));
        List<T> valueList = new ArrayList<>();
        for (Document document : documents) {
            valueList.add(document.get(field, clazz));
        }
        return valueList;
    }

    /**
     * Checks if any Document in the collection matches the filter criteria.
     * If not, throws the exception.
     */
    public static void checkDataNotNull(MongoCollection<Document> collection, String key, Object value) {
        if (hasDocument(collection, new Document(key, value))) {
            return;
        }
        throw new IllegalArgumentException("data (" + key + "=" + value + ") not found in collection "
                + collection.getNamespace().getCollectionName());
    }

    /**
     * Checks if any Document in the collection matches the primary key.
     * If not, throws the exception.
     */
    public static void checkDataNotNullPrimary(MongoCollection<Document> collection, Object value) {
        if (hasDocument(collection, new Document("_id", value))) {
            return;
        }
        throw new IllegalArgumentException("data (_id=" + value + ") not found in collection '"
                + collection.getNamespace().getCollectionName() + "'");
    }

    private final String address;
    private final String username;
    private final String password;
    private final AuthMechanism authMechanism;

    public MongoDB(String address, String username, String password, AuthMechanism authMechanism) {
        this.address = address;
        this.username = username;
        this.password = password;
        this.authMechanism = authMechanism;
    }

    private MongoClient client;

    public MongoClient getClient() {
        return client;
    }

    public void connect() {
        if (client != null) {
            throw new IllegalStateException("connection already opened");
        }

        HostPort address = HostPort.fromString(this.address);
        String host = address.getHost();
        int port = address.getPortOrDefault(27017);

        String url = "mongodb://" + (Strings.isNullOrWhite(password) ?
                host + ":" + port :
                username + ":" + password + "@" + host + ":" + port + "/?authMechanism="
                        + authMechanism.convert()) + "/";

        ConnectionString connection = new ConnectionString(url);
        MongoClientSettings settings = MongoClientSettings.builder()
                .applyConnectionString(connection)
                .uuidRepresentation(UuidRepresentation.STANDARD)
                .build();
        client = MongoClients.create(settings);
    }

    public boolean isConnected() {
        if (client == null) {
            return false;
        }

        try {
            getClient().getDatabase("local").runCommand(new Document("ping", 0));
        } catch (Exception e) {
            return false;
        }
        return true;
    }

    public void disconnect() {
        if (client == null) {
            throw new IllegalStateException("not connected yet");
        }

        client.close();
        client = null;
    }

    public enum AuthMechanism {
        DEFAULT("Default"),
        SCRAM_SHA_1("SCRAM-SHA-1"),
        SCRAM_SHA_256("SCRAM-SHA-256");

        private final String internal;

        AuthMechanism(String internal) {
            this.internal = internal;
        }

        public String convert() {
            return internal;
        }
    }
}