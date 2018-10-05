package org.datahem.avro.message;

import org.apache.avro.message.SchemaStore;
import com.google.common.collect.MapMaker;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.SchemaNormalization;

import com.google.cloud.datastore.Datastore;
import com.google.cloud.datastore.DatastoreOptions;
import com.google.cloud.datastore.Entity;
import com.google.cloud.datastore.Key;

public class DatastoreCache implements SchemaStore {
    private final Map<Long, Schema> schemas = new MapMaker().makeMap();

    /**
     * Adds a schema to this cache that can be retrieved using its AVRO-CRC-64
     * fingerprint.
     *
     * @param schema a {@link Schema}
     */
    public void addSchema(Schema schema) {
      long fingerprint = SchemaNormalization.parsingFingerprint64(schema);
      System.out.println("SchemaStore addSchema: " + Long.toString(fingerprint) + " : " + schema.toString());
      schemas.put(fingerprint, schema);
      addSchemaToDatastore(fingerprint, schema);
    }

	@Override
	public Schema findByFingerprint(long fingerprint) {
      //System.out.println("SchemaStore findByFingerprint: " + Long.toString(fingerprint) + " : " + schemas.get(fingerprint).toString());
		if (schemas.get(fingerprint) != null) {
			return schemas.get(fingerprint);
		}
		Schema schema = getSchemaFromDatastore(fingerprint);
		if (schema != null) {
			schemas.put(fingerprint, schema);
			return schema;
		}
		return null;
	}

	public void addSchemaToDatastore(long fingerprint, Schema s){
		Datastore datastore = DatastoreOptions.getDefaultInstance().getService();
		Key schemaKey = datastore.newKeyFactory().setKind("Schema").newKey(Long.toString(fingerprint));
		Entity schema = Entity.newBuilder(schemaKey)
			.set("fingerprint", Long.toString(fingerprint))
			.set("json", s.toString())
			.build();
		datastore.put(schema);
	}
	
	public Schema getSchemaFromDatastore(long fingerprint) {
		//System.out.println("SchemaStore findByFingerprint: " + Long.toString(fingerprint) + " : " + schemas.get(fingerprint).toString());
		Datastore datastore = DatastoreOptions.getDefaultInstance().getService();
		Key schemaKey = datastore.newKeyFactory().setKind("Schema").newKey(Long.toString(fingerprint));
		Entity schema = datastore.get(schemaKey);
		return new Schema.Parser().parse(schema.getString("json"));
    }
	
	
	
}