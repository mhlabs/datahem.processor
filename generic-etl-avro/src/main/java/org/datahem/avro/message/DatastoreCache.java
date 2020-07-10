package org.datahem.avro.message;

/*-
 * ========================LICENSE_START=================================
 * DataHem
 * %%
 * Copyright (C) 2018 Robert Sahlin and MatHem Sverige AB
 * %%
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 * =========================LICENSE_END==================================
 */

import com.google.cloud.datastore.Datastore;
import com.google.cloud.datastore.DatastoreOptions;
import com.google.cloud.datastore.Entity;
import com.google.cloud.datastore.Key;
import com.google.common.collect.MapMaker;
import org.apache.avro.Schema;
import org.apache.avro.SchemaNormalization;
import org.apache.avro.message.SchemaStore;

import java.io.Serializable;
import java.util.Map;

public class DatastoreCache implements SchemaStore, Serializable {
    //private final Map<Long, Schema> schemas = new MapMaker().makeMap();
    private final Map<Long, String> schemas = new MapMaker().makeMap();

    /**
     * Adds a schema to this cache that can be retrieved using its AVRO-CRC-64
     * fingerprint.
     *
     * @param schema a {@link Schema}
     */
    public void addSchema(Schema schema) {
        long fingerprint = SchemaNormalization.parsingFingerprint64(schema);
        System.out.println("SchemaStore addSchema: " + Long.toString(fingerprint) + " : " + schema.toString());
        //schemas.put(fingerprint, schema);
        schemas.put(fingerprint, schema.toString());
        addSchemaToDatastore(fingerprint, schema);
    }

    @Override
    public Schema findByFingerprint(long fingerprint) {
        //System.out.println("SchemaStore findByFingerprint: " + Long.toString(fingerprint) + " : " + schemas.get(fingerprint).toString());
        if (schemas.get(fingerprint) != null) {
            return new Schema.Parser().parse(schemas.get(fingerprint));
        }
        Schema schema = getSchemaFromDatastore(fingerprint);
        if (schema != null) {
            //schemas.put(fingerprint, schema);
            schemas.put(fingerprint, schema.toString());
            return schema;
        }
        return null;
    }

    public void addSchemaToDatastore(long fingerprint, Schema s) {
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
