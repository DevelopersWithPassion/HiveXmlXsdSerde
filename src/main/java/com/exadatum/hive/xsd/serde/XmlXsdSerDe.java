package com.exadatum.hive.xsd.serde;

/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


import org.apache.hadoop.hive.serde2.avro.*;
import com.exadatum.hive.xsd.serde.deserializer.AvroDeserializer;
import com.exadatum.hive.xsd.serde.converter.DatumBuilder;
import com.exadatum.hive.xsd.serde.converter.SchemaBuilder;
import com.exadatum.hive.xsd.serde.processor.XSDParser;
import com.exadatum.hive.xsd.serde.readerwriter.XmlInputFormat;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.log4j.Logger;
import org.apache.xmlbeans.XmlException;

import java.io.File;
import java.util.*;

public class XmlXsdSerDe implements SerDe {

    private static final Logger LOGGER = Logger.getLogger(XmlXsdSerDe.class);
    private static final String XSD_FILE_LOCATION = "schema.file.location";
    private ObjectInspector objectInspector = null;
    private static final String LIST_COLUMNS = "columns";
    private static final String LIST_COLUMN_TYPES = "columns.types";
    private static List<String> columnNamesFromXml = new ArrayList<>();
    private static List<TypeInfo> columnTypesFromXml = new ArrayList<>();
    private Schema schema = null;

    private AvroDeserializer avroDeserializer = null;

    /**
     * @see org.apache.hadoop.hive.serde2.Deserializer#initialize(Configuration, Properties)
     */
    @Override
    public void initialize(Configuration configuration, final Properties properties) throws SerDeException {

        String filePath = properties.getProperty(XSD_FILE_LOCATION);
        LOGGER.info("Schema file location " + filePath);

        if (filePath == null) {
            LOGGER.error("File path not set . please chheck if file exist at" + filePath);

            throw new SerDeException("Not ble to read file. file path may be missing " + filePath);
        }
        XSDParser.setRootElement(new File(filePath));
        setColumnListAndType(filePath);

        properties.setProperty("xmlinput.start", XmlInputFormat.startTag);
        properties.setProperty("xmlinput.end", XmlInputFormat.endTag);
        LOGGER.info("Start Tag " + XmlInputFormat.startTag + " End Tag " + XmlInputFormat.endTag);

        initialize(configuration, properties, XmlInputFormat.START_TAG_KEY, XmlInputFormat.END_TAG_KEY);

        List<String> columnNames;
        if (columnNamesFromXml.size() <= 0)
            columnNames = Arrays.asList(properties.getProperty(LIST_COLUMNS).split("[,:;]"));
        else
            columnNames = columnNamesFromXml;

        LOGGER.info("schema properties " + properties.stringPropertyNames());
        LOGGER.info("xml schema columns config" + properties.getProperty(LIST_COLUMNS));
        LOGGER.info("xml schema column data types " + columnTypesFromXml.toString());
        LOGGER.info("xml schema columns " + columnNames);
        LOGGER.info("xml schema column data types config " + properties.getProperty(LIST_COLUMN_TYPES));

    }

    private void setColumnListAndType(String filePath) throws SerDeException {
        SchemaBuilder schemaBuilder = new SchemaBuilder();
        Schema schema = schemaBuilder.createSchema(new File(filePath));
        if (schema.getType() == Schema.Type.ARRAY)
            schema = schema.getElementType();
        this.schema = schema;
        AvroObjectInspectorGenerator avroObjectInspectorGenerator = new AvroObjectInspectorGenerator(schema);
        columnNamesFromXml = avroObjectInspectorGenerator.getColumnNames();
        columnTypesFromXml = avroObjectInspectorGenerator.getColumnTypes();
        this.objectInspector = avroObjectInspectorGenerator.getObjectInspector();
    }


    private static void initialize(Configuration configuration, final Properties properties, String... keys) {
        for (String key : keys) {
            String configurationValue = configuration.get(key);
            String propertyValue = properties.getProperty(key);
            if (configurationValue == null) {
                if (propertyValue != null) {
                    configuration.set(key, propertyValue);
                }
            } else {
                if (propertyValue != null && !propertyValue.equals(configurationValue)) {
                    configuration.set(key, propertyValue);
                }
            }
        }
    }


    @Override
    public Object deserialize(Writable writable) throws SerDeException {
        Text text = (Text) writable;
        DatumBuilder datumBuilder = new DatumBuilder(schema);
        Object datum = datumBuilder.createDatum(text.toString());

        if (!(datum instanceof GenericRecord))
            try {
                throw new XmlException("record not compatible with schema");
            } catch (XmlException e) {
                e.printStackTrace();
            }
        GenericRecord avroRecord = (GenericRecord) datum;
        AvroGenericRecordWritable avroGenericRecordWritable = new AvroGenericRecordWritable(avroRecord);
        avroGenericRecordWritable.setFileSchema(schema);
        return getDeserializer().deserialize(columnNamesFromXml, columnTypesFromXml, avroGenericRecordWritable, schema);

    }

    private AvroDeserializer getDeserializer() {
        if (avroDeserializer == null) avroDeserializer = new AvroDeserializer();

        return avroDeserializer;
    }


    @Override
    public ObjectInspector getObjectInspector() throws SerDeException {
        return this.objectInspector;
    }

    @Override
    public SerDeStats getSerDeStats() {
        return null;
    }

    @Override
    public Class<? extends Writable> getSerializedClass() {
        return Text.class;
    }


    @Override
    public Writable serialize(Object object, ObjectInspector objectInspector) throws SerDeException {
        throw new UnsupportedOperationException();
    }


}
