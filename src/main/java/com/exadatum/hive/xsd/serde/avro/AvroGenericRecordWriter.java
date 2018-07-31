/*
 * Copyright 2011 LinkedIn
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.exadatum.hive.xsd.serde.avro;

import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.io.Writable;

import java.io.IOException;

/**
 * Write an Avro GenericRecord to an Avro data file.
 */
public class AvroGenericRecordWriter implements FileSinkOperator.RecordWriter{
  final private DataFileWriter<GenericRecord> dfw;

  public AvroGenericRecordWriter(DataFileWriter<GenericRecord> dfw) {
    this.dfw = dfw;
  }

  @Override
  public void write(Writable writable) throws IOException {
    if(!(writable instanceof AvroGenericRecordWritable))
      throw new IOException("Expecting instance of AvroGenericRecordWritable, but received" + writable.getClass().getCanonicalName());
    AvroGenericRecordWritable r = (AvroGenericRecordWritable)writable;
    dfw.append(r.getRecord());
  }

  @Override
  public void close(boolean abort) throws IOException {
    dfw.close();
  }

}
