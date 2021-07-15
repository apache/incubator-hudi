/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.common.table.log;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.util.SpillableMapUtils;
import org.apache.hudi.io.storage.HoodieFileReader;

import java.io.IOException;
import java.util.Iterator;

/**
 * Reads records from base file and merges any updates from log files and provides iterable over all records in the file slice.
 */
public class HoodieFileSliceReader implements Iterator<HoodieRecord<? extends HoodieRecordPayload>> {
  private Iterator<HoodieRecord<? extends HoodieRecordPayload>> recordsIterator;

  public static <R extends IndexedRecord, T extends HoodieRecordPayload> HoodieFileSliceReader getFileSliceReader(
      HoodieFileReader<R> baseFileReader, HoodieMergedLogRecordScanner scanner, Schema schema, String payloadClass, String preCombineField) throws IOException {
    Iterator<R> baseIterator = baseFileReader.getRecordIterator(schema);
    while (baseIterator.hasNext()) {
      GenericRecord record = (GenericRecord)  baseIterator.next();
      HoodieRecord<T> hoodieRecord = SpillableMapUtils.convertToHoodieRecordPayload(record, payloadClass, preCombineField);
      scanner.processNextRecord(hoodieRecord);
    }
    return new HoodieFileSliceReader(scanner.iterator());
  }

  private HoodieFileSliceReader(Iterator<HoodieRecord<? extends HoodieRecordPayload>> recordsItr) {
    this.recordsIterator = recordsItr;
  }

  @Override
  public boolean hasNext() {
    return recordsIterator.hasNext();
  }

  @Override
  public HoodieRecord<? extends HoodieRecordPayload> next() {
    return recordsIterator.next();
  }
}
