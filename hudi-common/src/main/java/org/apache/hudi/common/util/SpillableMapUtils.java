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

package org.apache.hudi.common.util;

import org.apache.hudi.common.fs.SizeAwareDataOutputStream;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.util.collection.BitCaskDiskMap.FileEntry;
import org.apache.hudi.exception.HoodieCorruptedDataException;

import org.apache.avro.generic.GenericRecord;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.zip.CRC32;

/**
 * A utility class supports spillable map.
 */
public class SpillableMapUtils {

  /**
   * Using the schema and payload class, read and convert the bytes on disk to a HoodieRecord.
   */
  public static byte[] readBytesFromDisk(RandomAccessFile file, long valuePosition, int valueLength)
      throws IOException {
    FileEntry fileEntry = readInternal(file, valuePosition, valueLength);
    return fileEntry.getValue();
  }

  /**
   * Reads the given file with specific pattern(|crc|timestamp|sizeOfKey|SizeOfValue|key|value|) then
   * returns an instance of {@link FileEntry}.
   */
  private static FileEntry readInternal(RandomAccessFile file, long valuePosition, int valueLength) throws IOException {
    file.seek(valuePosition);
    long crc = file.readLong();
    long timestamp = file.readLong();
    int keySize = file.readInt();
    int valueSize = file.readInt();
    byte[] key = new byte[keySize];
    file.readFully(key, 0, keySize);
    byte[] value = new byte[valueSize];
    if (valueSize != valueLength) {
      throw new HoodieCorruptedDataException("unequal size of payload written to external file, data may be corrupted");
    }
    file.readFully(value, 0, valueSize);
    long crcOfReadValue = generateChecksum(value);
    if (crc != crcOfReadValue) {
      throw new HoodieCorruptedDataException(
          "checksum of payload written to external disk does not match, data may be corrupted");
    }
    return new FileEntry(crc, keySize, valueSize, key, value, timestamp);
  }

  /**
   * Write Value and other metadata necessary to disk. Each entry has the following sequence of data
   * <p>
   * |crc|timestamp|sizeOfKey|SizeOfValue|key|value|
   */
  public static long spillToDisk(SizeAwareDataOutputStream outputStream, FileEntry fileEntry) throws IOException {
    return spill(outputStream, fileEntry);
  }

  private static long spill(SizeAwareDataOutputStream outputStream, FileEntry fileEntry) throws IOException {
    outputStream.writeLong(fileEntry.getCrc());
    outputStream.writeLong(fileEntry.getTimestamp());
    outputStream.writeInt(fileEntry.getSizeOfKey());
    outputStream.writeInt(fileEntry.getSizeOfValue());
    outputStream.write(fileEntry.getKey());
    outputStream.write(fileEntry.getValue());
    return outputStream.getSize();
  }

  /**
   * Generate a checksum for a given set of bytes.
   */
  public static long generateChecksum(byte[] data) {
    CRC32 crc = new CRC32();
    crc.update(data);
    return crc.getValue();
  }

  /**
   * Compute a bytes representation of the payload by serializing the contents This is used to estimate the size of the
   * payload (either in memory or when written to disk).
   */
  public static <R> long computePayloadSize(R value, SizeEstimator<R> valueSizeEstimator) throws IOException {
    return valueSizeEstimator.sizeEstimate(value);
  }

  /**
   * Utility method to convert bytes to HoodieRecord using schema and payload class.
   */
  public static <R> R convertToHoodieRecordPayload(GenericRecord rec, String payloadClazz, String preCombineField) {
    String recKey = rec.get(HoodieRecord.RECORD_KEY_METADATA_FIELD).toString();
    String partitionPath = rec.get(HoodieRecord.PARTITION_PATH_METADATA_FIELD).toString();
    Comparable preCombineFieldVal = preCombineField != null ? (Comparable) rec.get(preCombineField) : null;
    HoodieRecord<? extends HoodieRecordPayload> hoodieRecord = new HoodieRecord<>(new HoodieKey(recKey, partitionPath),
        preCombineFieldVal == null ? ReflectionUtils.loadPayload(payloadClazz, new Object[] {Option.of(rec)}, Option.class) :
            ReflectionUtils.loadPayload(payloadClazz, new Object[] {rec, preCombineFieldVal}, GenericRecord.class, Comparable.class));
    return (R) hoodieRecord;
  }

  /**
   * Utility method to convert bytes to HoodieRecord using schema and payload class.
   */
  public static <R> R generateEmptyPayload(String recKey, String partitionPath, String payloadClazz) {
    HoodieRecord<? extends HoodieRecordPayload> hoodieRecord = new HoodieRecord<>(new HoodieKey(recKey, partitionPath),
        ReflectionUtils.loadPayload(payloadClazz, new Object[] {Option.empty()}, Option.class));
    return (R) hoodieRecord;
  }
}
