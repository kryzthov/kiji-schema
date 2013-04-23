/**
 * (c) Copyright 2013 WibiData, Inc.
 *
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.kiji.schema.filter.hbase;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import com.google.common.base.Preconditions;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.filter.FilterBase;

/**
 * HBase column filter to filter KeyValues in a specified timestamp range.
 *
 * <p>
 *   This filter is intended to be combined with other filters (eg. a column selector).
 *   For example:
 *     AND(column=family/qualifier, TimeStampRange=[min, max))
 * <p>
 */
public class TimestampRangeFilter extends FilterBase {

  /** Max (most recent) timestamp to include. */
  private long mMaxTimestamp;

  /** Min (least recent) timestamp to include. */
  private long mMinTimestamp;

  /** Required to implement Writable. Do not use otherwise. */
  public TimestampRangeFilter() {
  }

  /**
   * Initializes a column filter with the specified timestamp range restriction.
   *
   * @param minTimestamp Minimum timestamp to return (inclusive).
   * @param maxTimestamp Maximum timestamp to return (exclusive).
   */
  public TimestampRangeFilter(long minTimestamp, long maxTimestamp) {
    mMinTimestamp = minTimestamp;
    mMaxTimestamp = maxTimestamp;
  }

  /** {@inheritDoc} */
  @Override
  public void reset() {
  }

  /** {@inheritDoc} */
  @Override
  public ReturnCode filterKeyValue(KeyValue kv) {
    final long ts = kv.getTimestamp();
    if (ts > mMaxTimestamp) {
      return ReturnCode.SEEK_NEXT_USING_HINT;
    } else if (ts < mMinTimestamp) {
      return ReturnCode.NEXT_COL;
    } else {
      return ReturnCode.INCLUDE;
    }
  }

  /** {@inheritDoc} */
  @Override
  public KeyValue getNextKeyHint(KeyValue kv) {
    Preconditions.checkArgument(kv.getTimestamp() > mMaxTimestamp);
    final KeyValue hint = new KeyValue(
        kv.getRow(),
        kv.getFamily(),
        kv.getQualifier(),
        mMaxTimestamp,
        KeyValue.Type.Maximum
    );
    return hint;
  }

  /** {@inheritDoc} */
  @Override
  public void write(DataOutput out) throws IOException {
    out.writeLong(mMinTimestamp);
    out.writeLong(mMaxTimestamp);
  }

  /** {@inheritDoc} */
  @Override
  public void readFields(DataInput in) throws IOException {
    mMinTimestamp = in.readLong();
    mMaxTimestamp = in.readLong();
  }
}
