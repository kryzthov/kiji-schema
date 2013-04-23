/**
 * (c) Copyright 2012 WibiData, Inc.
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

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.filter.FilterBase;

/**
 * HBase column filter to limit the number of bytes per reply to a Get request.
 */
public class MaxBytesFilter extends FilterBase {
  /** Configured maximum number of bytes to return. */
  private int mMaxBytes;

  /** Current number of bytes accumulated so far. */
  private int mBytesCount;

  /** Required to implement Writable. Do not use otherwise. */
  public MaxBytesFilter() {
  }

  /**
   * Initializes a new filter with the specified max bytes limit.
   *
   * @param maxBytes Maximum number of bytes to return in a Get reply.
   */
  public MaxBytesFilter(int maxBytes) {
    mMaxBytes = maxBytes;
  }

  /** {@inheritDoc} */
  @Override
  public void reset() {
    mBytesCount = 0;
  }

  /** {@inheritDoc} */
  @Override
  public ReturnCode filterKeyValue(KeyValue kv) {
    mBytesCount += kv.getLength();
    if (mBytesCount > mMaxBytes) {
      return ReturnCode.NEXT_ROW;  // we are done
    } else {
      return ReturnCode.INCLUDE;
    }
  }

  /** {@inheritDoc} */
  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(mMaxBytes);
  }

  /** {@inheritDoc} */
  @Override
  public void readFields(DataInput in) throws IOException {
    mMaxBytes = in.readInt();
  }
}
