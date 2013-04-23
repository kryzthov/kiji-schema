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

import scala.actors.threadpool.Arrays;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.filter.FilterBase;

/**
 * HBase column filter able to apply per-column max-versions.
 *
 * <p>
 *   This filter is intended to be combined with other filters (eg. a column selector).
 *   For example:
 *     OR(
 *       AND(column=family/qualifier1, max-versions=1),
 *       AND(column=family/qualifier2, max-versions=5),
 *       PassAll)  // other columns have no max-versions restriction
 * </p>
 */
public class MaxVersionsFilter extends FilterBase {
  /** Configured maximum number of versions to return per column. */
  private int mMaxVersions;

  /** Current column being counted (no value). */
  private byte[] mColumnKey;

  /** Current number of versions emitted so far. */
  private int mVersionCount;

  /** Required to implement Writable. Do not use otherwise. */
  public MaxVersionsFilter() {
  }

  /**
   * Initializes a new column filter with the specified per-column max-version restriction.
   *
   * @param maxVersions Maximum number of versions to return per column.
   */
  public MaxVersionsFilter(int maxVersions) {
    mMaxVersions = maxVersions;
  }

  /** {@inheritDoc} */
  @Override
  public void reset() {
    mColumnKey = null;
    mVersionCount = 0;
  }

  /** {@inheritDoc} */
  @Override
  public ReturnCode filterKeyValue(KeyValue kv) {
    final byte[] key = kv.getKey();  // this could be optimized without copying.
    if ((mColumnKey == null) || !Arrays.equals(mColumnKey, key)) {
      mColumnKey = key;
      mVersionCount = 0;
    }

    mVersionCount += 1;
    if (mVersionCount < mMaxVersions) {
      return ReturnCode.INCLUDE;
    } else {
      mVersionCount = 0;
      return ReturnCode.NEXT_COL;
    }
  }

  /** {@inheritDoc} */
  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(mMaxVersions);
  }

  /** {@inheritDoc} */
  @Override
  public void readFields(DataInput in) throws IOException {
    mMaxVersions = in.readInt();
  }
}
