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

import org.apache.hadoop.hbase.filter.FilterBase;

/**
 * Pass all HBase filter.
 */
public class PassAllFilter extends FilterBase {
  /**
   * Initializes a new pass-all filter.
   *
   * <p> This default constructor is also required to implement Writable. </p>
   */
  public PassAllFilter() {
  }

  /** {@inheritDoc} */
  @Override
  public void write(DataOutput out) throws IOException {
  }

  /** {@inheritDoc} */
  @Override
  public void readFields(DataInput in) throws IOException {
  }
}
