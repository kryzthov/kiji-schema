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

package org.kiji.schema.filter;

import java.io.IOException;

import com.google.common.base.Objects;
import org.apache.hadoop.hbase.filter.Filter;
import org.codehaus.jackson.JsonNode;

import org.kiji.annotations.ApiAudience;
import org.kiji.annotations.ApiStability;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.avro.CompositeKeyFilter;
import org.kiji.schema.filter.hbase.FormattedEntityIdRowFilter;

/**
 * Row filter applying per row key component comparators.
 */
@ApiAudience.Public
@ApiStability.Experimental
public final class KijiFormattedEntityIdRowFilter extends KijiRowFilter {

  private CompositeKeyFilter mFilter;

  /**
   * Initializes a new filter applying comparators to individual row key components.
   *
   * @param filter Filter specification.
   */
  public KijiFormattedEntityIdRowFilter(CompositeKeyFilter filter) {
    mFilter = CompositeKeyFilter.newBuilder(filter).build();
  }

  /** {@inheritDoc} */
  @Override
  public KijiDataRequest getDataRequest() {
    return KijiDataRequest.builder().build();  // nothing but the row key is required.
  }

  /** {@inheritDoc} */
  @Override
  public Filter toHBaseFilter(Context context) throws IOException {
    return new FormattedEntityIdRowFilter(mFilter);
  }

  /** {@inheritDoc} */
  @Override
  public boolean equals(Object other) {
    if (!(other instanceof KijiFormattedEntityIdRowFilter)) {
      return false;
    }
    final KijiFormattedEntityIdRowFilter that = (KijiFormattedEntityIdRowFilter) other;
    return Objects.equal(this.mFilter, that.mFilter);
  }

  /** {@inheritDoc} */
  @Override
  public int hashCode() {
    return mFilter.toString().hashCode();  // aouch
  }

  /** {@inheritDoc} */
  @Override
  public String toString() {
    return Objects.toStringHelper(KijiFormattedEntityIdRowFilter.class)
        .add("spec", mFilter)
        .toString();
  }

  /** {@inheritDoc} */
  @Override
  protected JsonNode toJsonNode() {
    throw new RuntimeException("Not implemented");
  }



  /** {@inheritDoc} */
  @Override
  protected Class<? extends KijiRowFilterDeserializer> getDeserializerClass() {
    throw new RuntimeException("Not implemented");
  }

}
