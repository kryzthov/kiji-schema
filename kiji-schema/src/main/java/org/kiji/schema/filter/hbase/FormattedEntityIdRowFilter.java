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

import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.regex.Pattern;

import org.apache.avro.Schema;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.hadoop.hbase.filter.FilterBase;
import scala.actors.threadpool.Arrays;

import org.kiji.schema.EntityId;
import org.kiji.schema.EntityIdFactory;
import org.kiji.schema.avro.CompositeKeyFilter;
import org.kiji.schema.avro.KeyComponentFilter;

/**
 * HBase row filter applying comparators on row key components.
 */
public class FormattedEntityIdRowFilter extends FilterBase {

  // Serialized state:

  /** Specification of the row key components and of the component filters. */
  private CompositeKeyFilter mSpec;

  // Live internal state:

  /** Factory to decode HBase row keys into formatted entity IDs. */
  private EntityIdFactory mFactory;

  /** Required to implement Writable. Do not use otherwise. */
  public FormattedEntityIdRowFilter() {
  }

  /**
   * Initializes a new filter applying comparators on row key components.
   *
   * @param filter Filter specification.
   */
  public FormattedEntityIdRowFilter(CompositeKeyFilter filter) {
    mSpec = CompositeKeyFilter.newBuilder(filter).build();  // make a copy
  }

  /** {@inheritDoc} */
  @Override
  public void reset() {
    mFactory = EntityIdFactory.getFactory(mSpec.getFormat());
  }

  /** {@inheritDoc} */
  @Override
  public boolean filterRowKey(byte[] buffer, int offset, int length) {
    final EntityId eid =
        mFactory.getEntityIdFromHBaseRowKey(Arrays.copyOfRange(buffer, offset, offset + length));
    for (KeyComponentFilter compFilter : mSpec.getFilters()) {
      final Object component = eid.getComponentByIndex(compFilter.getComponentIndex());
      switch (compFilter.getFilter().getOperator()) {
      case EQUAL: {
        if (!component.equals(compFilter.getFilter().getOperand())) {
          return true;
        }
        break;
      }
      case NOT_EQUAL: {
        if (component.equals(compFilter.getFilter().getOperand())) {
          return true;
        }
        break;
      }
      case GREATER_OR_EQUAL: {
        final Comparable<Object> comparable = (Comparable<Object>) component;
        if (comparable.compareTo(compFilter.getFilter().getOperand()) < 0) {
          return true;
        }
        break;
      }
      case GREATER_THAN: {
        final Comparable<Object> comparable = (Comparable<Object>) component;
        if (comparable.compareTo(compFilter.getFilter().getOperand()) <= 0) {
          return true;
        }
        break;
      }
      case LESS_OR_EQUAL: {
        final Comparable<Object> comparable = (Comparable<Object>) component;
        if (comparable.compareTo(compFilter.getFilter().getOperand()) > 0) {
          return true;
        }
        break;
      }
      case LESS_THAN: {
        final Comparable<Object> comparable = (Comparable<Object>) component;
        if (comparable.compareTo(compFilter.getFilter().getOperand()) >= 0) {
          return true;
        }
        break;
      }
      case MATCH_REGEX: {
        final String text = (String) component;
        final String regex = (String) compFilter.getFilter().getOperand();
        if (!Pattern.matches(regex, text)) {
          return true;
        }
        break;
      }
      default: {
        throw new RuntimeException("Unhandled operator: " + compFilter.getFilter());
      }
      }
    }
    return false;
  }

  /** {@inheritDoc} */
  @Override
  public void write(DataOutput out) throws IOException {
    // Serialize the CompositeKeyFilter Avro record to bytes:
    final DatumWriter<CompositeKeyFilter> writer =
        new SpecificDatumWriter<CompositeKeyFilter>(mSpec.getSchema());
    final ByteArrayOutputStream baos = new ByteArrayOutputStream();
    final Encoder encoder = EncoderFactory.get().directBinaryEncoder(baos, null);
    writer.write(mSpec, encoder);
    encoder.flush();
    final byte[] bytes = baos.toByteArray();

    // Encode as writable:
    out.writeUTF(mSpec.getSchema().toString());
    out.writeInt(bytes.length);
    out.write(bytes);
  }

  /** {@inheritDoc} */
  @Override
  public void readFields(DataInput in) throws IOException {
    // Decode from writable:
    final Schema writerSchema = new Schema.Parser().parse(in.readUTF());
    final int nbytes = in.readInt();
    final byte[] bytes = new byte[nbytes];
    in.readFully(bytes);

    // Deserialize the CompositeKeyFilter Avro record from bytes:
    final DatumReader<CompositeKeyFilter> reader =
        new SpecificDatumReader<CompositeKeyFilter>(
            writerSchema, CompositeKeyFilter.SCHEMA$);
    final Decoder decoder = DecoderFactory.get().binaryDecoder(bytes, null);
    mSpec = (CompositeKeyFilter) reader.read(null, decoder);
  }
}
