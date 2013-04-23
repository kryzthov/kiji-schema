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

package org.kiji.schema.filter;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.google.common.collect.Lists;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.schema.EntityId;
import org.kiji.schema.Kiji;
import org.kiji.schema.KijiClientTest;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiDataRequestBuilder.ColumnsDef;
import org.kiji.schema.KijiRowData;
import org.kiji.schema.KijiRowScanner;
import org.kiji.schema.KijiTable;
import org.kiji.schema.KijiTableReader;
import org.kiji.schema.KijiTableReader.KijiScannerOptions;
import org.kiji.schema.KijiTableWriter;
import org.kiji.schema.avro.CompositeKeyFilter;
import org.kiji.schema.avro.KeyComponentFilter;
import org.kiji.schema.avro.OperatorType;
import org.kiji.schema.avro.RowKeyFormat2;
import org.kiji.schema.avro.ValueFilter;
import org.kiji.schema.layout.KijiTableLayouts;

/** Tests the StripValueRowFilter. */
public class TestKijiFormattedEntityIdRowFilter extends KijiClientTest {
  private static final Logger LOG =
      LoggerFactory.getLogger(TestKijiFormattedEntityIdRowFilter.class);

  /** Number of values per row key component. */
  private static final int VALUE_COUNT = 4;

  private KijiTable mTable;
  private KijiTableReader mReader;
  private KijiDataRequest mDataRequest;

  @Before
  public final void setup() throws Exception {
    final Kiji kiji = getKiji();
    kiji.createTable(KijiTableLayouts.getLayout(KijiTableLayouts.FORMATTED_RKF));

    mTable = kiji.openTable("table");

    final KijiTableWriter writer = mTable.openTableWriter();
    try {
      // Row key format: str/str/str/int/long :
      for (int i1 = 0; i1 < VALUE_COUNT; ++i1) {
        for (int i2 = 0; i2 < VALUE_COUNT; ++i2) {
          for (int i3 = 0; i3 < VALUE_COUNT; ++i3) {
            for (int i4 = 0; i4 < VALUE_COUNT; ++i4) {
              for (int i5 = 0; i5 < VALUE_COUNT; ++i5) {
                final EntityId eid = mTable.getEntityId(
                    "dummy-" + i1,
                    "str1-" + i2,
                    "str2-" + i3,
                    i4,   // int
                    i5);  // long
                writer.put(eid, "family", "column", 1L, "data");
              }
            }
          }
        }
      }
    } finally {
      writer.close();
    }

    mReader = mTable.openTableReader();

    mDataRequest = KijiDataRequest.builder()
        .addColumns(ColumnsDef.create().addFamily("family"))
        .build();
  }

  @After
  public final void teardown() throws Exception {
    mReader.close();
    mReader = null;

    mTable.release();
    mTable = null;
  }

  /** Tests that the row key component filter works. */
  @Test
  public void testFilterEquals() throws Exception {
    final KijiRowFilter rowFilter = new KijiFormattedEntityIdRowFilter(
        CompositeKeyFilter.newBuilder()
            .setFormat((RowKeyFormat2) mTable.getLayout().getDesc().getKeysFormat())
            .setFilters(Lists.newArrayList(
                KeyComponentFilter.newBuilder()
                    .setComponentIndex(0)
                    .setFilter(ValueFilter.newBuilder()
                        .setOperator(OperatorType.EQUAL)
                        .setOperand("dummy-3")
                        .build())
                    .build(),
                KeyComponentFilter.newBuilder()
                    .setComponentIndex(1)
                    .setFilter(ValueFilter.newBuilder()
                        .setOperator(OperatorType.NOT_EQUAL)
                        .setOperand("str1-0")
                        .build())
                    .build()))
            .build());
    final KijiScannerOptions scannerOptions =
        new KijiScannerOptions().setKijiRowFilter(rowFilter);

    final KijiRowScanner scanner = mReader.getScanner(mDataRequest, scannerOptions);
    try {
      int counter = 0;
      for (KijiRowData row : scanner) {
        assertEquals("dummy-3", row.getEntityId().getComponentByIndex(0));
        counter += 1;
      }
      // Filter forces component #0 and removes 1 value for component #1:
      assertEquals((VALUE_COUNT - 1) * VALUE_COUNT * VALUE_COUNT * VALUE_COUNT, counter);
    } finally {
      scanner.close();
    }
  }

  /** Tests that the row key component filter works. */
  @Test
  public void testFilterLessAndGreater() throws Exception {
    final KijiRowFilter rowFilter = new KijiFormattedEntityIdRowFilter(
        CompositeKeyFilter.newBuilder()
            .setFormat((RowKeyFormat2) mTable.getLayout().getDesc().getKeysFormat())
            .setFilters(Lists.newArrayList(
                KeyComponentFilter.newBuilder()
                    .setComponentIndex(0)
                    .setFilter(ValueFilter.newBuilder()
                        .setOperator(OperatorType.LESS_THAN)
                        .setOperand("dummy-3")  // restrict to [dummy-0, dummy-1, dummy-2]
                        .build())
                    .build(),
                KeyComponentFilter.newBuilder()
                    .setComponentIndex(1)
                    .setFilter(ValueFilter.newBuilder()
                        .setOperator(OperatorType.LESS_OR_EQUAL)
                        .setOperand("str1-2")  // restrict to [str1-0, str1-1, str1-2]
                        .build())
                    .build(),
                KeyComponentFilter.newBuilder()
                    .setComponentIndex(2)
                    .setFilter(ValueFilter.newBuilder()
                        .setOperator(OperatorType.GREATER_OR_EQUAL)
                        .setOperand("str2-3")  // restrict to [str2-3]
                        .build())
                    .build(),
                KeyComponentFilter.newBuilder()
                    .setComponentIndex(3)
                    .setFilter(ValueFilter.newBuilder()
                        .setOperator(OperatorType.GREATER_THAN)
                        .setOperand(2)  // restrict to [3]
                        .build())
                    .build(),
                KeyComponentFilter.newBuilder()
                    .setComponentIndex(4)
                    .setFilter(ValueFilter.newBuilder()
                        .setOperator(OperatorType.LESS_THAN)
                        .setOperand(2L)  // restrict to [0, 1]
                        .build())
                    .build()
            ))
            .build());
    final KijiScannerOptions scannerOptions =
        new KijiScannerOptions().setKijiRowFilter(rowFilter);

    final KijiRowScanner scanner = mReader.getScanner(mDataRequest, scannerOptions);
    try {
      int counter = 0;
      for (KijiRowData row : scanner) {
        assertTrue(((Integer) row.getEntityId().getComponentByIndex(3)) > 2);
        assertTrue(((Long) row.getEntityId().getComponentByIndex(4)) < 2L);
        counter += 1;
      }
      assertEquals(3 * 3 * 1 * 1 * 2, counter);
    } finally {
      scanner.close();
    }
  }

  /** Tests that the row key component filter works with the regex matching operator. */
  @Test
  public void testFilterRegexMatch() throws Exception {
    final KijiRowFilter rowFilter = new KijiFormattedEntityIdRowFilter(
        CompositeKeyFilter.newBuilder()
            .setFormat((RowKeyFormat2) mTable.getLayout().getDesc().getKeysFormat())
            .setFilters(Lists.newArrayList(
                KeyComponentFilter.newBuilder()
                    .setComponentIndex(1)
                    .setFilter(ValueFilter.newBuilder()
                        .setOperator(OperatorType.MATCH_REGEX)
                        .setOperand("^str1-[13]$")
                        .build())
                    .build()
            ))
            .build());
    final KijiScannerOptions scannerOptions =
        new KijiScannerOptions().setKijiRowFilter(rowFilter);

    final KijiRowScanner scanner = mReader.getScanner(mDataRequest, scannerOptions);
    try {
      int counter = 0;
      for (KijiRowData row : scanner) {
        assertTrue(
            "str1-1".equals(row.getEntityId().getComponentByIndex(1))
            || "str1-3".equals(row.getEntityId().getComponentByIndex(1)));
        counter += 1;
      }
      // Filter allows 2 values for component #1:
      assertEquals(VALUE_COUNT * 2 * VALUE_COUNT * VALUE_COUNT * VALUE_COUNT, counter);
    } finally {
      scanner.close();
    }
  }
}
