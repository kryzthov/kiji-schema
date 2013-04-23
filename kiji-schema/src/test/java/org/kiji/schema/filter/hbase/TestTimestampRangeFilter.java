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

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.schema.KijiClientTest;
import org.kiji.testing.fakehtable.FakeHBase;

/** Tests the StripValueRowFilter. */
public class TestTimestampRangeFilter extends KijiClientTest {
  private static final Logger LOG = LoggerFactory.getLogger(TestTimestampRangeFilter.class);

  private HTableInterface mHTable;

  @Before
  public final void setup() throws Exception {
    final FakeHBase hbase = new FakeHBase();
    final HTableDescriptor desc = new HTableDescriptor("table");
    desc.addFamily(new HColumnDescriptor("family").setMaxVersions(HConstants.ALL_VERSIONS));
    hbase.getAdminFactory().create(getConf()).createTable(desc);
    mHTable = hbase.getHTableFactory().create(getConf(), "table");

    final int nbytes = 333;
    final Put put = new Put(Bytes.toBytes("row"));
    for (long timestamp = 0; timestamp < 30; ++timestamp) {
      put.add(Bytes.toBytes("family"), Bytes.toBytes("qualifier"), timestamp, new byte[nbytes]);
    }
    mHTable.put(put);
  }

  /** Tests that the timestamp range filter properly restricts the set of returned KeyValues. */
  @Test
  public void testMaxBytesFilter() throws Exception {
    final Result result = mHTable.get(new Get(Bytes.toBytes("row"))
        .addFamily(Bytes.toBytes("family"))
        .setMaxVersions()
        .setFilter(new TimestampRangeFilter(3, 7))
    );
    Assert.assertEquals(4, result.size());
    Assert.assertEquals(6, result.raw()[0].getTimestamp());
    Assert.assertEquals(5, result.raw()[1].getTimestamp());
    Assert.assertEquals(4, result.raw()[2].getTimestamp());
    Assert.assertEquals(3, result.raw()[3].getTimestamp());
  }
}
