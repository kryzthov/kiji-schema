package org.kiji.schema.filter;

import java.io.IOException;

import org.apache.hadoop.hbase.filter.Filter;

import org.kiji.schema.KijiColumnName;

public class KijiFamilyFilter extends KijiColumnFilter {

  /** {@inheritDoc} */
  @Override
  public Filter toHBaseFilter(KijiColumnName kijiColumnName, Context context) throws IOException {
    return null;
  }

}
