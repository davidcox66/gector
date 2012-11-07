package org.gector.db;

import me.prettyprint.cassandra.serializers.ByteBufferSerializer
import me.prettyprint.hector.api.Keyspace
import me.prettyprint.hector.api.beans.OrderedRows
import me.prettyprint.hector.api.beans.Row
import me.prettyprint.hector.api.factory.HFactory
import me.prettyprint.hector.api.query.QueryResult
import me.prettyprint.hector.api.query.RangeSlicesQuery

public class GScanIterable implements Iterable<GRow> {
	
  private GColumnFamily columnFamily;
  private boolean standard;
  private Closure queryModifier;
   
  public GScanIterable(GColumnFamily columnFamily,boolean standard, Closure queryModifier ) {
	  this.columnFamily = columnFamily;
	  this.standard = standard;
	  this.queryModifier = queryModifier;
  }

  @Override
  public Iterator<GRow> iterator() {
    return standard ? new GStandardScanIterator(columnFamily,queryModifier) : new GSuperScanIterator(columnFamily,queryModifier);
  }
}

