package org.gector.db;

import me.prettyprint.hector.api.Serializer

public class GScanIterable implements Iterable<GRow> {
	
  private GColumnFamily columnFamily;
  private Serializer keySerializer;
  private boolean standard;
  private Closure queryModifier;
   
  public GScanIterable(GColumnFamily columnFamily, Class keyClass, boolean standard, Closure queryModifier ) {
	  this.columnFamily = columnFamily;
	  this.keySerializer = GHelper.getSerializerEx( keyClass );
	  this.standard = standard;
	  this.queryModifier = queryModifier;
  }

  @Override
  public Iterator<GRow> iterator() {
    return standard ? 
		new GStandardScanIterator(columnFamily,keySerializer, queryModifier) : 
		new GSuperScanIterator(columnFamily,keySerializer, queryModifier);
  }
}

