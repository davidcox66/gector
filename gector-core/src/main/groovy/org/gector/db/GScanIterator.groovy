package org.gector.db;

import me.prettyprint.cassandra.serializers.ByteBufferSerializer
import me.prettyprint.cassandra.serializers.StringSerializer
import me.prettyprint.hector.api.query.QueryResult

  
abstract class GScanIterator implements Iterator<GRow> {
	  
	  protected static ByteBufferSerializer ser = new ByteBufferSerializer();
	  protected static final int QUERY_CHUNK = 500;
  
	  protected def query = null;
	  
	  private GColumnFamily columnFamily;
	  private Iterator iterator;
	  private boolean firstRun = true;
	  private Object row;
	  private Object last;
	  private Object lastKey;
	 
	  private Object startKey;
	  private Object endKey;
	    
	GScanIterator( GColumnFamily columnFamily ) {
		this( columnFamily, null, null );
	}
	GScanIterator( GColumnFamily columnFamily, Object startKey, Object endKey ) {
		this.columnFamily = columnFamily;
		this.startKey = startKey;
		this.endKey = endKey;
	} 
	  
	protected abstract def getSlice( def row );
	protected abstract boolean isEmpty( def row );
	
    @Override
    public boolean hasNext() {
      return row != null;
    }

    @Override
    public GRow next() {
	  GRow ret = new GRow( row.getKey(), columnFamily, getSlice(row)  ) ;
      findNext();
      return ret;
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException();
    }
	
	  private void findNext() {
	    row = null;
	    if (iterator != null) {
		    while (iterator.hasNext() && row == null) {
		      row  = iterator.next();
			  lastKey = row.getKey();
			  if( isEmpty(row) ) {
		        row = null;
		      }
		    }
		    if (!iterator.hasNext() && row == null) {
		      execute(lastKey, endKey);
		    }
	    }
	  }
	  protected void execute(Object start, Object end) {
	    iterator = null;
		
	    query.setKeys(start, end);
	    QueryResult result = query.execute();
	    def rows = (result != null) ? result.get() : null;
	    iterator = (rows != null) ? rows.iterator() : null;

	    // we'll skip this first one, since it is the same as the last one from previous time we executed
	    if (!firstRun  && iterator != null)  {
	      iterator.next();
	    }
		
	    firstRun = false;
	
	    if (!iterator.hasNext()) {
	      row = null;    // all done.  our iterator's hasNext() will now return false;
	    } 
		else {
	      findNext();
	    }
	  }
}



