package org.gector.db

import me.prettyprint.hector.api.factory.HFactory

class GStandardScanIterator extends GScanIterator {

	GStandardScanIterator( GColumnFamily columnFamily, Closure queryModifier ) {
		this( columnFamily, null, null, queryModifier );
	}
	
	GStandardScanIterator( GColumnFamily columnFamily, Object startKey, Object endKey, Closure queryModifier ) {
		super( columnFamily, startKey, endKey );
	    query = HFactory
	      .createRangeSlicesQuery(columnFamily.keyspace.keyspace, ser, ser, ser)
	      .setColumnFamily(columnFamily.columnFamilyName)
	      .setRowCount(QUERY_CHUNK);
		  
		if( queryModifier ) {  
			queryModifier.setDelegate( query );
			queryModifier.call( query );  
		}
		else {
		  query.setRange(null, null, false, Integer.MAX_VALUE)
		}
		execute( startKey, endKey );  
	}
	
	protected def getSlice( def row ) {
		return row.getColumnSlice();	
	}
	
	protected boolean isEmpty( def row ) {
		return row.getColumnSlice().getColumns().isEmpty();
	}
}
