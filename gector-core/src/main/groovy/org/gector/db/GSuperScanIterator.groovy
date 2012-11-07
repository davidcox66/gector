package org.gector.db

import me.prettyprint.hector.api.Serializer
import me.prettyprint.hector.api.factory.HFactory

class GSuperScanIterator extends GScanIterator {
	
	GSuperScanIterator( GColumnFamily columnFamily, Serializer keySerializer, Closure queryModifier ) {
		this( columnFamily, keySerializer, null, null, queryModifier );
	}
	
	GSuperScanIterator( GColumnFamily columnFamily, Serializer keySerializer, Object startKey, Object endKey, Closure queryModifier ) {
		super( columnFamily, startKey, endKey );
		query = HFactory
		  .createRangeSuperSlicesQuery(columnFamily.keyspace.keyspace, keySerializer, ser, ser, ser)
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
		return row.getSuperSlice();
	}
	protected boolean isEmpty( def row ) {
		return row.getSuperSlice().getSuperColumns().isEmpty();
	}
}
