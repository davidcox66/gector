package org.gector.db

import me.prettyprint.hector.api.beans.Row
import me.prettyprint.hector.api.beans.Rows

/**
* Allows iteration of multi-get results, converting the individual Hector Row objects into
* GRows one-by-one.
*
* @author david
* @since Mar 31, 2011
*/
class GRowIterator implements Iterator<GRow>
{
 /**
  * Supports iteration of standard column family or super column family by being loosely typed
  */
 private def rows;
 
 private GColumnFamily columnFamily;
 private Iterator<Row> iter;
  
 GRowIterator( GColumnFamily columnFamily, def rows ) {
   this.columnFamily = columnFamily;
   this.rows = rows;
   this.iter = rows.iterator();
 }
   
 public boolean hasNext() {
   return iter.hasNext();
 }

 public GRow next() {
   def row = iter.next();
   // Deal with difference between standard and super column family 
   return new GRow( row.getKey(), columnFamily, row instanceof Row ? row.getColumnSlice() : row.getSuperSlice() ) ;
 }

 public void remove() {
   throw new UnsupportedOperationException( "remove() not supported");
 }
}
