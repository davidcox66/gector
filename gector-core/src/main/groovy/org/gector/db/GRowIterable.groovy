package org.gector.db

import me.prettyprint.hector.api.beans.Rows

/**
* Allows the multi-get query methods to return an object that allows iteration of the query
* results without having to do much work up front if iteration isn't needed.
*/
class GRowIterable implements Iterable<GRow> {
  
 /**
  * Supports iteration of standard column family or super column family by being loosely typed
  */
 private def rows;
 private GColumnFamily columnFamily;
  
 GRowIterable( GColumnFamily columnFamily, def rows ) {
   this.columnFamily = columnFamily;
   this.rows = rows;
 }
 Iterator<GRow> iterator() {
   return new GRowIterator( columnFamily, rows );
 }
 
 int getCount() {
   return rows.getCount();
 }
};
