package org.gector.db.meta

import me.prettyprint.hector.api.Serializer

import org.slf4j.Logger
import org.slf4j.LoggerFactory

import org.gector.db.GColumn
import org.gector.db.GColumnFamily
import org.gector.db.GSubColumn

/**
 * The GAutoMetadataManager responds to notifications by creating keyspaces and column families
 * as needed and updating column metadata so values will be more clearly visible in the cassandra
 * command line interface. This does not add validation class information to any columns, merely
 * comparator types.
 *
 * @author david
 * @since Mar 28, 2011
 */
class GAutoMetadataManager extends GBasicMetadataManager
{
  private static final Logger LOGGER = LoggerFactory.getLogger(GAutoMetadataManager);

  /** 
   * Callback whenever an operation is occurring that must have the given keyspace defined.
   * 
   * @param keyspace
   */
  void notifyNeedingKeyspace( String keyspaceName ) {
    if( !isExistingKeyspace(keyspaceName) ) {
      createKeyspace( keyspaceName );
    }
  }
 
  /**
   * Callback whenever and operation is occurring that must have the given column family defined 
   * 
   * @param columnFamily
   */
  void notifyNeedingColumnFamily( GColumn column ) {
    notifyNeedingKeyspace( column.keyspace.keyspaceName );
    if( !isExistingColumnFamily(column.keyspaceName,column.columnFamilyName) ) {
      createColumnFamily( column.keyspaceName, column.columnFamilyName, 
        getComparatorType(column.serializer) );
    }
  }
  
  /**
   * Callback whenever and operation is occurring that must have the given column family defined 
   * 
   * @param column
   */
  void notifyNeedingSuperColumnFamily( GSubColumn subColumn ) {
    GColumnFamily columnFamily = subColumn.columnFamily;
    notifyNeedingKeyspace( columnFamily.keyspace.keyspaceName );
    if( !isExistingColumnFamily(columnFamily.keyspaceName,columnFamily.columnFamilyName) ) {
      createSuperColumnFamily( columnFamily.keyspaceName, columnFamily.columnFamilyName, 
        getComparatorType(subColumn.superColumn.serializer),
        getComparatorType(subColumn.serializer) );
    }
  }
 
  /**
   * Callback for when we know the type of key serializer for a given column family
   *  
   * @param columnFamily
   */
  void notifyKeySerializer( GColumnFamily columnFamily ) {
  }
 
  /**
   * Callback for when we know the serializer for a given column
   *  
   * @param column
   */
  void notifyColumnNameSerializer( GColumn column ) {
    // Can't assume it's a standard column family yet. Have to wait until they set a value or get a sub-column.
    // notifyNeedingColumnFamily( column );
  } 
 
  /**
   * Callback for when we know the serializer for a column value
   *  
   * @param column
   * @param serializer
   */
  void notifyColumnValueSerializer( GColumn column, Serializer serializer, Object value ) {
    notifyNeedingColumnFamily( column );
  }
 
  /**
   * Callback for when we know the serializer for a super column name 
   * 
   * @param column
   */
  void notifySubColumnNameSerializer( GSubColumn column ) {
    notifyNeedingSuperColumnFamily( column );
  } 
  
  /**
   * Callback for when we know the data type of a sub column value
   * 
   * @param column
   * @param serializer
   */
  void notifySubColumnValueSerializer( GSubColumn column, Serializer serializer, Object value ) {
    notifyNeedingSuperColumnFamily( column );
  }

}
