package org.gector.db.meta

import me.prettyprint.hector.api.Serializer
import me.prettyprint.hector.api.ddl.ComparatorType

import org.gector.db.GCluster
import org.gector.db.GColumn
import org.gector.db.GColumnFamily
import org.gector.db.GKeyspace
import org.gector.db.GSubColumn

/**
 * This provides a callback mechanism for the Cassandra Groovy API at various points when data
 * is being loaded or referenced in the code such that we can determine the keyspace/column families
 * being used and data types of column names and values.
 *
 * The GMetadataManager implementations can respond to these notifications in a variety of ways,
 * from doing validation, to creating keyspaces and column families as needed.
 *
 * The API tries to delay notification until that last moment when as much information as possible
 * can be inferred from what the code that is attempting to read/write from the DB.
 * 
 * The use of the GMetadataManager is optional. It is likely to be used in a dev/uat environment
 * but disabled in production where you do not want any automatic changes to database structure.
 * 
 * @author david
 * @since Mar 28, 2011
 */
interface GMetadataManager
{
  // Constants for the various GMetadataManager types
  final static String BASIC = 'basic';
  final static String FULL = 'full';
  final static String AUTO = 'auto';
  
  void setReplicationFactor( int replicationFactor );
  
  boolean isStandardColumnFamily( String keyspaceName, String columnFamilyName );
  
  boolean isSuperColumnFamily( String keyspaceName, String columnFamilyName );
  
  /**
   * Called by GCluster to let the GMetadataManager know which cluster is will be using
   * 
   * @param cluster
   */
  void setCluster( GCluster cluster );
   
  /** 
   * Callback whenever an operation is occurring that must have the given keyspace defined.
   * 
   * @param keyspace
   */
  void notifyNeedingKeyspace( String keyspaceName ); 
 
  /**
   * Callback whenever and operation is occurring that must have the given column family defined 
   * 
   * @param columnFamily
   */
  void notifyNeedingColumnFamily( GColumn column ); 
  
  /**
   * Callback whenever and operation is occurring that must have the given column family defined 
   * 
   * @param column
   */
  void notifyNeedingSuperColumnFamily( GSubColumn column ); 
 
  /**
   * Callback for when we know the type of key serializer for a given column family
   *  
   * @param columnFamily
   */
  void notifyKeySerializer( GColumnFamily columnFamily ); 
 
  /**
   * Callback for when we know the serializer for a given column
   *  
   * @param column
   */
  void notifyColumnNameSerializer( GColumn column ); 
 
  /**
   * Callback for when we know the serializer for a column value
   *  
   * @param column
   * @param serializer
   */
  void notifyColumnValueSerializer( GColumn column, Serializer serializer, Object value ); 
 
  /**
   * Callback for when we know the serializer for a super column name 
   * 
   * @param column
   */
  void notifySubColumnNameSerializer( GSubColumn column ); 
  
  /**
   * Callback for when we know the data type of a sub column value
   * 
   * @param column
   * @param serializer
   */
  void notifySubColumnValueSerializer( GSubColumn column, Serializer serializer, Object value ); 

  /**
   * Callback for whenever a keyspace is dropped
   * 
   * @param keyspace
   */
  void notifyDropKeyspace( GKeyspace keyspace );

  /**
   * Callback for whenever a column family is dropped
   * 
   * @param columnFamily
   */
  void notifyDropColumnFamily( GColumnFamily columnFamily );  
  
  boolean isExistingKeyspace( String keyspaceName ); 
    
  boolean isExistingColumnFamily( String keyspace, String columnFamily );
  
  /**
   * Creates a keyspace of the given name in the cluster.
   *  
   * @param keyspaceName
   */
  void createKeyspace( String keyspaceName ); 
  
  /**
   * Creates a standard column family with the given comparator type
   * 
   * @param keyspace
   * @param columnFamily
   * @param comparator
   */
  void createColumnFamily( String keyspace, String columnFamily, ComparatorType comparator ); 
  
  /**
   * Creates a super column family with the given column name comparator types
   *  
   * @param keyspace
   * @param superColumnFamily
   * @param comparator
   * @param subComparator
   */
  void createSuperColumnFamily( String keyspace, String superColumnFamily, ComparatorType comparator, ComparatorType subComparator ); 
}
