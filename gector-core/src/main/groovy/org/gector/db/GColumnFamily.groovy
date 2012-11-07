package org.gector.db;

import java.nio.ByteBuffer

import me.prettyprint.cassandra.serializers.ByteBufferSerializer
import me.prettyprint.cassandra.serializers.StringSerializer
import me.prettyprint.hector.api.Serializer
import me.prettyprint.hector.api.beans.OrderedRows
import me.prettyprint.hector.api.beans.OrderedSuperRows
import me.prettyprint.hector.api.beans.Row
import me.prettyprint.hector.api.beans.Rows
import me.prettyprint.hector.api.beans.SuperRow
import me.prettyprint.hector.api.beans.SuperRows
import me.prettyprint.hector.api.factory.HFactory
import me.prettyprint.hector.api.query.MultigetSliceQuery
import me.prettyprint.hector.api.query.MultigetSubSliceQuery
import me.prettyprint.hector.api.query.MultigetSuperSliceQuery
import me.prettyprint.hector.api.query.Query
import me.prettyprint.hector.api.query.QueryResult
import me.prettyprint.hector.api.query.RangeSlicesQuery
import me.prettyprint.hector.api.query.RangeSuperSlicesQuery

import org.gector.db.trans.GTransactionManager
import org.gector.db.update.GUpdater
import org.slf4j.Logger
import org.slf4j.LoggerFactory

/**
 * Provides methods to allow simplified access to a column family using Groovy associative-array
 * notation to reference individual rows within this column family.
 * 
 * @author david
 * @since Mar 29, 2011
 */
class GColumnFamily
{
  static final Logger LOGGER = LoggerFactory.getLogger(GColumnFamily);
  
  /**
   * The owning keyspace 
   */
  final GKeyspace keyspace;
  
  /** 
   * The name of this column family
   */
  final String columnFamilyName;
 
  /**
   * The key serializer. This type is not determined until something attempts to access a row
   * by a particular data type. At that point, we determine the serializer type and ensure
   * that is the type used from then on.
   */
  private Serializer serializer;
  
  /**
   * Holds the result of multi-get queries so that closures within the context of query()
   * will have access to the results of the multi-get transparently.
   */
  private def queriedRows;
  
  private static final ByteBuffer EMPTY = ByteBuffer.allocate(0);
  private static final int QUERY_CHUNK = 100;
   
  /**
   * Creates the named column family in the given keyspace
   *  
   * @param columnFamilyName
   * @param keyspace
   */
  GColumnFamily( String columnFamilyName, GKeyspace keyspace ) {
    this.keyspace = keyspace;
    this.columnFamilyName = columnFamilyName;
  }

  boolean exists() {
      return keyspace.isExistingColumnFamily(columnFamilyName);
  }
  void create( Class columnNameClass ) {
      keyspace.createColumnFamily( columnFamilyName, columnNameClass ); 
  }
  void create( Class superColumnNameClass, Class subColumnNameClass ) {
      keyspace.createSuperColumnFamily( columnFamilyName, superColumnNameClass, subColumnNameClass ); 
  }
  void createIfNeeded( Class columnNameClass ) {
      if( !exists() ) {
	      create( columnNameClass ); 
      }
  }
  void createIfNeeded( Class superColumnNameClass, Class subColumnNameClass ) {
      if( !exists() ) {
	      create( superColumnNameClass, subColumnNameClass ); 
      }
  }
  void recreate( Class columnNameClass ) {
      if( exists() ) {
          drop();
      }
      keyspace.createColumnFamily( columnFamilyName, columnNameClass ); 
  }
  void recreate( Class superColumnNameClass, Class subColumnNameClass ) {
      if( exists() ) {
          drop();
      }
      keyspace.createSuperColumnFamily( columnFamilyName, superColumnNameClass, subColumnNameClass ); 
  }
  
  void drop() {
    GExceptionHelper.runIgnoringMissingColumnFamily{
	    if( LOGGER.isDebugEnabled() ) { LOGGER.debug( "drop: dropping ${keyspace.keyspaceName}.${columnFamilyName}"); }
      cluster.withSchemaSynchronization{
		    cluster.cluster.dropColumnFamily( keyspace.keyspaceName, columnFamilyName );
      }
    }
    keyspace.metadata?.notifyDropColumnFamily( this );
  }
  
  
  int delete( Iterable keys, Object start, Object end, boolean reversed ) {
    int totalColumns = 0;
    for( Object key : keys ) {
      totalColumns += getAt(key).delete( start, end, reversed );
    }
    return totalColumns;
  }
  
  int delete( Iterable keys, Object start, Object end, boolean reversed, int max ) {
    int totalColumns = 0;
    for( Object key : keys ) {
      totalColumns += getAt(key).delete( start, end, reversed, max );
    }
    return totalColumns;
  }
  
  int deleteSuper( Iterable keys, Object start, Object end, boolean reversed ) {
    int totalColumns = 0;
    for( Object key : keys ) {
      totalColumns += getAt(key).deleteSuper( start, end, reversed );
    }
    return totalColumns;
  }
  
  int deleteSuper( Iterable keys, Object start, Object end, boolean reversed, int max ) {
    int totalColumns = 0;
    for( Object key : keys ) {
      totalColumns += getAt(key).deleteSuper( start, end, reversed, max );
    }
    return totalColumns;
  }

  /**
   * Executes the given closure with the context of a GRow specified by the given row key 
   * 
   * @param key
   * @param closure
   */
  void withRow( Object key, Closure closure ) {
    closure.call( getAt(key) );
  }

  /**
   * Executes the given query closure with the context of a GColumn representing the specified super column
   * 
   * @param key
   * @param columnName
   * @param closure
   */
  void querySuperColumn( Object key, Object columnName, Closure closure ) {
    withRow( key ) { GRow row ->
      row.querySuperColumn( columnName, closure ); 
    }
  }
  
  /**
   * Executes the given update closure with the context of a GColumn representing the specified super column
   * 
   * @param key
   * @param columnName
   * @param closure
   */
  void updateSuperColumn( Object key, Object columnName, Closure closure ) {
    withRow( key ) { GRow row ->
      row.updateSuperColumn( columnName, closure ); 
    }
  }
 
  /*
   *  Getter routines to allow associative-array syntax to address rows by a given key
   */
  GRow getAt( GString key ) {
    return getAtImpl( key.toString() );
  }
  GRow getAt( String key ) {
    return getAtImpl( key );
  }
  GRow getAt( long key ) {
    return getAtImpl( key );
  }
  GRow getAt( int key ) {
    return getAtImpl( key );
  }
  GRow getAt( boolean key ) {
    return getAtImpl( key );
  }
  GRow getAt( Date key ) {
    return getAtImpl( key );
  }
  GRow getAt( byte[] key ) {
    return getAtImpl( key );
  }
  GRow getAt( ByteBuffer key ) {
    return getAtImpl( key );
  }

  /**
   * Underlying implementation method for the various getAt() methods. 
   * @param key
   * @return
   */
  private GRow getAtImpl( Object key ) {
    initSerializer( GHelper.getSerializer(key) );
    if( LOGGER.isDebugEnabled() ) {
      String keySerializerName = GHelper.getSerializerName( serializer );
      LOGGER.debug( "getAtImpl: ${columnFamilyName}[${key}] (keySerializer=${keySerializerName})" );
    }
    return new GRow( key, this );
  }  
 
  /**
   * The serializer for the column family key. This will be null until one of the getAt() methods
   * has been called which implicitly assigns a serializer based upon  
   * @return
   */
  Serializer getSerializer() {
    return serializer;
  } 

  GColumnFamily setSerializer( Serializer serializer ) {
    this.serializer = serializer;
    return this;
  }
  
  /**
   * @return the owning GCluster 
   */
  GCluster getCluster() {
    return keyspace.cluster;
  }
  
  /**
   * @return the owning keyspace's name
   */
  String getKeyspaceName() {
    return keyspace.keyspaceName;
  }  
 
  /**
   * @return the cached GUpdater instance or asks the owning GKeyspace for a updater
   */
  GUpdater getUpdater() {
      return GTransactionManager.currentTransaction.getUpdater( this );
  }
 
  /**
   * Can be used within the closure of one of the multi-get query() calls to iterate over the
   * result rows.
   * 
   * @return
   */
  Iterable<GRow> getRows() {
    return queriedRows ? new GRowIterable(this,queriedRows) : null; 
  }
  
  /**
   * Used by subordinate GRow/GColumn to fetch the contents of a previously fetched row from a multi-get
   * if called within the context of GColumnFamily.query() methods.
   * 
   * @return may be either a ColumnSlice or SuperColumnSlice based upon which GColumnFamily.query() method
   *  we were called from. The calling code with deal with their individual slice type accordingly.
   */
  def getMultigetSlice( Object key ) {
    def slice = null;
    if( queriedRows ) {
      def row = queriedRows.getByKey( key );
      if( row ) {
        slice = row instanceof SuperRow ? row.getSuperSlice() : row.getColumnSlice();
        if( LOGGER.isDebugEnabled() ) { LOGGER.debug( "getMultigetSlice: multi-get rows exists - key={${key}}, row=${row}"); }
      }
    }
    return slice;
  }

  GScanIterable queryAll( Closure queryModifier, Closure closure ) {
	  GScanIterable ret = new GScanIterable( this, true, queryModifier );
	  if( closure != null ) {
		  GScanIterator iter = ret.iterator();
		  while( iter.hasNext() ) {
			  closure.call( iter.next() );
		  }
	  }
	  return ret;
  }
  
  GScanIterable queryAllSuper( Closure queryModifier, Closure closure ) {
	  GScanIterable ret = new GScanIterable( this, false, queryModifier );
	  if( closure != null ) {
		  GScanIterator iter = ret.iterator();
		  while( iter.hasNext() ) {
			  closure.call( iter.next() );
		  }
	  }
	  return ret;
  }

  
  /**
   * Does a multi-get query and allows you to either operate the rows returned or to 
   * pass in a closure which will have access to the results. Calls to get rows 
   * from within the context of the closure will use the pre-fetched results.
   *   
   * @param keys
   * @param start
   * @param end
   * @return
   */
  Iterable<GRow> query( Iterable keys, Closure closure=null ) {
    query( keys, null, null, false, Integer.MAX_VALUE, closure );
  }
  
  /**
   * Does a multi-get query and allows you to either operate the rows returned or to 
   * pass in a closure which will have access to the results. Calls to get rows 
   * from within the context of the closure will use the pre-fetched results.
   *   
   * @param keys
   * @param start
   * @param end
   * @return
   */
  Iterable<GRow> query( Iterable keys, Object start, Object end, Closure closure=null ) {
    query( keys, start, end, false, Integer.MAX_VALUE, closure );
  }

  /**
   * Does a multi-get query and allows you to either operate the rows returned or to 
   * pass in a closure which will have access to the results. Calls to get rows 
   * from within the context of the closure will use the pre-fetched results.
   * 
   * @param keys
   * @param start
   * @param end
   * @param max
   * @return
   */
  Iterable<GRow> query( Iterable keys, Object start, Object end, boolean reversed, Closure closure=null ) {
    return query( keys, start, end, reversed, Integer.MAX_VALUE, closure );
  }
  
  /**
   * Does a multi-get query and allows you to either operate the rows returned or to 
   * pass in a closure which will have access to the results. Calls to get rows 
   * from within the context of the closure will use the pre-fetched results.
   * 
   * @param keys
   * @param start
   * @param end
   * @param max
   * @return
   */
  Iterable<GRow> query( Iterable keys, Object start, Object end, boolean reversed, int max, Closure closure=null ) {
    MultigetSliceQuery query = createMultigetSliceQuery( keys );
    query.setRange( GHelper.toByteBuffer(start), GHelper.toByteBuffer(end), reversed, max );
    return executeMultigetQuery( query, closure );
  }

  /**
   * Does a multi-get query and allows you to either operate the rows returned or to 
   * pass in a closure which will have access to the results. Calls to get rows 
   * from within the context of the closure will use the pre-fetched results.
   *   
   * @param keys
   * @param columns
   * @return
   */
  Iterable<GRow> query( Iterable keys, List columns, Closure closure=null ) {
    MultigetSliceQuery query = createMultigetSliceQuery( keys );
    query.setColumnNames( GHelper.toByteBufferList(columns.toArray()) );
    return executeMultigetQuery( query, closure );
  }
 
  private MultigetSliceQuery createMultigetSliceQuery( Iterable keys )
  {
    initSerializerForMultiget( keys );
    Serializer bser = ByteBufferSerializer.get();
    MultigetSliceQuery query = HFactory.createMultigetSliceQuery( keyspace.keyspace, serializer, bser, bser );
    query.setKeys( keys );
    query.setColumnFamily( columnFamilyName );
    return query; 
  }

  /**
   * Does a multi-get query and allows you to either operate the rows returned or to 
   * pass in a closure which will have access to the results. Calls to get rows 
   * from within the context of the closure will use the pre-fetched results.
   *   
   * @param keys
   * @param start
   * @param end
   * @return
   */
  Iterable<GRow> querySub( Iterable keys, Object superColumnName, Closure closure=null ) {
    querySub( keys, superColumnName, null, null, false, Integer.MAX_VALUE, closure );
  }
  
  /**
   * Does a multi-get query and allows you to either operate the rows returned or to 
   * pass in a closure which will have access to the results. Calls to get rows 
   * from within the context of the closure will use the pre-fetched results.
   *   
   * @param keys
   * @param start
   * @param end
   * @return
   */
  Iterable<GRow> querySub( Iterable keys, Object superColumnName, Object start, Object end, Closure closure=null ) {
    querySub( keys, superColumnName, start, end, false, Integer.MAX_VALUE, closure );
  }

  /**
   * Does a multi-get query and allows you to either operate the rows returned or to 
   * pass in a closure which will have access to the results. Calls to get rows 
   * from within the context of the closure will use the pre-fetched results.
   * 
   * @param keys
   * @param start
   * @param end
   * @param max
   * @return
   */
  Iterable<GRow> querySub( Iterable keys, Object superColumnName, Object start, Object end, boolean reversed, Closure closure=null ) {
    return querySub( keys, superColumnName, start, end, reversed, Integer.MAX_VALUE, closure );
  }

  /**
   * Does a multi-get query and allows you to either operate the rows returned or to 
   * pass in a closure which will have access to the results. Calls to get rows 
   * from within the context of the closure will use the pre-fetched results.
   * 
   * @param keys
   * @param start
   * @param end
   * @param max
   * @return
   */
  Iterable<GRow> querySub( Iterable keys, Object superColumnName, Object start, Object end, boolean reversed, int max, Closure closure=null ) {
    MultigetSubSliceQuery query = createMultigetSubSliceQuery( keys, superColumnName );
    query.setRange( GHelper.toByteBuffer(start), GHelper.toByteBuffer(end), reversed, max );
    return executeMultigetQuery( query, closure );
  }
  /**
   * Does a multi-get query and allows you to either operate the rows returned or to 
   * pass in a closure which will have access to the results. Calls to get rows 
   * from within the context of the closure will use the pre-fetched results.
   *   
   * @param keys
   * @param columns
   * @return
   */
  Iterable<GRow> querySub( Iterable keys, Object superColumnName, List columns, Closure closure=null ) {
    MultigetSubSliceQuery query = createMultigetSubSliceQuery( keys, superColumnName );
    query.setColumnNames( GHelper.toByteBufferList(columns.toArray()) );
    return executeMultigetQuery( query, closure );
  }
 
  private MultigetSubSliceQuery createMultigetSubSliceQuery( Iterable keys, Object superColumnName )
  {
    initSerializerForMultiget( keys );
    Serializer bser = ByteBufferSerializer.get();
    MultigetSubSliceQuery query = HFactory.createMultigetSubSliceQuery( keyspace.keyspace, serializer, bser, bser );
    query.setKeys( keys );
    query.setColumnFamily( columnFamilyName );
    query.setSuperColumn( superColumnName );
    return query; 
  }

  /**
   * Does a multi-get query and allows you to either operate the rows returned or to 
   * pass in a closure which will have access to the results. Calls to get rows 
   * from within the context of the closure will use the pre-fetched results.
   *   
   * @param keys
   * @param start
   * @param end
   * @return
   */
  Iterable<GRow> querySuper( Iterable keys, Closure closure=null ) {
    querySuper( keys, null, null, false, Integer.MAX_VALUE, closure );
  }
  
  /**
   * Does a multi-get query and allows you to either operate the rows returned or to 
   * pass in a closure which will have access to the results. Calls to get rows 
   * from within the context of the closure will use the pre-fetched results.
   *   
   * @param keys
   * @param start
   * @param end
   * @return
   */
  Iterable<GRow> querySuper( Iterable keys, Object start, Object end, Closure closure=null ) {
    querySuper( keys, start, end, false, Integer.MAX_VALUE, closure );
  }

  /**
   * Does a multi-get query and allows you to either operate the rows returned or to 
   * pass in a closure which will have access to the results. Calls to get rows 
   * from within the context of the closure will use the pre-fetched results.
   * 
   * @param keys
   * @param start
   * @param end
   * @param max
   * @return
   */
  Iterable<GRow> querySuper( Iterable keys, Object start, Object end, boolean reversed, Closure closure=null ) {
    return querySuper( keys, start, end, reversed, Integer.MAX_VALUE, closure );
  }

  /**
   * Does a multi-get query and allows you to either operate the rows returned or to 
   * pass in a closure which will have access to the results. Calls to get rows 
   * from within the context of the closure will use the pre-fetched results.
   * 
   * @param keys
   * @param start
   * @param end
   * @param max
   * @return
   */
  Iterable<GRow> querySuper( Iterable keys, Object start, Object end, boolean reversed, int max, Closure closure=null ) {
    MultigetSuperSliceQuery query = createMultigetSuperSliceQuery( keys );
    query.setRange( GHelper.toByteBuffer(start), GHelper.toByteBuffer(end), reversed, max );
    return executeMultigetQuery( query, closure );
  }
  
  /**
   * Does a multi-get query and allows you to either operate the rows returned or to 
   * pass in a closure which will have access to the results. Calls to get rows 
   * from within the context of the closure will use the pre-fetched results.
   *   
   * @param keys
   * @param columns
   * @return
   */
  Iterable<GRow> querySuper( Iterable keys, List columns, Closure closure=null ) {
    MultigetSuperSliceQuery query = createMultigetSuperSliceQuery( keys );
    query.setColumnNames( GHelper.toByteBufferList(columns.toArray()) );
    return executeMultigetQuery( query, closure );
  }

  /**
   * A convenience function to iterate over each row of the current multi-get query
   * @param closure
   */
  void eachQueriedRow( Closure closure ) {
    Iterable iterable = new GRowIterable(this, queriedRows );
    for( GRow row : iterable ) {
      closure.call( row );  
    }
  }
 
  private MultigetSuperSliceQuery createMultigetSuperSliceQuery( Iterable keys )
  {
    initSerializerForMultiget( keys );
    Serializer bser = ByteBufferSerializer.get();
    MultigetSuperSliceQuery query = HFactory.createMultigetSuperSliceQuery( keyspace.keyspace, serializer, bser, bser, bser );
    query.setKeys( GHelper.toArray(keys) );
    query.setColumnFamily( columnFamilyName );
    return query; 
  }
  
  /**
   * Execute the given multi-get query. This calls the given closure if present and returns a
   * an Iterable which can optionally be used to iterate over the results.
   *  
   * @param key
   * @param query
   * @param closure
   * @return
   */
  private Iterable<GRow> executeMultigetQuery( Query query, Closure closure ) {
    QueryResult result = query.execute();
    def ret = result.get();
    if( closure ) {
	    queriedRows = ret;
	    try {
	      closure.call( this );
	    }
	    finally {
	      queriedRows = null;
	    }
    }
    return new GRowIterable(this,ret);
  }
  
  private void initSerializerForMultiget( Iterable keys )
  {
    if( !serializer ) {
      Iterator iter = keys.iterator();
      if( !iter.hasNext() ) {
        throw new GException( "No serializer defined for column family and no keys in query to determine serializer");
      }
      Object first = iter.next();
      Serializer ser = GHelper.getSerializer(first);
      if( !ser ) {
        throw new GException( "Unable to determine serializer for first element: ${first}" );
      }
      initSerializer( ser );
      
	    if( LOGGER.isDebugEnabled() ) {
		    LOGGER.debug( "initSerializerForMultiget: key serializer=" + GHelper.getSerializerName(serializer) );
	    }
    }
  }
  
  /**
   * Assigns a serializer type for this column family if none has been assigned yet. Also, ensures
   * that the serializer type remains consistent for this column family.
   *   
   * @param ser
   * @return
   */
  private Serializer initSerializer( Serializer ser ) {
    if( serializer == null ) {
	    if( ser == null ) {
	      throw new GException( "Key serializer cannot be null" );
	    }
      serializer = ser;
      keyspace.metadata?.notifyKeySerializer( this );
      if( LOGGER.isDebugEnabled() ) {
        String serializerName = GHelper.getSerializerName( ser );
        LOGGER.debug( "initSerializer: column family=${columnFamilyName}  - setting serializer=${serializerName}" );
      }
    }
    else if( serializer != ser ) {
      throw new GException( "Serializer types for row key do not match: original=${serializer}, new=${ser}");
    }
    return serializer;
  }
  
}
