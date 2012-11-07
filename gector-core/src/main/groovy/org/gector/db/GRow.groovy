package org.gector.db

import java.nio.ByteBuffer

import me.prettyprint.cassandra.serializers.ByteBufferSerializer
import me.prettyprint.cassandra.serializers.StringSerializer
import me.prettyprint.hector.api.Serializer
import me.prettyprint.hector.api.beans.ColumnSlice
import me.prettyprint.hector.api.beans.HColumn
import me.prettyprint.hector.api.factory.HFactory
import me.prettyprint.hector.api.query.CountQuery
import me.prettyprint.hector.api.query.MultigetSuperSliceQuery
import me.prettyprint.hector.api.query.Query
import me.prettyprint.hector.api.query.QueryResult
import me.prettyprint.hector.api.query.SliceQuery
import me.prettyprint.hector.api.query.SuperCountQuery
import me.prettyprint.hector.api.query.SuperSliceQuery

import org.gector.db.update.GUpdater
import org.slf4j.Logger
import org.slf4j.LoggerFactory

/**
 * Provides read/write access to the contents of a specified column family's row. This includes
 * both standard and super column families. This includes both associative-array and property
 * style notation for access to individual columns.
 *  
 * @author david
 * @since Mar 29, 2011
 */
class GRow
{
  static final Logger LOGGER = LoggerFactory.getLogger(GRow);
 
  /**
   * The owning GColumnFamily 
   */
  private GColumnFamily columnFamily;
  
  /**
   * This is used as an optimization when getting individual column values. By using the query() method,
   * access to individual columns will use a pre-fetched queried ColumnSlice instead of making individual
   * queries for each column.
   */
  private def slice;
  
  /**
   * The key for this row 
   */
  final Object key;
   
  GRow( Object key, GColumnFamily columnFamily ) {
    this.key = key;
    this.columnFamily = columnFamily;
  }

  /**
   * Create a row for a given pre-fetched slice from a multi-get. This supports both standard and 
   * super column families by holding a loosely typed reference to the slice or super slice.
   * 
   * @param key
   * @param columnFamily
   * @param slice
   */
  GRow( Object key, GColumnFamily columnFamily, def slice ) {
    this.key = key;
    this.columnFamily = columnFamily;
    this.slice = slice;
  }

  public String getKeyAsString() {
	  return GHelper.asString( key );
  }
  
  public String getKeyAsString( Serializer ser ) {
	  return GHelper.asString( key, ser );
  }
  
  /**
   * Deletes this row from the database
   */
  void delete() {
    updater.delete(key, columnFamilyName );
  }
  
  int delete( Object start, Object end, boolean reversed ) {
    int totalColumns=0;
    while( true ) {
      int columnCount = delete( start, end, reversed, GCluster.MAX_DELETE_CHUNK ); 
      totalColumns += columnCount;
      // if( LOGGER.isDebugEnabled() ) { LOGGER.debug( "delete: deleted chunk - start=${start}, end=${end}, columnCount=${columnCount}"); }
      LOGGER.info( "delete: deleted chunk - key=${key}, }start=${start}, end=${end}, columnCount=${columnCount}"); 
      if( columnCount < GCluster.MAX_DELETE_CHUNK )  {
        return totalColumns;
      }
    }
  }
  
  int delete( Object start, Object end, boolean reversed, int max ) {
    Class cls = GHelper.getClassOfRange( start, end );
    int columnCount = 0;
    query( start, end, reversed, max ) { GRow row ->
      columnCount = row.getSliceColumnCount();
      row.eachSliceColumn(cls) { GColumn col ->
        col.delete();
      }
    }
    return columnCount;
  }
  
  int deleteSuper( Object start, Object end, boolean reversed ) {
    int totalColumns=0;
    while( true ) {
      int columnCount = deleteSuper( start, end, reversed, GCluster.MAX_DELETE_CHUNK ); 
      totalColumns += columnCount;
      if( LOGGER.isDebugEnabled() ) { LOGGER.debug( "deleteSuper: deleted chunk - key=${key}, start=${start}, end=${end}, columnCount=${columnCount}"); }
      if( columnCount < GCluster.MAX_DELETE_CHUNK )  {
        return totalColumns;
      }
    }
  }
  
  int deleteSuper( Object start, Object end, boolean reversed, int max ) {
    Class cls = GHelper.getClassOfRange( start, end );
    int columnCount = 0;
    query( start, end, reversed, max ) { GRow row ->
      columnCount = row.getSliceColumnCount();
      row.eachSliceColumn(cls) { GColumn col ->
        col.deleteSuper();
      }
    }
    return columnCount;
  }

  /*
   * These are the various getter routines that allow associative-array notation access to a row's
   * columns.
   */
  GColumn getAt( GString columnName ) {
    return getAtImpl( columnName.toString() );
  }
  GColumn getAt( String columnName ) {
    return getAtImpl( columnName );
  }
  GColumn getAt( long columnName ) {
    return getAtImpl( columnName );
  }
  GColumn getAt( int columnName ) {
    return getAtImpl( columnName );
  }
  GColumn getAt( boolean columnName ) {
    return getAtImpl( columnName );
  }
  GColumn getAt( Date columnName ) {
    return getAtImpl( columnName );
  }
  GColumn getAt( byte[] columnName ) {
    return getAtImpl( columnName );
  }
  GColumn getAt( ByteBuffer columnName ) {
    return getAtImpl( columnName );
  }
  GColumn propertyMissing( String columnName ) {
    return getAtImpl( columnName );
  }
  
  /**
   * @param columnName
   * @return underlying implementation method for the various getAt() methods
   */
  private GColumn getAtImpl( Object columnName ) {
    Serializer columnNameSerializer = GHelper.getSerializer(columnName);
    if( LOGGER.isDebugEnabled() ) {
      String columnNameSerializerName = GHelper.getSerializerName( columnNameSerializer );
      LOGGER.debug( "getAtImpl: ${columnFamilyName}[${key}][${columnName}] (columnNameSerializer=${columnNameSerializerName})" );
    }
	  return new GColumn( columnName, columnNameSerializer, this );
  }

  /**
   * Convenience method to allow multiple updates of super column values within the context
   * of the specified super column
   *  
   * @param columnName
   * @param closure
   */
  void updateSuperColumn( Object columnName, Closure closure ) {
    getAtImpl(columnName).update( closure );   
  } 
 
  /**
   * Convenience method to allow efficient multiple reads within the sub columns of the specified
   * super column.
   *  
   * @param columnName
   * @param closure
   */
  void querySuperColumn( Object columnName, Closure closure ) {
    getAtImpl(columnName).query( closure );   
  } 
 
  /**
   * Allows more efficient access to multiple columns within this row by pre-fetching the column
   * values. Within the context of the closure, child GColumns will use these pre-fetched results
   * instead of querying the database for each individual column.
   *  
   * @param closure
   */
  GRow query( Closure closure=null ) {
    return query( null, null, false, Integer.MAX_VALUE, closure );
  }
 
  /**
   * Allows more efficient access to multiple columns within this row by pre-fetching the column
   * values. Within the context of the closure, child GColumns will use these pre-fetched results
   * instead of querying the database for each individual column.
   *  
   * @param closure
   */
  GRow query( Object start, Object end, Closure closure=null ) {
    return query( start, end, false, Integer.MAX_VALUE, closure );
  }
  
  /**
   * Allows more efficient access to multiple columns within this row by pre-fetching the column
   * values. Within the context of the closure, child GColumns will use these pre-fetched results
   * instead of querying the database for each individual column.
   *  
   * @param closure
   */
  GRow query( Object start, Object end, boolean reversed, Closure closure=null ) {
    return query( start, end, reversed, Integer.MAX_VALUE, closure );
  }
  
  /**
   * Allows more efficient access to multiple columns within this row by pre-fetching the column
   * values. Within the context of the closure, child GColumns will use these pre-fetched results
   * instead of querying the database for each individual column.
   *  
   * @param closure
   */
  GRow query( Object start, Object end, boolean reversed, int max, Closure closure=null ) {
    SliceQuery query = createSliceQuery();
    query.setRange( GHelper.toByteBuffer(start), GHelper.toByteBuffer(end), reversed, max );
    return executeQuery( query, closure );
  }

  /**
   * Allows more efficient access to multiple columns within this row by pre-fetching the column
   * values. Within the context of the closure, child GColumns will use these pre-fetched results
   * instead of querying the database for each individual column.
   *  
   * @param closure
   */
  GRow query( def columnNames, Closure closure=null ) {
    SliceQuery query = createSliceQuery();
    query.setColumnNames( GHelper.toByteBufferList(columnNames) );
    return executeQuery( query, closure );
  }
  
  /**
   * @param columnName
   * @return just the column value for the specified column name
   */
  ColumnSlice querySingleSlice( Object columnName ) {  
    SliceQuery query = createSliceQuery();
    ByteBuffer columnNameBuffer = GHelper.toByteBuffer( columnName );
    query.setRange( columnNameBuffer, columnNameBuffer, false, Integer.MAX_VALUE );
    return query.execute().get();
  }
 
  /**
   * Underlying implementation method to construct a query for the current row 
   * @return
   */
  private SliceQuery createSliceQuery() {
    Serializer bser = ByteBufferSerializer.get();
    SliceQuery query = 
      HFactory.createSliceQuery( keyspace.keyspace, columnFamily.serializer, bser, bser );
    query.setKey( key );
    query.setColumnFamily( columnFamilyName );
    return query;
  } 
  
  /**
   * Does a query and allows you to either operate the rows returned or to 
   * pass in a closure which will have access to the results. Calls to get rows 
   * from within the context of the closure will use the pre-fetched results.
   *   
   * @param keys
   * @param start
   * @param end
   * @return
   */
  GRow querySuper( Closure closure=null ) {
    querySuper( null, null, false, Integer.MAX_VALUE, closure );
  }
  
  /**
   * Does a query and allows you to either operate the rows returned or to 
   * pass in a closure which will have access to the results. Calls to get rows 
   * from within the context of the closure will use the pre-fetched results.
   *   
   * @param keys
   * @param start
   * @param end
   * @return
   */
  GRow querySuper( Object start, Object end, Closure closure=null ) {
    querySuper( start, end, false, Integer.MAX_VALUE, closure );
  }

  /**
   * Does a query and allows you to either operate the rows returned or to 
   * pass in a closure which will have access to the results. Calls to get rows 
   * from within the context of the closure will use the pre-fetched results.
   * 
   * @param keys
   * @param start
   * @param end
   * @param max
   * @return
   */
  GRow querySuper( Object start, Object end, boolean reversed, Closure closure=null ) {
    return querySuper( start, end, reversed, Integer.MAX_VALUE, closure );
  }
  
  /**
   * Does a query and allows you to either operate the rows returned or to 
   * pass in a closure which will have access to the results. Calls to get rows 
   * from within the context of the closure will use the pre-fetched results.
   * 
   * @param keys
   * @param start
   * @param end
   * @param max
   * @return
   */
  GRow querySuper( Object start, Object end, boolean reversed, int max, Closure closure=null ) {
    SuperSliceQuery query = createSuperSliceQuery();
    query.setRange( GHelper.toByteBuffer(start), GHelper.toByteBuffer(end), reversed, max );
    return executeQuery( query, closure );
  }

  /**
   * Does a query and allows you to either operate the rows returned or to 
   * pass in a closure which will have access to the results. Calls to get rows 
   * from within the context of the closure will use the pre-fetched results.
   *   
   * @param keys
   * @param columns
   * @return
   */
  GRow querySuper( List columns, Closure closure=null ) {
    MultigetSuperSliceQuery query = createSuperSliceQuery();
    query.setColumnNames( GHelper.toByteBufferList(columns.toArray()) );
    return executeQuery( query, closure );
  }
 
  private SuperSliceQuery createSuperSliceQuery()
  {
    initSerializerForGet( key );
    Serializer bser = ByteBufferSerializer.get();
    SuperSliceQuery query = HFactory.createSuperSliceQuery( keyspace.keyspace, columnFamily.serializer, bser, bser, bser );
    query.setKey( key );
    query.setColumnFamily( columnFamilyName );
    return query; 
  }
  
  private void initSerializerForGet( Object key )
  {
    if( !columnFamily.serializer ) {
      if( !key ) {
        throw new GException( "No serializer defined for column family and no keys in query to determine serializer");
      }
      columnFamily.initSerializer( GHelper.getSerializer(key) );
    }
    if( LOGGER.isDebugEnabled() ) {
	    LOGGER.debug( "initSerializerForGet: key serializer=" + GHelper.getSerializerName(columnFamily.serializer) );
    }
  }
  
  /**
   * Execute the given  query. This calls the given closure if present and returns a
   * an Iterable which can optionally be used to iterate over the results.
   *  
   * @param query
   * @param closure
   * @return
   */
  private GRow executeQuery( Query query, Closure closure ) {
    QueryResult result = query.execute();
    def ret = result.get();
    return callForQuerySlice( ret, closure );
  }
  
  /**
   * Underlying implementation method to invoke the given closure for the currently queried slice.
   * The cached slice is removed at the end of the invocation.
   *  
   * @param closure
   */
  private GRow callForQuerySlice( def result, Closure closure ) {
    if( closure ) {
	    slice = result;
	    try {
	      closure.call( this );
	    }
	    finally {
	      slice = null;
	    }
      return null;
    }
    else {
	    return new GRow(this.key,this.columnFamily,result);
    }
  } 
 
  /**
   * Iterates through each column within the context of a query on the row. 
   * @param nameClass
   * @param closure
   */
  void eachSliceColumn( Class nameClass, Closure closure )  {
    def sliceOrSuperSlice = findColumnSlice();
    if( sliceOrSuperSlice ) {
        Serializer nameSerializer = GHelper.getSerializer( nameClass );
        def columns = sliceOrSuperSlice instanceof ColumnSlice ? sliceOrSuperSlice.columns : sliceOrSuperSlice.superColumns;
        if( LOGGER.isDebugEnabled() ) { 
          String str = GHelper.toString( columns, nameSerializer, null );
          LOGGER.debug( "eachSliceColumn: columns=${str}"); 
        }
        for( def col : columns ) {
          Object currentColumnName = GHelper.fromByteBuffer( col.getName(), nameSerializer );    
          if( LOGGER.isDebugEnabled() ) { LOGGER.debug( "eachSliceColumn: currentColumnName=${currentColumnName}"); }
          GColumn gcol = new GColumn( currentColumnName, nameSerializer, this );
          closure.call( gcol );
        } 
    }
  }

  int getSliceColumnCount() {
    if( slice ) {
      List lst =  slice instanceof ColumnSlice ? slice.getColumns() : slice.getSuperColumns();
      return lst ? lst.size() : 0;
    }
    return null;
  }
  
  /**
   * Gets the column slice or super slice this is currently using. This is loosely types to
   * support both standard and super column families.
   * 
   * @return
   */
  def getColumnSlice() {
    return slice;
  }

  /**
   * Looks for previously queried results in this row or within the context of a column family multi-get
   * @return
   */
  def findColumnSlice()  {
    def ret = slice;
    if( !ret ) {
      ret = columnFamily.getMultigetSlice( key );
      if( LOGGER.isDebugEnabled() ) { 
        if( ret != null ) { 
          LOGGER.debug( "findColumnSlice: using multi-get column slice=${ret}"); 
        }
        else {
          LOGGER.debug( "findColumnSlice: no cached results found" ); 
        }
      }
    }
    return ret;
  }

  def findOrQuerySlice( Object columnName ) {
    def sliceOrSuperSlice = findColumnSlice();
    if( !sliceOrSuperSlice ) {
      sliceOrSuperSlice = querySingleSlice( columnName );
    } 
    return sliceOrSuperSlice;
  }
  
  /**
   * @return the owning GCluster 
   */
  GCluster getCluster() {
    return keyspace.cluster;
  }
  /**
   * @return the owning keyspace
   */
  GKeyspace getKeyspace() {
    return columnFamily.keyspace;
  }   
 
  /**
   * @return the GUpdater for this row's column family
   */
  GUpdater getUpdater() {
    return columnFamily.updater;
  }  

  /**
   * @return the column family name to which this belongs
   */
  String getColumnFamilyName() {
    return columnFamily.getColumnFamilyName();
  }   

  /**
   * This is used by child GColumn objects to fetch their column values. This will use
   * pre-fetched results from query() if the GColumn is being used within query() or
   * will fetch the column value as a less efficient one-off operation.
   * 
   * @param columnName
   * @return the contents of the column value
   */
  ByteBuffer getColumnValue( Object columnName ) {
    Serializer columnNameSerializer = GHelper.getSerializer(columnName);
    ColumnSlice cs = findOrQuerySlice( columnName );
    HColumn col = cs.getColumnByName(columnNameSerializer.toByteBuffer(columnName));
    return col != null ? col.getValue() : null;
  }

	/**
	 * Checks if there are any columns at a row specified by key in a standard column family
	 * 
	 * @return true if columns exist
	 */
	boolean isColumnsExist() {
	    return countColumns() > 0;
	}
	
	/**
	 * @return the number of columns in a standard column family at the specified row key
	 */
	int countColumns() {
	    return countColumns(key, null, null, Integer.MAX_VALUE);
	}
	
	/**
	 * Counts columns in the specified range of a standard column family
	 * 
	 * @param start
	 * @param end
	 * @param max
	 * @return
	 */
	int countColumns(Object start, Object end, int max) {
	    CountQuery query = HFactory.createCountQuery(keyspace, columnFamily.serializer, ByteBufferSerializer.get());
	    query.setKey(key);
	    query.setColumnFamily(columnFamilyName);
	    query.setRange(start, end, max);
	    return query.execute().get();
	}
	
	/**
	 * Checks if there are any columns at a row specified by key in a super column family
	 * 
	 * @return true if columns exist
	 */
	public boolean isSuperColumnsExist() {
	    return countSuperColumns() > 0;
	}
	
	/**
	 * @return the number of columns in a super column family at the specified row key
	 */
	int countSuperColumns() {
	    return countSuperColumns(null, null, Integer.MAX_VALUE);
	}
	
	/**
	 * Counts columns in the specified range of a super column family
	 * 
	 * @param start
	 * @param end
	 * @param max
	 * @return
	 */
	int countSuperColumns(final Object start, final Object end, final int max) {
	    final SuperCountQuery query = HFactory.createSuperCountQuery(keyspace, columnFamily.serializer, ByteBufferSerializer.get());
	    query.setKey(key);
	    query.setColumnFamily(columnFamilyName);
	    query.setRange(start, end, max);
	    return query.execute().get();
	}
	
  boolean equals( Object obj ) {
    if( obj != null && obj instanceof GRow ) {
      GRow other = (GRow)obj;
      return keyspace == other.keyspace && key == other.key; 
    }
    return false;
  }
  
  int hashCode() {
    return key.hashCode();
  }
  
  String toString() {
      return "GRow{ key=${key} }";
  }
}
