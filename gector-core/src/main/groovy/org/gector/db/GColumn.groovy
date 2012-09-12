package org.gector.db

import java.nio.ByteBuffer

import me.prettyprint.cassandra.serializers.BooleanSerializer
import me.prettyprint.cassandra.serializers.ByteBufferSerializer
import me.prettyprint.cassandra.serializers.BytesArraySerializer
import me.prettyprint.cassandra.serializers.DateSerializer
import me.prettyprint.cassandra.serializers.DoubleSerializer
import me.prettyprint.cassandra.serializers.IntegerSerializer
import me.prettyprint.cassandra.serializers.LongSerializer
import me.prettyprint.cassandra.serializers.StringSerializer
import me.prettyprint.cassandra.serializers.TypeInferringSerializer
import me.prettyprint.cassandra.serializers.UUIDSerializer
import me.prettyprint.hector.api.Serializer
import me.prettyprint.hector.api.beans.ColumnSlice
import me.prettyprint.hector.api.beans.HColumn
import me.prettyprint.hector.api.beans.HSuperColumn
import me.prettyprint.hector.api.beans.SuperSlice
import me.prettyprint.hector.api.factory.HFactory
import me.prettyprint.hector.api.mutation.MutationResult
import me.prettyprint.hector.api.query.Query
import me.prettyprint.hector.api.query.QueryResult
import me.prettyprint.hector.api.query.SubColumnQuery
import me.prettyprint.hector.api.query.SubCountQuery
import me.prettyprint.hector.api.query.SubSliceQuery

import org.slf4j.Logger
import org.slf4j.LoggerFactory

import org.gector.db.update.GUpdater;

/**
 * Provides simple associative-array or property notation access to individual columns within a specified
 * row. Also allows further access into sub-columns when using a super column family.
 * 
 * @author david
 * @since Mar 29, 2011
 */
class GColumn
{
  private static final Logger LOGGER = LoggerFactory.getLogger(GColumn);
 
  /**
   * Holds a list of pending updates to sub-columns when working within the context of the update() method.
   */
  private List subColumns; 
  
  /**
   * Holds a map of sub-column names/values when querying all of the sub-columns within the
   * context of the query method.
   */
  private Map subColumnsMap;

  /** 
   * The owning GRow 
   */
  final GRow row;
 
  /**
   * This object's column name 
   */
  final Object columnName;
  
  /**
   * The serializer for this column. This is determined by the data type used when addressing
   * this column from GRow. The serializer type is inferred from how this column is accessed. 
   */
  final Serializer serializer;
  
  
  GColumn( Object columnName, Serializer serializer, GRow row ) {
    this.columnName = columnName;
    this.serializer = serializer;
    this.row = row;
    keyspace.metadata?.notifyColumnNameSerializer( this );
  }
 
  /**
   * Removes this standard column from the database 
   * @return
   */
  MutationResult delete() {
    if( LOGGER.isDebugEnabled() ) { LOGGER.debug( "delete: ${fullyQualifiedLocation}" ); }
	  return updater.delete(key, columnFamilyName, columnName, serializer );
  }
 
  /**
   * Removes this standard column from the database 
   * @return
   */
  MutationResult deleteSuper() {
    if( LOGGER.isDebugEnabled() ) { LOGGER.debug( "deleteSuper: ${fullyQualifiedLocation}" ); }
	  return updater.superDelete(key, columnFamilyName, columnName, serializer );
  }
 
  /*
   * Various getAt() methods to allow associate-array notation for access to sub-columns. 
   */
  GSubColumn getAt( GString subColumnName )  {
    return getAtImpl( subColumnName.toString() );
  }
  GSubColumn getAt( String subColumnName )  {
    return getAtImpl( subColumnName );
  }
  GSubColumn getAt( long subColumnName )  {
    return getAtImpl( subColumnName );
  }
  GSubColumn getAt( double subColumnName )  {
    return getAtImpl( subColumnName );
  }
  GSubColumn getAt( int subColumnName )  {
    return getAtImpl( subColumnName );
  }
  GSubColumn getAt( boolean subColumnName )  {
    return getAtImpl( subColumnName );
  }
  GSubColumn getAt( Date subColumnName )  {
    return getAtImpl( subColumnName );
  }
  GSubColumn getAt( byte[] subColumnName )  {
    return getAtImpl( subColumnName );
  }
  GSubColumn getAt( ByteBuffer subColumnName )  {
    return getAtImpl( subColumnName );
  }
  GSubColumn getAt( UUID subColumnName )  {
    return getAtImpl( subColumnName );
  }

  /**
   * Allow property style notation to refer to a sub-column. This is not as efficient as it propertyMissing()
   * is invoked from the error-handling path in Groovy's property resolution. This could be changed later
   * if needed for a more efficient implementation.
   * 
   * @param columnName
   * @return
   */
  GSubColumn propertyMissing( String columnName ) {
    return getAtImpl( columnName );
  }
  
  /**
   * Underlying implementation method for all of the getAt()/propertyMissing() methods to retrieve
   * a specified sub-column.
   * 
   * @param subColumnName
   * @return
   */
  private GSubColumn getAtImpl( Object subColumnName )
  {
    Serializer subColumnNameSerializer = GHelper.getSerializer(subColumnName);
    return new GSubColumn( subColumnName, subColumnNameSerializer, this );
  }

  /*
   * Various setter/setter methods to allow read/write access to this column value. 
   */
  String getString() { 
    return getValue( StringSerializer.get() );
  }
  GColumn setString( String val ) {
    return this.leftShift( val );
  } 
  Long getLong() { 
    return getValue( LongSerializer.get() );
  }
  GColumn setLong( long val ) {
    return this.leftShift( val );
  } 
  Double getDouble() { 
    return getValue( DoubleSerializer.get() );
  }
  GColumn setDouble( double val ) {
    return this.leftShift( val );
  } 
  Integer getInt() { 
    return getValue( IntegerSerializer.get() );
  }
  GColumn setInt( int val ) {
    return this.leftShift( val );
  } 
  Boolean getBoolean() { 
    return getValue( BooleanSerializer.get() );
  }
  GColumn setBoolean( String val ) {
    return this.leftShift( val );
  } 
  Date getDate() { 
    return getValue( DateSerializer.get() );
  }
  GColumn setDate( Date val ) {
    return this.leftShift( val );
  } 
  byte[] getByteArray() { 
    return getValue( BytesArraySerializer.get() );
  }
  GColumn setByteArray( byte[] val ) {
    return this.leftShift( val );
  } 
  UUID getUUID() { 
    return getValue( UUIDSerializer.get() );
  }
  GColumn setUUID( UUID val ) {
    return this.leftShift( val );
  } 
  ByteBuffer getByteBuffer() { 
    return row.getColumnValue( columnName );
  }
  GColumn setByteBuffer( ByteBuffer val ) {
    return this.leftShift( val );
  }

  /**
   * Inserts empty data for the value of a column so that the column can just be used for column-name-as-values
   * 
   * @return
   */
  GColumn columnNameAsValue() {
    return this.setValue( GHelper.EMPTY, 0 );
  }  
  GColumn columnNameAsValue( int ttl ) {
    return this.setValue( GHelper.EMPTY, ttl );
  }  
  
  /**
   * Underlying implementation method to fetch this column's value and de-serialize it.
   * 
   * @param valueSerializer
   * @return
   */
  private Object getValue( Serializer valueSerializer )  {
    keyspace.metadata?.notifyColumnValueSerializer( this, valueSerializer, null );
    ByteBuffer buffer = row.getColumnValue( columnName );
    return buffer != null ? GHelper.fromByteBuffer( buffer, valueSerializer ) : null;
  }
  
  /**
   * This allows multiple updates of sub-columns to be bundled up into one super column mutation.
   * All GSubColumn updates within the context of this update() will be gathered into one 
   * super column mutation.
   * 
   * @param closure
   */
  void update( Closure closure ) {
      subColumns = new ArrayList();
      try { 
	      closure.call( this );
	      
	      HSuperColumn hSuperColumn = HFactory.createSuperColumn( columnName, subColumns, serializer, 
          TypeInferringSerializer.get(), TypeInferringSerializer.get() );
			  updater.insert( key, columnFamilyName, hSuperColumn  );
      }
      finally {
	      subColumns = null; 
      }
  }

  /** 
   * Just a general untyped method for assigning an arbitrary value to the column value without being 
   * concerned about its data type.
   *  
   * @param value
   * @return
   */
  GColumn leftShift( Object value ) {
    return this.setValue( value, 0 );  
  }
 
  /**
   * Override specifically for the Groovy string type so that GStrings will be property stored as 
   * java.lang.String instead of serialized objects.
   *  
   * @param value
   * @return
   */
  GColumn leftShift( GString value ) {
    return setValue( value?.toString(), 0 );
  }
  
  GColumn setValue( Object value ) {
      return setValue( value, 0 );
  }
  
  /**
   * This implements the functionality of the '<<' operator and is used internally by other
   * assignment methods. This add the mutation for given column value. 
   * 
   * @param value
   * @return
   */
  GColumn setValue( Object value, int ttl ) {
    if( value == null ) {
      throw new GException( "Value cannot be null: ${fullyQualifiedLocation}" );
    }
    try {
	    Serializer valueSerializer = GHelper.getSerializer(value);
	    keyspace.metadata?.notifyColumnValueSerializer( this, valueSerializer, value );
	    HColumn column = HFactory.createColumn( columnName, value, serializer, valueSerializer );
        if( ttl > 0 ) {
	        column.setTtl( ttl );
        }
	    if( LOGGER.isDebugEnabled() ) {
            String nameSerializerName = GHelper.getSerializerName( serializer );
	        String valueSerializerName = GHelper.getSerializerName( valueSerializer );
		    LOGGER.debug( "setValue: ${fullyQualifiedLocation} << ${value} (nameSerializer=${nameSerializerName}, valueSerializer=${valueSerializerName})");
	    }
	    updater.insert( key, columnFamilyName, column );
	    return this;
    }
    catch( Exception ex ) {
      throw new GException( "Error updating: ${fullyQualifiedLocation}", ex );
      
    }
  }

  /**
   * @return the key for this row
   */
  Object getKey() {
    return row.key;
  }  
 
  /**
   * @return the owning GKeyspace
   */
  GKeyspace getKeyspace() {
    return row.keyspace;
  }   

  String getKeyspaceName() {
    return keyspace.keyspaceName;
  } 
  
  /**
   * @return the owning GCluster 
   */
  GCluster getCluster() {
    return keyspace.cluster;
  }
  
  /**
   * @return the GUpdater for this column family
   */
  GUpdater getUpdater() {
    return row.updater;
  }

  /**
   * @return the owning column family
   */
  GColumnFamily getColumnFamily() {
    return row.columnFamily;
  } 
 
  /**
   * @return the name of the owning column family
   */
  String getColumnFamilyName() {
    return row.columnFamilyName;
  }
 
  /**
   * @return the unique, dotted notation name to which this column refers in the format:
   * keyspace.columnFamily.columnName. This could be used in debugging output or as
   * an identifier/hash key for the column.
   */
  String getFullyQualifiedName() {
      return "${keyspace.keyspaceName}.${columnFamilyName}{${columnName}}";
  }
  
  /**
   * @return the unique, dotted notation name (including key) to which this column refers in the format:
   * keyspace.columnFamily[key].columnName. This could be used in debugging output or as
   * an identifier/hash key for the column.
   */
  String getFullyQualifiedLocation() {
      return "${keyspace.keyspaceName}.${columnFamilyName}[${key}]{${columnName}}";
  }
 
  /**
   * Used by GSubColumn to get individual sub-column values. This will use pre-fetched
   * query results if within the context of the query() method or fetch the sub-column
   * value as a less efficient one-off query if not within the query() context. If this
   * is within the larger context of a multi-get on the column family, it will attempt
   * to get values from there.
   * 
   * @param subColumnName
   * @return
   */
  ByteBuffer getSubColumnValue( Object subColumnName ) {
    if( subColumnsMap ) {
	    HColumn col = subColumnsMap.get( GHelper.toByteBuffer(subColumnName) );
      if( col ) {
		    return col.getValue();
      }
    }
    else { 
      return querySubColumn( subColumnName );
    }
  }

  /**
   * Fetches a single sub column and returns its value as a ByteBuffer
   * 
   * @param subColumnName
   * @return
   */
  private ByteBuffer querySubColumn( Object subColumnName ) 
  {
    Serializer subColumnNameSerializer = GHelper.getSerializer(subColumnName);
    SubColumnQuery query = HFactory.createSubColumnQuery( 
      keyspace.keyspace, columnFamily.serializer, serializer, subColumnNameSerializer, ByteBufferSerializer.get() );  
     query.setKey( key ); 
     query.setColumnFamily( columnFamilyName );
     query.setSuperColumn( columnName );
     query.setColumn( subColumnName ); 
     HColumn col = query.execute()?.get();
     return col?.getValue();
  }
 
  /**
   * Used by GSubColumn to update values in their sub-column. This method will gather
   * the updates into a single super-column mutation if called within the context of
   * update(). Otherwise, a one-off super-column update will be created with the 
   * single sub-column.
   * 
   * @param hSubColumn
   */
  void setSubColumnValue( HColumn hSubColumn )
  {
    if( subColumns == null ) {
      List subs = new ArrayList();
	    subs.add( hSubColumn );
			HSuperColumn hSuperColumn = HFactory.createSuperColumn( columnName, subs, 
        serializer, hSubColumn.nameSerializer, hSubColumn.valueSerializer );
			updater.insert( key, columnFamilyName, hSuperColumn  );
    }
	  // When used inside of GColumns.values{ }
	  else {
      subColumns.add( hSubColumn );  
	  }
  } 
  
	/**
	 * Checks if there are any columns at a row specified by key in a specific super column
	 * 
	 * @return true if columns exist
	 */
	boolean isSubColumnsExist() {
	    return countSubColumns() > 0;
	}
	
	/**
	 * @return the number of child columns in a specified super column
	 */
	int countSubColumns() {
	    return countSubColumns( null, null, Integer.MAX_VALUE );
	}
	
	/**
	 * Counts child columns in the specified range of a children in a specified super column
	 * 
	 * @param start
	 * @param end
	 * @param max
	 * @return
	 */
	int countSubColumns(final Object start, final Object end, final int max) {
	    final SubCountQuery query = HFactory.createSubCountQuery(keyspace, columnFamily.serializer, serializer, ByteBufferSerializer.get() );
	    query.setKey(key);
	    query.setColumnFamily(columnFamilyName);
	    query.setSuperColumn(columnName);
	    query.setRange(start, end, max);
	    return query.execute().get();
	}
	
  /**
   * Allows more efficient access to individual sub-columns by pre-fetching all of the 
   * sub-column values. Any sub-column access within the context of the query() method
   * will use these pre-fetched results.
   *  
   * @param closure
   */
  GColumn query( Closure closure=null ) {  
    return query( null, null, Integer.MAX_VALUE, false, closure );
  }
  
  /**
   * Allows more efficient access to individual sub-columns by pre-fetching all of the 
   * sub-column values. Any sub-column access within the context of the query() method
   * will use these pre-fetched results.
   *  
   * @param closure
   */
  GColumn query( Object start, Object end, Closure closure=null ) {  
    return query( start, end, Integer.MAX_VALUE, false, closure );
  }
  
  /**
   * Allows more efficient access to individual sub-columns by pre-fetching all of the 
   * sub-column values. Any sub-column access within the context of the query() method
   * will use these pre-fetched results.
   *  
   * @param closure
   */
  GColumn query( Object start, Object end, int max, Closure closure=null ) {  
    return query( start, end, max, false, closure );
  }

  /**
   * Allows more efficient access to individual sub-columns by pre-fetching all of the 
   * sub-column values. Any sub-column access within the context of the query() method
   * will use these pre-fetched results.
   *  
   * @param closure
   */
  GColumn query( Object start, Object end, int max, boolean reversed, Closure closure=null ) {  
    SubSliceQuery query = createSubSliceQuery();
    query.setRange( GHelper.toByteBuffer(start), GHelper.toByteBuffer(end), reversed, max );
    return executeQuery( query, closure );
  }

  /**
   * Allows more efficient access to individual sub-columns by pre-fetching all of the 
   * sub-column values. Any sub-column access within the context of the query() method
   * will use these pre-fetched results.
   *  
   * @param closure
   */
  GColumn query( List columns, Closure closure=null ) {  
    SubSliceQuery query = createSubSliceQuery();
    query.setColumnNames( GHelper.toByteBufferArray(columns) );
    return executeQuery( query, closure );
  }
  /**
   * Execute the given  query. This calls the given closure if present and returns a
   * an Iterable which can optionally be used to iterate over the results.
   *  
   * @param query
   * @param closure
   * @return
   */
  private GColumn executeQuery( Query query, Closure closure ) {
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
  private GColumn callForQuerySlice( def result, Closure closure ) {
    if( closure ) {
      try {
	      subColumnsMap = queryResultsToMap(result);
	      closure.call( this );
      }
      finally {
        subColumnsMap = null;
      } 
      return null;
    }
    else {
	    GColumn ret = new GColumn(columnName,serializer,row);
	    ret.subColumnsMap = queryResultsToMap(result);
	    return ret;
    }
  } 
  
  /** 
   * Iterates over each sub column within the current query slice for this column. It is assumed that this is called
   * within the context of calling one of the query methods of column,  row, or multi-get on the column family.
   * This will search for this column in those previously queries results. 
   * 
   * @param nameClass the class of the super column. We must know this upfront in order to serialize the column name we wish to find 
   * @param closure the closure which will operate on each column in the results
   */
  void eachSubSliceColumn( Class nameClass, Closure closure )  {
    if( subColumnsMap ) {
      iterateOverSubColumns( nameClass, subColumnsMap.values(), closure );
    }
    else {
	    def subSlice = findOrQuerySlice();
	    HSuperColumn superColumn = getSuperColumnInSlice( subSlice );
      if( LOGGER.isDebugEnabled() ) { 
	      String str = GHelper.toString( subSlice, nameClass, null );
	      LOGGER.debug( "eachSubSliceColumn: columnName=${columnName}, subSlice=${str}, superColumn=${superColumn}"); 
      }
      if( superColumn != null ) { 
	      try {
			      subColumnsMap = queryResultsToMap( superColumn );
		        iterateOverSubColumns( nameClass, superColumn.columns, closure );
	      }
	      finally {
	        subColumnsMap = null;
	      }
      }
    }
  }
  
  /** 
   * Fetches the results of a previous query done by the GRow or GColumnFamily and puts them in a temporary map
   * for use by GSubColumns within the given closure for fast access to the previously queried values 
   */
  void withSubColumns( Closure closure )  {
    if( subColumnsMap ) {
      closure.call( this );
    }
    else {
	    def subSlice = findOrQuerySlice();
	    HSuperColumn superColumn = getSuperColumnInSlice( subSlice );
	    if( LOGGER.isDebugEnabled() ) { LOGGER.debug( "useSubSliceColumns: columnName=${columnName}, superColumn=${superColumn}"); }
	    if( superColumn != null ) { 
	      callForQuerySlice( superColumn, closure );
	    }
    }
  }
  
  void iterateOverSubColumns( Class nameClass, List<HColumn> columns, Closure closure )  {
    Serializer nameSerializer = GHelper.getSerializer( nameClass );
    columns.each{ HColumn col ->
      Object subName = GHelper.fromByteBuffer( col.getName(), nameSerializer );    
      GSubColumn gcol = new GSubColumn( subName, nameSerializer, this ); 
      closure.call( gcol );
    }
  }
  
  /**
   * Fetches all of the sub-column name/values. If within the context of a multi-get on the column
   * family, this will attempt to get results from there, or if failing that, will attempt the 
   * query itself.
   * 
   * @return
   */
  def findOrQuerySlice() {  
    def sliceOrSuperSlice = row.findColumnSlice();
    if( !sliceOrSuperSlice ) {
      LOGGER.debug( 'findOrQuerySlice: no cached results found, querying...' ); 
    }
    else {
      if( LOGGER.isDebugEnabled() ) { LOGGER.debug( "findOrQuerySlice: using row column slice=${sliceOrSuperSlice}"); }
    }
    return sliceOrSuperSlice != null ? sliceOrSuperSlice : querySlice();
  }
 
  /**
   * Will fetch all of the sub-columns for this super column. This is called when not called within the context
   * of the column family or row query and results must be fetched on the fly. This may not be the most efficient
   * as is queries all sub columns with the expectation that they may all need to be referenced and are pre-fetched
   * as an optimization.
   * 
   * super columns 
   * @return
   */
  ColumnSlice querySlice() {  
      if( LOGGER.isDebugEnabled() ) { LOGGER.debug( "querySlice: creating query - key={${key}}, column={${columnName}}"); }
	    SubSliceQuery query = createSubSliceQuery();
      query.setRange( null, null, false, Integer.MAX_VALUE );
	    // query.setColumnNames( GHelper.toByteBuffer(columnName) );
      // NOTE: see serializer information in createSuperSliceQuery() 
	    ColumnSlice subSlice = query.execute().get();
      if( LOGGER.isDebugEnabled() ) { LOGGER.debug( "querySlice: query slice=${subSlice}"); }
      return subSlice;
  }

  /**
   * Looks for our super column in the given slice of a previously run query.
   *  
   * @param slice
   * @return
   */
  private HSuperColumn getSuperColumnInSlice( SuperSlice slice ) {
    HSuperColumn ret = null;
    if( slice != null ) {
      // NOTE: see serializer information in createSuperSliceQuery() 
	    ret = slice.getColumnByName( GHelper.toByteBuffer(columnName) );
      if( !ret && LOGGER.isDebugEnabled() ) {
        String str = GHelper.toString( slice, serializer, null );
        LOGGER.debug( "getSuperColumnInSlice: super column {${columnName}} not found in: ${str}" );
      }
    }
    return ret;
  }  
  
  /**
   * Converts the results of a super slice query into a Map for easy digestion by individual column value
   * getters. 
   * @param subSlice
   * @return
   */
  private Map queryResultsToMap( SuperSlice slice ) {
    if( slice != null ) {
	    HSuperColumn scol = getSuperColumnInSlice( slice );
      return queryResultsToMap( scol );
    }
    return null;
  } 

  /**
   * Converts all of the sub columns of the given super column to a map keyed by column name. This method is
   * overloaded to deal with either super slices or super columns so that calling methods can be unaware of
   * which type they are dealing with.
   * 
   * @param scol
   * @return
   */
  private Map queryResultsToMap( HSuperColumn scol ) {
	  if( scol != null ) { 
      Map colsMap = new HashMap();
      scol.columns.each{ HColumn col ->
        colsMap.put( col.name, col );
      }
      return colsMap;
	  }
    return null;
  }
 
  /**
   * Converts the results of a super slice query into a Map for easy digestion by individual column value
   * getters. 
   * @param subSlice
   * @return
   */
  private Map queryResultsToMap( ColumnSlice slice ) {
    if( slice != null ) {
      // NOTE: see serializer information in createSuperSliceQuery() 
      List cols = slice.getColumns();
      if( cols != null ) { 
	      Map colsMap = new HashMap();
	      cols.each{ HColumn col ->
	        colsMap.put( col.name, col );
	      }
        return colsMap;
      }
    }
    return null;
  } 

  /*  
  private SuperSliceQuery createSuperSliceQuery()
  {
    //
    // NOTE: Using ByteBufferSerializer even though we know the serializer type for this column. This is to
    // suppor doing multi-get queries from the column family level where you won't yet know what the serializer
    // type is going to be. We unify the usage internally, of super column queries using ByteBuffer, so that
    // data is accessed uniformly
    //
    SuperSliceQuery query = 
      HFactory.createSuperSliceQuery( keyspace.keyspace, columnFamily.serializer, ByteBufferSerializer.get(), ByteBufferSerializer.get(), ByteBufferSerializer.get() );
    query.setKey( key );
    query.setColumnFamily( columnFamilyName );
    return query; 
  }
  */
  
  /**
   * Underlying implementation method to create a sub column slice query for the current super column.
   * @return
   */
  private SubSliceQuery createSubSliceQuery()
  {
    //
    // NOTE: Using ByteBufferSerializer even though we know the serializer type for this column. This is to
    // support doing multi-get queries from the column family level where you won't yet know what the serializer
    // type is going to be. We unify the usage internally, of super column queries using ByteBuffer, so that
    // data is accessed uniformly
    //
    Serializer bser = ByteBufferSerializer.get();
    SubSliceQuery query = HFactory.createSubSliceQuery( keyspace.keyspace, columnFamily.serializer, bser, bser, bser );
    query.setKey( key );
    query.setColumnFamily( columnFamilyName );
    query.setSuperColumn( GHelper.toByteBuffer(columnName) );
    return query; 
  }
  
  String toString() {
      return "GColumn{ columnName=${columnName} }";
  }
}
