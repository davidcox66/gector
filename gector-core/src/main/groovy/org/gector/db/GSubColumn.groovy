package org.gector.db

import java.nio.ByteBuffer

import me.prettyprint.cassandra.serializers.BooleanSerializer
import me.prettyprint.cassandra.serializers.BytesArraySerializer
import me.prettyprint.cassandra.serializers.DateSerializer
import me.prettyprint.cassandra.serializers.DoubleSerializer
import me.prettyprint.cassandra.serializers.IntegerSerializer
import me.prettyprint.cassandra.serializers.LongSerializer
import me.prettyprint.cassandra.serializers.StringSerializer
import me.prettyprint.cassandra.serializers.UUIDSerializer
import me.prettyprint.hector.api.Serializer
import me.prettyprint.hector.api.beans.HColumn
import me.prettyprint.hector.api.factory.HFactory
import me.prettyprint.hector.api.mutation.MutationResult

import org.slf4j.Logger
import org.slf4j.LoggerFactory

import org.gector.db.update.GUpdater

/**
 * Allows associative-array or property style notation access to sub-columns.
 * 
 * @author david
 * @since Mar 29, 2011
 */
class GSubColumn
{
  private static final Logger LOGGER = LoggerFactory.getLogger(GSubColumn);
 
  /**
   * This sub-column name 
   */
  private Object columnName;
  
  /** 
   * The owning super column
   */
  private GColumn superColumn;
  
  /**
   * The sub-column name serializer 
   */
  private Serializer serializer;
  
  GSubColumn( Object columnName, Serializer serializer, GColumn superColumn ) {
    this.columnName = columnName;
    this.serializer = serializer;
    this.superColumn = superColumn;
    keyspace.metadata?.notifySubColumnNameSerializer( this );
  }

  /**
   * Deletes this sub-column from the database
   */
  MutationResult delete() {
    if( LOGGER.isDebugEnabled() ) { LOGGER.debug( "delete: ${fullyQualifiedLocation}" ); }
    return updater.subDelete(key, columnFamilyName, superColumn.columnName, columnName, superColumn.serializer, serializer);
  }
 
  /*
   * Various setter/getter methods for accessing the column value 
   */
  String getString() { 
    return getValue( StringSerializer.get() );
  }
  GSubColumn setString( String val ) {
    return this.leftShift( val );
  } 
  Long getLong() { 
    return getValue( LongSerializer.get() );
  }
  GSubColumn setLong( long val ) {
    return this.leftShift( val );
  } 
  Double getDouble() { 
    return getValue( DoubleSerializer.get() );
  }
  GSubColumn setDouble( double val ) {
    return this.leftShift( val );
  } 
  Integer getInt() { 
    return getValue( IntegerSerializer.get() );
  }
  GSubColumn setInt( int val ) {
    return this.leftShift( val );
  } 
  Boolean getBoolean() { 
    return getValue( BooleanSerializer.get() );
  }
  GSubColumn setBoolean( String val ) {
    return this.leftShift( val );
  } 
  Date getDate() { 
    return getValue( DateSerializer.get() );
  }
  GSubColumn setDate( Date val ) {
    return this.leftShift( val );
  } 
  byte[] getByteArray() { 
    return getValue( BytesArraySerializer.get() );
  }
  GSubColumn setByteArray( byte[] val ) {
    return this.leftShift( val );
  } 
  UUID getUUID() { 
    return getValue( UUIDSerializer.get() );
  }
  GSubColumn setUUID( UUID val ) {
    return this.leftShift( val );
  } 
  ByteBuffer getByteBuffer() { 
    return superColumn.getSubColumnValue( columnName );
  }
  GSubColumn setByteBuffer( ByteBuffer val ) {
    return this.leftShift( val );
  }
  
  /**
   * Inserts empty data for the value of a column so that the column can just be used for columns-as-values
   * 
   * @return
   */
  GSubColumn columnNameAsValue() {
    return this.setValue( GHelper.EMPTY, 0 );
  }  
  GSubColumn columnNameAsValue( int ttl ) {
    return this.setValue( GHelper.EMPTY, ttl );
  }  
  
  /** 
   * Underlying implementation method for the various value getter methods. This
   * asks the owning super-column to get the value either by pre-fetched values
   * or as a one-off. The column data is de-serialized using the given serializer.
   * 
   * @param valueSerializer
   * @return
   */
  private Object getValue( Serializer valueSerializer )  {
    keyspace.metadata?.notifySubColumnValueSerializer( this, valueSerializer, null );
    ByteBuffer buffer = superColumn.getSubColumnValue( columnName );
    return buffer != null ? GHelper.fromByteBuffer( buffer, valueSerializer ) : null;
  }

  GSubColumn leftShift( GString value ) {
    return setValue( value.toString() );
  }
  
  GSubColumn leftShift( Object value ) {
      return setValue( value );
  }
  
  GSubColumn setValue( Object value ) {
      return setValue( value, 0 );
  }
  /** 
   * Implementation method for the '<<' operator used to assign a value and used internally
   * by other setter methods. This asks the owning GColumn to add the update mutations, which
   * will gather the update into a single super-column mutation if called within
   * GColumn.update(). Otherwise, a one-off mutation will be created. 
   * 
   * @param value
   * @return
   */
  GSubColumn setValue( Object value, int ttl ) {
    try {
	    Serializer valueSerializer = GHelper.getSerializer(value);
	    if( LOGGER.isDebugEnabled() ) {
            String nameSerializerName = GHelper.getSerializerName( serializer );
	        String valueSerializerName = GHelper.getSerializerName( valueSerializer );
		      LOGGER.debug( "setValue: ${fullyQualifiedLocation} << ${value} (nameSerializer=${nameSerializerName}, valueSerializer=${valueSerializerName})");
	    }
	    keyspace.metadata?.notifySubColumnValueSerializer( this, valueSerializer, value );
		HColumn hSubColumn = HFactory.createColumn( columnName, value, serializer, valueSerializer );
        if( ttl > 0 ) {
	        hSubColumn.setTtl( ttl );
        }
	    superColumn.setSubColumnValue( hSubColumn );
	    return this;
    }
    catch( Exception ex ) {
      throw new GException( "Failed updating super column: ${fullyQualifiedLocation}, value: ${value}", ex );
    }
  }

  /**
   * @return the owning keyspace
   */
  GKeyspace getKeyspace() {
    return superColumn.keyspace;
  } 
 
  /**
   * @return the owning GCluster 
   */
  GCluster getCluster() {
    return keyspace.cluster;
  }
  /**
   * @return the updater for this column family
   */
  GUpdater getUpdater() {
    return superColumn.updater;
  }

  /**
   * @return the current row key being used
   */
  Object getKey() {
    return superColumn.key;
  }  

  /**
   * @return the column family to which this belongs
   */
  GColumnFamily getColumnFamily() {
    return superColumn.columnFamily;
  }
    
  /**
   * @return the name of the column family to which this belongs
   */
  String getColumnFamilyName() {
    return superColumn.columnFamilyName;
  }
 
  /**
   * @return the owning super-column's name
   */
  String getSuperColumnName() {
    return superColumn.columnName;
  }
  
  /**
   * @return the unique, dotted notation name to which this column refers in the format:
   * keyspace.columnFamily.superColumnName.subColumnName. This could be used in debugging output or as
   * an identifier/hash key for the column.
   */
  String getFullyQualifiedName() {
      return "${keyspace.keyspaceName}.${columnFamilyName}{${superColumn.columnName}}{${columnName}}";
  }
  
  /**
   * @return the unique, dotted notation name (including key) to which this column refers in the format:
   * keyspace.columnFamily[key].superColumnName.subColumName. This could be used in debugging output or as
   * an identifier/hash key for the column.
   */
  String getFullyQualifiedLocation() {
      return "${keyspace.keyspaceName}.${columnFamilyName}[${key}]{${superColumn.columnName}}{${columnName}}";
  }

}
