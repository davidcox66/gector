package org.gector.db.meta

import java.nio.ByteBuffer

import me.prettyprint.cassandra.serializers.StringSerializer
import me.prettyprint.cassandra.service.ThriftCfDef
import me.prettyprint.cassandra.service.ThriftColumnDef
import me.prettyprint.hector.api.Serializer
import me.prettyprint.hector.api.ddl.ColumnDefinition
import me.prettyprint.hector.api.ddl.ColumnFamilyDefinition

import org.apache.cassandra.thrift.ColumnDef
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import org.gector.db.GColumn
import org.gector.db.GHelper
import org.gector.db.GSubColumn

/**
 * The GAutoMetadataManager responds to notifications by creating keyspaces and column families
 * as needed and updating column metadata so values will be more clearly visible in the cassandra
 * command line interface. In addition to what GAutoMetadataManager does, this will write
 * validation class information to the database. This would only be useful if the columns 
 * were very static in nature where a given column name will always have a specific value type 
 * and the number of columns are not excesive. This would not make snese for columns names that are
 * dates, numbers, etc.
 *
 * @author david
 * @since Mar 28, 2011
 */
class GFullMetadataManager extends GAutoMetadataManager
{
  private static final Logger LOGGER = LoggerFactory.getLogger(GFullMetadataManager);

  /**
   * Callback for when we know the serializer for a column value
   *  
   * @param column
   * @param serializer
   */
  void notifyColumnValueSerializer( GColumn column, Serializer serializer, Object value ) {
    super.notifyColumnValueSerializer( column, serializer, value );
    updateColumnOrSuperColumnMetadata( column, serializer, value ); 
  }
 
  /**
   * Callback for when we know the data type of a sub column value
   * 
   * @param column
   * @param serializer
   */
  void notifySubColumnValueSerializer( GSubColumn column, Serializer serializer, Object value ) {
    super.notifySubColumnValueSerializer( column, serializer, value );
    updateColumnOrSuperColumnMetadata( column, serializer, value ); 
  }

  /** 
   * Adds column metadata if not already present for the given column or super column. This defines
   * the validator_class for the column value so that is may appear properly in the cassandra-cli.
   * 
   * Note, the parameter columnOrSuperColumn relies on the fact that Groovy is a duck-typed language.
   * GColumn and GSuperColumn do not implement a common interface though they both implement the
   * methods thati this update routine cares about so Groovy allows this to work properly.
   *  
   * @param columnOrSuperColumn
   * @param serializer
   * @return
   */
  protected updateColumnOrSuperColumnMetadata( def columnOrSuperColumn, Serializer serializer, Object value ) {
   
    if( columnOrSuperColumn.serializer != StringSerializer.get() )  {
      if( LOGGER.isDebugEnabled() ) {
        LOGGER.debug( "updateColumnOrSuperColumnMetaData: skipping non-string column: ${columnOrSuperColumn.fullyQualifiedName}");
      }
      return;   
    }
    if( GHelper.EMPTY.is(value) )  {
      if( LOGGER.isDebugEnabled() ) {
        LOGGER.debug( "updateColumnOrSuperColumnMetaData: skipping column-name-as-value: ${columnOrSuperColumn.fullyQualifiedName}");
      }
      return;   
    }
    
    ColumnFamilyDefinition cfDef = getColumnFamilyDefinition( columnOrSuperColumn.keyspace.keyspaceName, columnOrSuperColumn.columnFamilyName );
    ColumnDefinition colDef = getColumnDefinition( cfDef, columnOrSuperColumn.columnName );
   
    if( !colDef || !colDef.validationClass ) { 
      String validationClass = getValidationClassForSerializer( serializer );
      ByteBuffer columnNameBuffer = GHelper.toByteBuffer(columnOrSuperColumn.columnName);
      List<ColumnDefinition> newColumnMetadata = null;
      
      ColumnDef cColDef = new ColumnDef();
      cColDef.setName( columnNameBuffer );
      cColDef.setValidation_class( validationClass );
      ThriftColumnDef tColDef = new ThriftColumnDef( cColDef );
      
	    if( !colDef ) {
        if( LOGGER.isDebugEnabled() ) {
	        String serializerName = GHelper.getSerializerName( serializer );
		      LOGGER.debug( "updateColumnOrSuperColumnMetadata: adding column metadata - " +
		        "column: ${columnOrSuperColumn.fullyQualifiedName},  validation: ${validationClass}, serializer: ${serializerName}" );
        }
	      newColumnMetadata = new ArrayList<ColumnDefinition>( cfDef.columnMetadata );
	      newColumnMetadata.add( tColDef );
	    }
	    else if( !colDef.validationClass ) {
        if( LOGGER.isDebugEnabled() ) {
	        String serializerName = GHelper.getSerializerName( serializer );
		      LOGGER.debug( "updateColumnOrSuperColumnMetadata: updating column metadata - " +
		        "column: ${columnOrSuperColumn.fullyQualifiedName}, validation: ${validationClass}, serializer: ${serializerName}");
        }
	      newColumnMetadata = new ArrayList<ColumnDefinition>();
        cfDef.columnMetadata.each{ ColumnDefinition cd ->
          newColumnMetadata.add( columnNameBuffer != cd.name ? cd : tColDef ); 
        }
	    }
      ((ThriftCfDef)cfDef).setColumnMetadata( newColumnMetadata );
      cluster.cluster.updateColumnFamily( cfDef );
      refreshKeyspaceDefinitions(); 
    }
  }
}
