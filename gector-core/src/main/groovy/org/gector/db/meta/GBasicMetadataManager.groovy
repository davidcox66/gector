package org.gector.db.meta

import java.nio.ByteBuffer

import me.prettyprint.cassandra.serializers.BooleanSerializer
import me.prettyprint.cassandra.serializers.ByteBufferSerializer
import me.prettyprint.cassandra.serializers.BytesArraySerializer
import me.prettyprint.cassandra.serializers.DateSerializer
import me.prettyprint.cassandra.serializers.DoubleSerializer
import me.prettyprint.cassandra.serializers.IntegerSerializer
import me.prettyprint.cassandra.serializers.LongSerializer
import me.prettyprint.cassandra.serializers.ObjectSerializer
import me.prettyprint.cassandra.serializers.StringSerializer
import me.prettyprint.cassandra.serializers.UUIDSerializer
import me.prettyprint.cassandra.service.ThriftCfDef
import me.prettyprint.cassandra.service.ThriftKsDef
import me.prettyprint.hector.api.Serializer
import me.prettyprint.hector.api.ddl.ColumnDefinition
import me.prettyprint.hector.api.ddl.ColumnFamilyDefinition
import me.prettyprint.hector.api.ddl.ColumnType
import me.prettyprint.hector.api.ddl.ComparatorType
import me.prettyprint.hector.api.ddl.KeyspaceDefinition
import me.prettyprint.hector.api.factory.HFactory

import org.slf4j.Logger
import org.slf4j.LoggerFactory

import org.gector.db.GCluster
import org.gector.db.GColumn
import org.gector.db.GColumnFamily
import org.gector.db.GException
import org.gector.db.GHelper
import org.gector.db.GKeyspace
import org.gector.db.GSubColumn

/**
 * The GBasicMetadataManager ignores notifications to creating keyspaces and column families
 * and acts as a base class for use by other implementations. It may also be used in 
 * production environments where you want some metadata functionality but no actual updates
 * to the schema.
 * 
 * @author david
 * @since Mar 28, 2011
 */
class GBasicMetadataManager implements GMetadataManager
{
  private static final Logger LOGGER = LoggerFactory.getLogger(GBasicMetadataManager);

  GCluster cluster;
  protected Map<String,KeyspaceDefinition> keyspaceDefinitions = new HashMap<String,KeyspaceDefinition>();
  protected int replicationFactor = 1;
  
  /**
   * Most of the data types manipulation is concerned with serializers. ComparatorType is neededed when
   * manipulating the metadata. Therefore, this map allows the mapping of data types when creating
   * Cassandra metadata from updates/queries being requested. 
   */
  private static Map<Serializer,ComparatorType> serializerToComparatorTypes = new HashMap<Serializer,ComparatorType>();
  static {
    serializerToComparatorTypes.put(BooleanSerializer.get(), ComparatorType.BYTESTYPE );
    serializerToComparatorTypes.put(BytesArraySerializer.get(), ComparatorType.BYTESTYPE );
    serializerToComparatorTypes.put(ByteBufferSerializer.get(), ComparatorType.BYTESTYPE );
    serializerToComparatorTypes.put(DateSerializer.get(), ComparatorType.LONGTYPE );
    serializerToComparatorTypes.put(DoubleSerializer.get(), ComparatorType.BYTESTYPE );
    serializerToComparatorTypes.put(IntegerSerializer.get(), ComparatorType.INTEGERTYPE );
    serializerToComparatorTypes.put(LongSerializer.get(), ComparatorType.LONGTYPE );
    serializerToComparatorTypes.put(ObjectSerializer.get(), ComparatorType.BYTESTYPE );
    serializerToComparatorTypes.put(StringSerializer.get(), ComparatorType.UTF8TYPE );
    serializerToComparatorTypes.put(UUIDSerializer.get(), ComparatorType.BYTESTYPE );
  }

  GBasicMetadataManager() {
    setReplicationFactor( Integer.parseInt(System.getProperty('cassandra.replicationFactor','1')));
  }
  
  void setCluster( GCluster cluster ) {
      this.cluster = cluster;
  }
  
  void setReplicationFactor( int replicationFactor ) {
    this.replicationFactor = replicationFactor;
  }
  
  boolean isStandardColumnFamily( String keyspaceName, String columnFamilyName ) {
    return getColumnFamilyType(keyspaceName,columnFamilyName) == ColumnType.STANDARD;
  }

  boolean isSuperColumnFamily( String keyspaceName, String columnFamilyName ) {
    return getColumnFamilyType(keyspaceName,columnFamilyName) == ColumnType.SUPER;
  }

  private ColumnType getColumnFamilyType( String columnFamilyName ) {
    ColumnFamilyDefinition cf = getColumnFamilyDefinition( columnFamilyName );
    if( !cf ) {
      throw new GException( "Column familiy does not exist: ${columnFamilyName}" );
    }
    return cf.getColumnType();
  }
  
  /** 
   * Callback whenever an operation is occurring that must have the given keyspace defined.
   * 
   * @param keyspace
   */
  void notifyNeedingKeyspace( String keyspaceName ) {
    if( LOGGER.isDebugEnabled() ) { LOGGER.debug( "notifyNeedingKeyspace: keyspaceName=${keyspaceName}"); }
  }

  /**
   * Callback whenever and operation is occurring that must have the given column family defined 
   * 
   * @param columnFamily
   */
  void notifyNeedingColumnFamily( GColumn column ) {
    if( LOGGER.isDebugEnabled() ) {LOGGER.debug( "notifyNeedingKeyspace: column=${column}"); }
  }

  /**
   * Callback whenever and operation is occurring that must have the given column family defined 
   * 
   * @param column
   */
  void notifyNeedingSuperColumnFamily( GSubColumn subColumn ) {
    if( LOGGER.isDebugEnabled() ) { LOGGER.debug( "notifyNeedingSuperColumnFamily: subColumn=${subColumn}"); }
  }

  /**
   * Callback for when we know the type of key serializer for a given column family
   *  
   * @param columnFamily
   */
  void notifyKeySerializer( GColumnFamily columnFamily ) {
    if( LOGGER.isDebugEnabled() ) { LOGGER.debug( "notifyKeySerializer: columnFamily=${columnFamily}"); }
  }

  /**
   * Callback for when we know the serializer for a given column
   *  
   * @param column
   */
  void notifyColumnNameSerializer( GColumn column ) {
    if( LOGGER.isDebugEnabled() ) { LOGGER.debug( "notifyColumnNameSerializer: column=${column}"); }
  }

  /**
   * Callback for when we know the serializer for a column value
   *  
   * @param column
   * @param serializer
   */
  void notifyColumnValueSerializer( GColumn column, Serializer serializer, Object value ) {
    if( LOGGER.isDebugEnabled() ) { LOGGER.debug( "notifyColumnValueSerializer: column=${column}, serializer=${serializer}, value=${value}"); }
  }

  /**
   * Callback for when we know the serializer for a super column name 
   * 
   * @param column
   */
  void notifySubColumnNameSerializer( GSubColumn column ) {
    if( LOGGER.isDebugEnabled() ) { LOGGER.debug( "notifySubColumnNameSerializer: column=${column}"); }
  }

  /**
   * Callback for when we know the data type of a sub column value
   * 
   * @param column
   * @param serializer
   */
  void notifySubColumnValueSerializer( GSubColumn column, Serializer serializer, Object value ) {
    if( LOGGER.isDebugEnabled() ) { LOGGER.debug( "notifySubColumnValueSerializer: column=${column}, serializer=${serializer}, value=${value}"); }
  }

  void notifyDropKeyspace( GKeyspace keyspace ) {
    if( LOGGER.isDebugEnabled() ) { LOGGER.debug( "notifyDropKeyspace: dropped '${keyspace.keyspaceName}" ); }
    refreshKeyspaceDefinitions();
  }

  void notifyDropColumnFamily( GColumnFamily columnFamily ) {
    if( LOGGER.isDebugEnabled() ) { LOGGER.debug( "notifyDropColumnFamily: dropped '${columnFamily.columnFamilyName}" ); }
    refreshKeyspaceDefinitions();
  }

  /**
   * Creates a keyspace of the given name in the cluster.
   *  
   * @param keyspaceName
   */
  void createKeyspace( String keyspaceName ) {
    LOGGER.info( "createKeyspace: creating keyspace '${keyspaceName}'");
    KeyspaceDefinition ksDefinition = HFactory.createKeyspaceDefinition( keyspaceName );
    ((ThriftKsDef)ksDefinition).setReplicationFactor( replicationFactor );
    cluster.withSchemaSynchronization{
      cluster.cluster.addKeyspace( ksDefinition );
    }
    refreshKeyspaceDefinitions();
  }
 
  /**
   * Creates a standard column family with the given comparator type
   * 
   * @param keyspace
   * @param columnFamily
   * @param comparator
   */
  void createColumnFamily( String keyspace, String columnFamily, ComparatorType comparator ) {
    LOGGER.info( "createColumnFamily: creating column family '${columnFamily}' with comparator '${comparator.className}'");
    try {
      ColumnFamilyDefinition cfDefinition = HFactory.createColumnFamilyDefinition( keyspace, columnFamily, comparator, new ArrayList<ColumnDefinition>() );
	    cluster.withSchemaSynchronization{
	      cluster.cluster.addColumnFamily( cfDefinition );
	    }
    }
    catch( Exception ex ) {
      LOGGER.error( "Error creating column family: keyspace=${keyspace}, columnFamily=${columnFamily}, comparator=${comparator}", ex );
      throw ex;
    }
    refreshKeyspaceDefinitions();
  }
 
  /**
   * Creates a super column family with the given column name comparator types
   *  
   * @param keyspace
   * @param superColumnFamily
   * @param comparator
   * @param subComparator
   */
  void createSuperColumnFamily( String keyspace, String superColumnFamily, ComparatorType comparator, ComparatorType subComparator ) {
    LOGGER.info( "createSuperColumnFamily: creating column family '${superColumnFamily}' with comparator '${comparator.className}' and subcomparator '${subComparator.className}'");
    try {
		  // Hector interface forgot access to key data, so have to refer to Thrift implementation directly :-(
		  ThriftCfDef cfDefinition = (ThriftCfDef)HFactory.createColumnFamilyDefinition( keyspace, superColumnFamily, comparator, new ArrayList<ColumnDefinition>() );
		  cfDefinition.setColumnType( ColumnType.SUPER );
		  cfDefinition.setSubComparatorType( subComparator );
	    cluster.withSchemaSynchronization{
	      cluster.cluster.addColumnFamily( cfDefinition );
	    }
	    refreshKeyspaceDefinitions();
    }
    catch( Exception ex ) {
      LOGGER.error( "Error creating column family: keyspace=${keyspace}, superColumnFamily=${superColumnFamily}, comparator=${comparator}, subComparator=${subComparator}", ex );
      throw ex;
    }
  }
  /**
   * Converts the serializer for a column value into a validator_class appropriate for definition column
   * metadata for column values
   * 
   * @param serializer
   * @return
   */
  protected String getValidationClassForSerializer( Serializer serializer ) {
    ComparatorType comparator = getComparatorType( serializer );
    String validationClass = comparator.getClassName();
    return validationClass.substring( validationClass.lastIndexOf('.')+1 );
  }

  public static ComparatorType getComparatorType( Class cls ) {
    if( !cls ) {
      throw new GException( "Cannot find comparator for null class" );
    }
    return getComparatorType( GHelper.getSerializer(cls) );
  }
  
  /**
   * Converts the serializer type for a column or super column name into a ComparatorType for use
   * in defining a column
   *  
   * @param serializer
   * @return
   */
  public static ComparatorType getComparatorType( Serializer serializer )  {
    if( !serializer ) {
      throw new GException( "Cannot find comparator for null serializer" );
    }
    ComparatorType ct = serializerToComparatorTypes[serializer];
    if( !ct ) {
      throw new GException( "No mapping defined for serializer to comparator: serializer=${serializer}" );
    }
    return ct;
  }

  /**
   *  
   * @param keyspaceName
   * @return true if a keyspace of the given name exists
   */
  boolean isExistingKeyspace( String keyspaceName ) {
    return getKeyspaceDefinition( keyspaceName ) != null;
  }

  /**
   *  
   * @param keyspaceName
   * @param columnFamilyName
   * @return true if a column/super column family exists with the given name
   */
  boolean isExistingColumnFamily( String keyspaceName, String columnFamilyName ) {
    return getColumnFamilyDefinition( keyspaceName, columnFamilyName ) != null;
  }

  /**
   * @param keyspaceName
   * @param columnFamilyName
   * @param columnName
   * @return the specified column definition, or null if none found
   */
  protected ColumnDefinition getColumnDefinition( String keyspaceName, String columnFamilyName, Object columnName ) {
    ColumnFamilyDefinition cfDef = getColumnFamilyDefinition( column.keyspace.keyspaceName, column.columnFamilyName );
    return getColumnDefinition( cfDef, columnName );
  }

  /**
   * @param cfDef
   * @param columnName
   * @return the column definition within the given column family, or null if not found
   */
  protected ColumnDefinition getColumnDefinition( ColumnFamilyDefinition cfDef, Object columnName ) {
    if( cfDef ) {
      ByteBuffer columnNameBuffer = GHelper.toByteBuffer( columnName );
      return cfDef.columnMetadata.find{ it.name == columnNameBuffer };
    }
    return null;
  }

  /**
   * @param keyspaceName
   * @param columnFamilyName
   * @return the specified column definition, or null if none exists
   */
  protected ColumnFamilyDefinition getColumnFamilyDefinition( String keyspaceName, String columnFamilyName ) {
    KeyspaceDefinition keyspaceDefinition = getKeyspaceDefinition( keyspaceName );
    if( keyspaceDefinition ) {
      return keyspaceDefinition.cfDefs.find{ it.name == columnFamilyName };
    }
    return null;
  }

  /**
   * Fetches and caches keyspace definitions for Cassandra and returns the keyspace of the given name,
   * or null if none exits.
   * 
   * @param keyspaceName
   * @return
   */
  synchronized protected KeyspaceDefinition getKeyspaceDefinition( String keyspaceName ) {
    if( !keyspaceDefinitions ) {
      List<KeyspaceDefinition> kdList = cluster.cluster.describeKeyspaces();
      kdList.each{ KeyspaceDefinition kd ->
        keyspaceDefinitions[kd.name] = kd;
      }
    }
    return keyspaceDefinitions[keyspaceName];
  }

  /**
   * Clears the cache of keyspace definitions so that any subsequent metadata query will refetch from
   * Cassandra. This isn't necessary fast but it is a convenient way to make sure we are looking at
   * current metadata.
   *  
   * @return
   */
  synchronized protected void refreshKeyspaceDefinitions() {
    LOGGER.debug( 'refreshKeyspaceDefinitions: clearing');
    keyspaceDefinitions.clear();
  }

}
