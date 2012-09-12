package org.gector.db

// @Grapes([ @Grab( value='me.prettyprint:hector-core:0.8.0-2', initClass=false ) ])
import me.prettyprint.cassandra.service.CassandraHostConfigurator
import me.prettyprint.hector.api.Cluster
import me.prettyprint.hector.api.ddl.KeyspaceDefinition
import me.prettyprint.hector.api.factory.HFactory

import org.slf4j.Logger
import org.slf4j.LoggerFactory

import org.gector.db.meta.GMetadataManager
import org.gector.db.meta.GMetadataManagerFactory

/**
 * This is the root object in the Groovy-Cassandra API. All updates/queries start from here, navigating
 * down to the keyspace, column family, row, column, and optionally sub-columns.
 * 
 * @author david
 * @since Mar 29, 2011
 */
class GCluster
{
  /*
   static {
   Grape.grab( [group:'me.prettyprint', module:'hector-core', version: '0.7.0-29', classLoader: GVersion.getRootClassLoader()] );
   Grape.grab( [group:'org.slf4j', module:'slf4j-log4j12', version: '1.6.1', classLoader: GVersion.getRootClassLoader()] );
   }
   */
   
  static final Logger LOGGER = LoggerFactory.getLogger(GCluster);
  
  public static final int MAX_SCHEMA_RETRIES = 30;
  public static final String SYSTEM_TABLE = "system";
  
  /**
   * The cluster name that will be used by default if the GCluster no-arg constructor is used.
   */
  static String defaultClusterName = 'Test Cluster';

  /**
   * The underlying Hector cluster 
   */
  final Cluster cluster;

  /**
   * The object which receives notifications when certain relevant operations involving metadata knowledge occur.
   */
  private GMetadataManager metadata;

  /**
   * Cache of previously used keyspace objects
   */
  final Map<String,GKeyspace> keyspaces = new HashMap<String,GKeyspace>();

  public static final int MAX_VALUE = Integer.MAX_VALUE;

  public static final int MAX_DELETE_CHUNK = 10000;  
  
  GCluster() {
    this( GHelper.getClusterName(defaultClusterName) );
  }

  GCluster( String name ) {
    this( name, GHelper.getHost(), GHelper.getPort() );
  }

  /**
   * Constructs a Hector cluster with the given parameters. 
   *  
   * @param name
   * @param host
   * @param port
   */
  GCluster( String name, String host, int port, String metaName=GMetadataManager.AUTO ) {
    this( createHectorCluster(name, host, port), metaName );
  }

  /**
   * Creates a GCluster with an already initialized Hector Cluster object
   * 
   * @param cluster
   */
  GCluster( Cluster cluster, String metaName=GMetadataManager.AUTO ) {
    this.cluster = cluster;
    setMetadata( GMetadataManagerFactory.create(metaName ? metaName : GHelper.getMetadataName()) );
    
  }

  private static Cluster createHectorCluster( String name, String host, int port ) {
    CassandraHostConfigurator config = new CassandraHostConfigurator(host);
    config.setPort( port );
    /*
    config.setCassandraThriftSocketTimeout(
      Integer.parseInt( System.getProperty("cassandra.timeout", "30")) * 1000 );
    */
    return HFactory.getOrCreateCluster( name, config );
  }
  
  void withSchemaSynchronization( Closure closure ) 
  {
    waitForAllSchemaVersionsSynchronized();
    closure.call( this );
    waitForAllSchemaVersionsSynchronized();
  }
  
  void waitForAllSchemaVersionsSynchronized() 
  {
    for( int i=0 ; i < MAX_SCHEMA_RETRIES ; i++ ) {
      if( isAllSchemaVersionsSynchronized() ) {
        LOGGER.debug( 'waitForAllSchemaVersionsSynchronized: versions are synchronized');
        return;
      }
      Thread.sleep( 1000 );  
    }
    LOGGER.warn( 'waitForAllSchemaVersionsSynchronized: gave up waiting for schema synchronization');
    throw new GException( "Not all schema versions synchronized" );
  } 
  
  boolean isAllSchemaVersionsSynchronized() 
  {
    Map<String,List<String>> versions = cluster.describeSchemaVersions();
    int count=0;
    versions?.each{ String schemaUid, List<String> hosts -> 
      if( schemaUid == 'UNREACHABLE' ) {
        LOGGER.warn( "isAllSchemaVersionsSynchronized: nodes are down, skipping schema count: ${hosts}");
      }
      else {
        count++;
      }
    }
    if( LOGGER.isDebugEnabled() ) { LOGGER.debug( "isAllSchemaVersionsSynchronized: versions=${versions}, count=${count}}" ); }
    return count == 1; 
  } 

  void createKeyspace( String keyspaceName ) {
      metadata.createKeyspace( keyspaceName );
  } 
  
  void recreateKeyspace( String keyspaceName ) {
      if( metadata.isExistingKeyspace(keyspaceName) ) {
          dropKeyspace( keyspaceName );
      }
      metadata.createKeyspace( keyspaceName );
  } 
  
  void dropKeyspace( String keyspaceName ) {
      cluster.dropKeyspace( keyspaceName );
  } 
  
  void dropAllKeyspaces()
  {
    List<KeyspaceDefinition> keyspaces = cluster.describeKeyspaces();
    for( KeyspaceDefinition keyspace : keyspaces ) {
      if( !SYSTEM_TABLE.equals(keyspace.getName()) ) {
	      LOGGER.info( "dropAllKeyspaces: dropping '${keyspace.name}'");
	      dropKeyspace( keyspace.getName() );
      }
    }
  }
 
  /**
   * Gets or creates the given GKeyspace object for interacting with a Cassandra keyspace and 
   * invokes the given closure. 
   * 
   * @param keyspaceName
   * @param closure
   */
  void withKeyspace( String keyspaceName, Closure closure ) {
    GKeyspace keyspace = getAt( keyspaceName );
    closure.call( keyspace );
  }

  /**
   * Get an existing GKeyspace object or creates one for the given keyspace if none exists yet.
   * 
   * @param keyspaceName
   * @return
   */
  GKeyspace getAt( String keyspaceName ) {
    synchronized( keyspaces ) {
	    GKeyspace keyspace = keyspaces[keyspaceName];
	    if( !keyspace ) {
	      keyspace = new GKeyspace( keyspaceName, this );
	      keyspaces[keyspaceName] = keyspace;
	    }
	    return keyspace;
    }
  }

  GCluster setMetadata( GMetadataManager metadata ) {
    this.metadata = metadata;
    metadata.setCluster( this );
    return this;
  }
 
  GMetadataManager getMetadata() {
    return metadata;
  } 
  
  boolean equals( Object obj ) {
    if( obj != null && obj instanceof GCluster ) {
      return ((GCluster)obj).cluster == this.cluster;
    }
    return false;
  }

  int hashCode() {
    return cluster.hashCode();
  }
}
