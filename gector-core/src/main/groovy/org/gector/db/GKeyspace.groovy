package org.gector.db

@Grapes([
  // @Grab( value='me.prettyprint:hector-core:0.8.0-2', initClass=false ),
  // @Grab( value='org.slf4j:slf4j-log4j12:1.6.1', initClass=false )
  // @Grab( value='org.gector:gector-logging:1.0-SNAPSHOT', initClass=false ) 
])
import me.prettyprint.hector.api.Keyspace
import me.prettyprint.hector.api.factory.HFactory

import org.slf4j.Logger
import org.slf4j.LoggerFactory

import org.gector.db.meta.GBasicMetadataManager
import org.gector.db.meta.GMetadataManager

/**
 * The next level down from the cluster in the API to allow access to keyspace functionality. From
 * here we may perform operations of specified column families for query and update.
 * 
 * @author david
 * @since Mar 29, 2011
 */
class GKeyspace
{
  /*
   static {
   Grape.grab( [group:'me.prettyprint', module:'hector-core', version: '0.7.0-29', classLoader: GVersion.getRootClassLoader()] );
   Grape.grab( [group:'org.slf4j', module:'slf4j-log4j12', version: '1.6.1', classLoader: GVersion.getRootClassLoader()] );
   }
  */ 
  static final Logger LOGGER = LoggerFactory.getLogger(GKeyspace);
 
  /**
   * The owning cluster 
   */
  final GCluster cluster; 
  
  /**
   * The Hector keyspace
   */
  private Keyspace keyspace;

  private String keyspaceName;
    
  GKeyspace( String name, GCluster cluster ) {
    this.keyspaceName = name;
    this.cluster = cluster;
    //
    // NOTE: Keyspace must exist in DB prior to calling HFactory.createKeyspace. Otherwise, any updates will
    // fail later because there was no keyspace in session
    //
    metadata?.notifyNeedingKeyspace( keyspaceName );
    keyspace = HFactory.createKeyspace(keyspaceName, cluster.cluster);
  }
 
  
  /**
   * Drops the keyspace from the database, ignoring any error arising from the keyspace not already existing.
   */
  void drop()
  {
    GExceptionHelper.runIgnoringMissingKeyspace{
      if( LOGGER.isDebugEnabled() ) { LOGGER.debug( "dropKeyspace: dropping '${keyspaceName}'"); }
      cluster.withSchemaSynchronization{
        cluster.cluster.dropKeyspace( keyspaceName );
      }
    }
    metadata?.notifyDropKeyspace( this );
  }


  /**
   * Executes the given closure within the context of the given column family 
   * 
   * @param columnFamilyName
   * @param closure
   */
	void withColumnFamily( String columnFamilyName, Closure closure )  {
        closure.call( getAt(columnFamilyName) );
	}
 
  /**
   * Allows associative-array notation to access a column family within this keyspace 
   * @param columnFamilyName
   * @return
   */
  GColumnFamily getAt( String columnFamilyName ) {
    return new GColumnFamily( columnFamilyName, this );
  }
 
  /**
   * Allows property style notation to access a column family within this keyspace 
   * @param columnFamilyName
   * @return
   */
  GColumnFamily propertyMissing( String columnFamilyName ) {
    return getAt( columnFamilyName );
  }
 
  /**
   * @return the name of this keyspace
   */
  String getKeyspaceName() {
    return keyspaceName;
  }

  /**
   * @return the GMetadataManager responsible for the cluster which owns this keyspace
   */
  GMetadataManager getMetadata() {
    return cluster.metadata;
  }  

  public boolean isExistingColumnFamily( String columnFamily ) {
    return metadata.isExistingColumnFamily( keyspaceName, columnFamily );
  }

  public void createColumnFamily( String columnFamilyName, Class columnNameClass ) {
      metadata.createColumnFamily( keyspaceName, columnFamilyName, 
          GBasicMetadataManager.getComparatorType(columnNameClass) );
  }
  
  public void createSuperColumnFamily( String columnFamilyName, Class columnNameClass, Class subColumnNameClass ) {
      metadata.createSuperColumnFamily( keyspaceName, columnFamilyName, 
          GBasicMetadataManager.getComparatorType(columnNameClass),
          GBasicMetadataManager.getComparatorType(subColumnNameClass) );
  }
  
  boolean equals( Object obj ) {
    if( obj != null && obj instanceof GKeyspace ) {
      return ((GKeyspace)obj).keyspace == this.keyspace;
    }
    return false;
  }
  
  int hashCode() {
    return keyspace.hashCode();
  }
}
