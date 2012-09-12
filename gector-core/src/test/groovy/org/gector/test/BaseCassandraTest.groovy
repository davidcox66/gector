package org.gector.test

import org.junit.BeforeClass

import org.gector.db.GCluster
import org.gector.db.GKeyspace
import org.gector.db.meta.GFullMetadataManager
import org.gector.db.trans.GTransactionManager

abstract class BaseCassandraTest extends BaseTest
{
  GCluster cluster;
  GKeyspace keyspace;

  @BeforeClass 
  static void initCassandra() {
    GTransactionManager.setTracingEnabled( true );
    EmbeddedCassandra.start();
    EmbeddedCassandra.dropAllKeyspaces();
  }  
  
  BaseCassandraTest() {
    cluster = new GCluster().setMetadata( new GFullMetadataManager() );
    keyspace = cluster['RevKeyspace'];
  }
  
}
