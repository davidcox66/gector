package org.gector.test

import org.junit.Assert
import org.junit.Test

import org.gector.db.GColumnFamily
import org.gector.db.GKeyspace
import org.gector.db.GRow

class KeyspaceTest extends BaseCassandraTest
{
  KeyspaceTest() {
  }
 
  @Test 
  void testDrop() {
    keyspace.drop();
  }
  
  @Test 
  void testGetColumnFamily() {
    GColumnFamily columnFamily = keyspace['TestColumnFamily'];
    assertNotNull( columnFamily );
  }
}
