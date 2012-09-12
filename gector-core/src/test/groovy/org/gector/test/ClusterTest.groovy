package org.gector.test

import org.junit.Test

class ClusterTest extends BaseCassandraTest
{
  ClusterTest() {
  }

  @Test  
  void testDropAll() {
    cluster.dropAllKeyspaces();
  }
}
