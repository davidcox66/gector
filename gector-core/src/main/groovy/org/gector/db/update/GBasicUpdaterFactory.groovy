package org.gector.db.update


import me.prettyprint.hector.api.Keyspace
import me.prettyprint.hector.api.Serializer
import me.prettyprint.hector.api.factory.HFactory
import me.prettyprint.hector.api.mutation.Mutator

class GBasicUpdaterFactory implements GUpdaterFactory
{
  private boolean immediate;
  private boolean tracing;
   
  GBasicUpdaterFactory( boolean immediate, boolean tracing=false ) {
    this.immediate = immediate;
    this.tracing = tracing;
  }
  
  GUpdater create( Keyspace keyspace, Serializer serializer ) {
	  Mutator mutator = HFactory.createMutator( keyspace, serializer );
    GUpdater ret = immediate ? new GImmediateUpdater(mutator) : new GBatchedUpdater(mutator);
    if( tracing ) {
        ret = new GTracingUpdater( ret );
    }
    return ret;
  }
}
