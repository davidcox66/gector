package org.gector.db.update

import me.prettyprint.hector.api.Keyspace
import me.prettyprint.hector.api.Serializer

interface GUpdaterFactory
{
  GUpdater create( Keyspace keyspace, Serializer serializer ); 
}
