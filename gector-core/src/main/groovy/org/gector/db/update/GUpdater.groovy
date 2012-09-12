package org.gector.db.update

import me.prettyprint.hector.api.Serializer;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.beans.HSuperColumn;
import me.prettyprint.hector.api.mutation.MutationResult;


interface GUpdater<K>
{
    MutationResult insert(final K key, final String cf, final HColumn c);
    MutationResult insert(final K key, final String cf, final HSuperColumn superColumn);
  
    MutationResult delete(final K key, final String cf );
    MutationResult delete(final K key, final String cf, final Object columnName, final Serializer nameSerializer);
    MutationResult delete(final K key, final String cf, final Object columnName, final Serializer nameSerializer, long clock);
  
    MutationResult subDelete(final K key, final String cf, final Object supercolumnName, final Object columnName, final Serializer sNameSerializer, final Serializer nameSerializer);
    MutationResult superDelete(K key, String cf, Object supercolumnName, Serializer sNameSerializer);
    
    MutationResult execute();
  
    void discardPendingMutations();
}
