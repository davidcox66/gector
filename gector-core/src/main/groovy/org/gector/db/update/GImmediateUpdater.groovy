package org.gector.db.update

import me.prettyprint.hector.api.Serializer
import me.prettyprint.hector.api.beans.HColumn
import me.prettyprint.hector.api.beans.HSuperColumn
import me.prettyprint.hector.api.mutation.MutationResult
import me.prettyprint.hector.api.mutation.Mutator


class GImmediateUpdater<K>
  implements GUpdater<K>
{
  private Mutator<K> mutator;
  
  GImmediateUpdater( Mutator<K> mutator ) {
    this.mutator = mutator;
  }
 
	MutationResult insert(final K key, final String cf, final HColumn c) {
    mutator.insert( key , cf, c );
  }
	MutationResult insert(final K key, final String cf, final HSuperColumn superColumn) {
    mutator.insert( key, cf, superColumn );
  }
	  
	MutationResult delete(final K key, final String cf ) {
    mutator.delete( key, cf, null, null );
  }
  
	MutationResult delete(final K key, final String cf, final Object columnName, final Serializer nameSerializer) {
    mutator.delete( key, cf, columnName, nameSerializer );
  }
	MutationResult delete(final K key, final String cf, final Object columnName, final Serializer nameSerializer, long clock) {
    mutator.delete( key, cf, columnName, nameSerializer, clock );
  }
	  
	MutationResult subDelete(final K key, final String cf, final Object supercolumnName, final Object columnName, final Serializer sNameSerializer, final Serializer nameSerializer) {
    mutator.subDelete( key, cf, supercolumnName, columnName, sNameSerializer, nameSerializer );
  }
	MutationResult superDelete(K key, String cf, Object supercolumnName, Serializer sNameSerializer) {
    mutator.superDelete( key, cf, supercolumnName, sNameSerializer );
	} 
  
	MutationResult execute() {
    mutator.execute();
  }
	  
	void discardPendingMutations() {
    mutator.discardPendingMutations();
  }
}
