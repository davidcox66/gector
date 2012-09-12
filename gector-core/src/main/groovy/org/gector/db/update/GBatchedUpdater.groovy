package org.gector.db.update

import me.prettyprint.hector.api.Serializer
import me.prettyprint.hector.api.beans.HColumn
import me.prettyprint.hector.api.beans.HSuperColumn
import me.prettyprint.hector.api.mutation.MutationResult
import me.prettyprint.hector.api.mutation.Mutator


class GBatchedUpdater<K>
  implements GUpdater<K>
{
  private Mutator<K> mutator;
  
  GBatchedUpdater( Mutator<K> mutator ) {
    this.mutator = mutator;
  }
 
	MutationResult insert(final K key, final String cf, final HColumn c) {
    mutator.addInsertion( key , cf, c );
    return null;
  }
	MutationResult insert(final K key, final String cf, final HSuperColumn superColumn) {
    mutator.addInsertion( key, cf, superColumn );
    return null;
  }
	  
	MutationResult delete(final K key, final String cf ) {
    mutator.addDeletion( key, cf );
    return null;
  }
  
	MutationResult delete(final K key, final String cf, final Object columnName, final Serializer nameSerializer) {
    mutator.addDeletion( key, cf, columnName, nameSerializer );
    return null;
  }
	MutationResult delete(final K key, final String cf, final Object columnName, final Serializer nameSerializer, long clock) {
    mutator.addDeletion( key, cf, columnName, nameSerializer, clock );
    return null;
  }
	  
	MutationResult subDelete(final K key, final String cf, final Object supercolumnName, final Object columnName, final Serializer sNameSerializer, final Serializer nameSerializer) {
    mutator.addSubDelete( key, cf, supercolumnName, columnName, sNameSerializer, nameSerializer );
    return null;
  }
	MutationResult superDelete(K key, String cf, Object supercolumnName, Serializer sNameSerializer) {
    mutator.addSuperDelete( key, cf, supercolumnName, sNameSerializer );
    return null;
	} 
  
	MutationResult execute() {
    return mutator.execute();
  }
	  
	void discardPendingMutations() {
    mutator.discardPendingMutations();
  }
}
