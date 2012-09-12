package org.gector.db.trans;

import me.prettyprint.hector.api.mutation.MutationResult

import org.slf4j.Logger
import org.slf4j.LoggerFactory

import org.gector.db.GColumnFamily
import org.gector.db.GException
import org.gector.db.GHelper
import org.gector.db.update.GUpdater


/**
 * This provides transaction-like behavior for Cassandra updates. This does not imply that GTransaction
 * provides ACID transaction semantics beyond anything provided by Cassandra itself. What is does is
 * provide scope, or grouping, of related mutations to the database. It does this by maintaining 
 * a set of GUpdaters being used within the current 'transaction'. There is one GUpdater per column
 * family. Though there could be one GUpdater per key serializer. By column family is more convenient
 * at the moment for tracing/debugging purposes.
 * 
 * Though this does provide ACID transaction semantics, it does represent a consolidation of update
 * logic into a central point of control This may pave the way for further robustness to the updates
 * in lieu of ACID transactions.
 *  
 * @author david
 * @since Jan 4, 2012
 */
public class GTransaction {

    static final Logger LOGGER = LoggerFactory.getLogger(GTransaction);
 
	/**
	  * A cache of Mutators keyed by column family name. We retain these so multiple separate updates
	  * to the same column family will use the same mutator without losing previous update requests.
	  */
	private final Map<String,GUpdater> updaters = new HashMap<String,GUpdater>();

    final GTransactionManager manager;
    
    /**
     * A unique id assigned to this GTransaction by the initiator of the transaction
     */
    final Object transactionKey;
     
    GTransaction( GTransactionManager manager, Object transactionKey ) {
        this.manager = manager;
        this.transactionKey = transactionKey;
    }
    
    GUpdater getUpdater( GColumnFamily columnFamily ) {
        GUpdater updater = updaters.get(columnFamily.columnFamilyName);
        if( !updater ) {
	        if( columnFamily.serializer == null ) {
	            throw new GException( "Cannot get mutator without having serializer. Call GColumnFamily.getAt(...) first.")
	        } 
            if( LOGGER.isDebugEnabled() ) {
                String keySerializerName = GHelper.getSerializerName( columnFamily.serializer );
                LOGGER.debug( "getUpdater: columFamily=${columnFamily.columnFamilyName}, keySerializer=${keySerializerName} ");
            }
            updater = manager.createUpdater( columnFamily );
            updaters.put(columnFamily.columnFamilyName, updater );
        }
        return updater;
	}
    
    /**
     * Executes all of the pending mutations for column families within this keyspace
     */
    void commit() {
        long start = 0;
        if( LOGGER.isDebugEnabled() ) { 
            start = System.nanoTime();
            LOGGER.debug( "commit: transationKey=${transactionKey}, updaters.size=${updaters.size()}" ); 
        } 
        
        updaters.each{  String columnFamilyName, GUpdater updater ->
            MutationResult result = updater.execute();
            if( LOGGER.isDebugEnabled() ) { LOGGER.debug( "commit: column family: ${columnFamilyName} execute time (micro): ${result.executionTimeMicro}" ); } 
        }
        
        if( LOGGER.isDebugEnabled() ) { 
	        long total = (System.nanoTime() - start) / 1000;
	        LOGGER.debug( "commit: execution time (micro): ${total}"); 
        }
    }
   
    /**
     * Discards all of the pending mutations in all updaters of the current GTransaction 
     */
    void rollback() {
        if( LOGGER.isDebugEnabled() ) { LOGGER.debug( "rollback: transationKey=${transactionKey}, updaters.size=${updaters.size()}" ); } 
        updaters.each{  String columnFamilyName, GUpdater updater ->
            updater.discardPendingMutations();
        }
    }
}
