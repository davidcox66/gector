package org.gector.db.trans;

import org.slf4j.Logger
import org.slf4j.LoggerFactory

import org.gector.db.GColumnFamily
import org.gector.db.GException
import org.gector.db.update.GBasicUpdaterFactory
import org.gector.db.update.GUpdater
import org.gector.db.update.GUpdaterFactory

/**
 * Manages transactions on a per-thread basis where each thread has its own GTransactionManager instance.
 * The transactions within a GTransactionManager are organized as a stack. Each begin of a transaction pushed
 * on the stack and leaving the transaction will pop the latest one from the stack. This push/pop behavior 
 * is regulated by the GTransactionPropagation of the transaction. A new transaction object may or may not 
 * be pushed/popped based upon this policy.
 *
 * The GTransactionManager is also responsible for creating GUpdaters within the thread through its
 * GUpdaterFactory.
 * 
 * @author david
 * @since Jan 4, 2012
 */
class GTransactionManager {

    static final Logger LOGGER = LoggerFactory.getLogger( GTransactionManager );
    
    private static ThreadLocal<GTransactionManager> managers = new ThreadLocal<GTransactionManager>();

    private static boolean tracing;
    private GUpdaterFactory updaterFactory;
    private LinkedList<GTransaction> transactions = new LinkedList<GTransaction>();
   
    /**
     * Requests that any GTransactionManager instantiated will have updaters that dump all of
     * their mutations upon commit/rollback for debugging purposes.
     *  
     * @param tracing
     * @return
     */
    static boolean setTracingEnabled( boolean tracing ) {
        GTransactionManager.tracing = tracing;    
    } 
   
    /**
     * Gets the current GTransactionManager associated with this thread, instantiating one if necessary
     * with its default settings.
     */
    static final GTransactionManager getInstance() {
        GTransactionManager ret = managers.get();
        if( !ret ) {
           ret = new GTransactionManager( new GBasicUpdaterFactory(false, tracing) );
           managers.set( ret );
        }
        return ret;
    } 

    /**
     * Changes the GTransactionManager for the current thread  
     * @param manager
     */
    static void setInstance( GTransactionManager manager ) {
        managers.set( manager );
    }

    /**
     * A convenience method to get the current innermost transaction of this current thread.
     * @return
     */
    static GTransaction getCurrentTransaction() {
        return instance.getTransaction();
    } 
    
    GTransactionManager( GUpdaterFactory updaterFactory ) {
        this.updaterFactory = updaterFactory;
    }
   
    /**
     * The transaction manager is given ultimate control of the GUpdaters that are created within the current
     * thread. This consolidates the responsabilty in one place instead of micro-managing it.
     * @param columnFamily
     * @return
     */
    GUpdater createUpdater( GColumnFamily columnFamily )  {
        if( LOGGER.isDebugEnabled() ) { LOGGER.debug( "createUpdater: creating updater for columnFamily=${columnFamily.columnFamilyName}" ); }
        return updaterFactory.create( columnFamily.keyspace.keyspace, columnFamily.serializer );
    }
  
    /**
     * Flushes (commits) all the pending changes of the innermost transaction to the database. 
     * This is currently just and  alias for commit() though we could change the semantics in the future.  
     * The intention of this method is to push changes immediately to the database either because you 
     * simply need them there now or are concerned about too many pending mutations in the case of a 
     * large batch of updates.
     */
    void flush() {
        commit();
    }
    
    /**
     * Flushes (commits) all the pending changes of all transaction in the current thread to the database. 
     * This is currently just and  alias for commit() though we could change the semantics in the future.  
     * The intention of this method is to push changes immediately to the database either because you 
     * simply need them there now or are concerned about too many pending mutations in the case of a 
     * large batch of updates.
     */
    void flushAll() {
        commitAll();
    }
    
    /**
     * Commits all the pending changes of the innermost transaction to the database. 
     */
    void commit() {
        if( isInTransaction() ) {
           getTransaction().commit(); 
        }
    }
    
    /**
     * Commits all the pending changes of all transaction in the current thread to the database. 
     */
    void commitAll() {
        if( isInTransaction() ) {
            transactions.each{ GTransaction trans ->
                trans.commit(); 
            }
        }
    }
    
    /**
     * @return true if a transaction has been started in the current thread and we are within 
     * the scope of that transaction. The GTransactionTemplate determines the scope of the
     * transaction.
     */
    boolean isInTransaction() {
        return transactions.size() > 0;
    }  
   
    /**
     * @return the current innermost transaction of the current thread, throwing and exception if
     * not in a transaction.
     */
    GTransaction getTransaction() {
        ensureInTransaction();
        return transactions.getLast(); 
    } 
    
    /**
     * Creates a new GTransaction on the transaction stack of the current thread. The thread is
     * assigned the given transactionKey to simply uniquely identify the transaction from others.
     * 
     * @param transactionKey
     * @return
     */
    GTransaction pushTransaction( Object transactionKey ) {
        GTransaction ret = new GTransaction( this, transactionKey );
        transactions.add( ret );
        return ret;
    }

    /**
     * Pops the innermost transaction for the current thread from the transaction stack, throwing
     * an exception if not in a transaction.    
     */
    void popTransaction()  {
        ensureInTransaction();
        transactions.removeLast();
    }
  
    /**
     * Validates we are within the scope of a transaction defined by GTransactionTemplate, throwing
     * an exception if not in a transaction. 
     */
    void ensureInTransaction() {
        if( !transactions ) {
            throw new GException( "Not in a transaction");
        }
    } 
}
