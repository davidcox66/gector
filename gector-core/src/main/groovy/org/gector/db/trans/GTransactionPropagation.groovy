package org.gector.db.trans

import org.slf4j.Logger
import org.slf4j.LoggerFactory

import org.gector.db.GException

/**
 * A set of policies for the behavior of requests to begin/end a transaction. This is used
 * as a parameter to the GTransactionTemplate.execute(GTransactionPropagation) which defines 
 * transactional scope. This enum control whether a new transaction is really created at the beginning 
 * of the transaction scope and whether or not to commit at the end of the transaction scope.
 * @author david
 * @since Jan 4, 2012
 */
enum GTransactionPropagation {

    /**
     * This policy requires that a previously initiated transaction must exist at the entry of 
     * this transaction scope. Not already having a transaction will generate an exception. 
     * A new GTransaction object is not created at the entry of this transaction scope,
     * nor is one committed upon exit. This merely requires that some greater encompassing
     * transaction exists.
     */
    REQUIRES {
        GTransaction enterTransaction( GTransactionManager manager, Object transactionKey ) {
            if( !manager.isInTransaction() ) {
                throw new GException( "Requires a transaction, but not in one");
            }
            return manager.getTransaction();
        } 
    },
    /**
     * This policy specifies that a new transaction will be created if one does not yet exist.
     * Otherwise, this will simply use the transaction which already exists and place all its
     * mutations there.
     */
    INHERIT {
        GTransaction enterTransaction( GTransactionManager manager, Object transactionKey ) {
            return manager.isInTransaction() ? manager.getTransaction() : manager.pushTransaction( transactionKey );
        } 
    },
    /**
     * This policy always creates a new GTransaction at the entry of the transaction scope.
     */
    NEW {
        GTransaction enterTransaction( GTransactionManager manager, Object transactionKey ) {
            return manager.pushTransaction(transactionKey);
        } 
    };
    
    static final Logger LOGGER = LoggerFactory.getLogger( GTransactionPropagation );
  
    /**
     * Never called, just the base implementation. The expectation of the various enum instances is
     * to either return an existing GTransaction which should be used or to create a new
     * GTransaction with the supplied transactionKey. 
     * 
     * This transactionKey is what uniquely identifies the GTransaction. The transactionKey is a unique 
     * object created by the GTransactionTemplate to uniquely identify a GTransaction. The transactionKey
     * allows enterTransaction() and exitTransaction() to coordinate if the current transaction 
     * scope created the GTransaction currently being used. If this scope did indeed create the
     * GTransaction in its enterTransaction() then the corresponding exitTransaction() should end
     * the transaction, committing changes or whatever is particular to the enum instance.
     */
    GTransaction enterTransaction( GTransactionManager manager, Object transactionKey ) {
        return null;
    }
   
    /**
     * Performs whatever steps are necessary at the point of exiting a transaction scope. This functionality  
     * is currently the same for all the different GTransactionPropagation types.
     * 
     * The supplied transactionKey is a local variable passed from the GTransactionTemplate.execute(). By
     * comparing this with the transactionKey of the current transaction, we can determine if this scope
     * was responsible for instantiated the GTransaction. If so, we take appropriate steps to exit the
     * transaction such as committing the pending updates.
     * 
     * @param manager
     * @param trans the current transaction object for this scope
     * @param transactionKey the unique id defined GTransactionTemplate scope
     * @param finished true if the transaction scope completed without errors
     */
    void exitTransaction( GTransactionManager manager, GTransaction trans, Object transactionKey, boolean finished ) {
        if( trans.transactionKey == transactionKey ) {
            try {
                if( finished ) {
		            trans.commit();
                } 
            }
            finally {
                manager.popTransaction();    
            }
        }
        else {
            if( LOGGER.isDebugEnabled() ) { 
                LOGGER.debug( "exitTransaction: not commiting yet - trans.transactionKey=${trans.transactionKey}, transactionKey=${transactionKey}"); 
            }
        }
    }
}
