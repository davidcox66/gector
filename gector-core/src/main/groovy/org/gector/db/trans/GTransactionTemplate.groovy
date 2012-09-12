package org.gector.db.trans

import me.prettyprint.hector.api.mutation.MutationResult


import org.slf4j.Logger
import org.slf4j.LoggerFactory

/**
 * This defines the scope of a transaction. Its execute() method is simply passed a closure where
 * the duration of the closure call will occur in a transaction scope. The behavior of the 
 * transaction begin/end will depend on the GTransactionPropagation parameters. This will decide
 * if an existing transaction is required, one will be created only if needed, or one will be 
 * explicitly created.
 * 
 * @author david
 * @since Jan 4, 2012
 */
class GTransactionTemplate {

    static final Logger LOGGER = LoggerFactory.getLogger( GTransactionTemplate );
    
    static MutationResult execute( Closure closure ) {
        return execute( GTransactionPropagation.INHERIT, closure );
    }
    /**
     * Executes the given closure within a transaction scope.
     * 
     * @param propagation
     * @param closure
     */
    static void execute( GTransactionPropagation propagation, Closure closure ) {
        // A unique key to give to any GTransaction, if one is instantiated within
        // propagation.enterTransaction(). This will be used later in
        // propagation.exitTransaction() to determine if this GTransactionTemplate
        // had triggered the GTransaction instantiation or if one was inherited.
        Object transactionKey = new Object();
        if( LOGGER.isDebugEnabled() ) { LOGGER.debug( "execute: entering template transactionKey=${transactionKey}") }
        GTransactionManager manager = GTransactionManager.getInstance();
        GTransaction trans = propagation.enterTransaction( manager, transactionKey );
        boolean finished = false;
        try {
	        closure.call(); 
            finished = true;
        }
        catch( Exception ex ) {
            LOGGER.error( "execute: error durring transaction", ex );
        }
        finally {
            if( LOGGER.isDebugEnabled() ) { LOGGER.debug( "execute: exiting template transactionKey=${transactionKey}, finished=${finished}") }
	        propagation.exitTransaction( manager, trans, transactionKey, finished );
        }
    }
}
