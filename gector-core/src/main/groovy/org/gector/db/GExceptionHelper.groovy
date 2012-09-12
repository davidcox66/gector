package org.gector.db

import groovy.lang.Closure
import me.prettyprint.hector.api.exceptions.HInvalidRequestException
import me.prettyprint.hector.api.exceptions.HectorException

import org.apache.cassandra.thrift.InvalidRequestException
import org.slf4j.Logger
import org.slf4j.LoggerFactory

class GExceptionHelper
{
  static final Logger LOGGER = LoggerFactory.getLogger(GExceptionHelper);
  
    /**
     * Provides a convenient way of running a block of code where any exception related to the keyspace not
     * existing will be ignored. This may be used by various initialization/cleanup logic which needs to run
     * without error regardless of whether the keyspace exists.
     *
     * @param closure
     */
    public static void runIgnoringMissingKeyspace( Closure closure ) {
      try {
        closure.call();
      }
      catch( HInvalidRequestException ex ) {
        if( isKeyspaceNotExistException(ex) ) {
          LOGGER.debug( "Keyspace does not exist yet" );
        }
        else {
          throw ex;
        }
      }
    }
    
    public static void runIgnoringExistingColumnFamily( Closure closure ) {
      try {
        closure.call();
      }
      catch( HInvalidRequestException ex ) {
        if( isExistingColumnFamilyException(ex) ) {
          LOGGER.debug( "Column family already exists" );
        }
        else {
          throw ex;
        }
      }
    }
   
  static void runIgnoringMissingColumnFamily( Closure closure ) {
    try {
      closure.call();
    }
    catch( HInvalidRequestException ex ) {
      if( isKeyspaceNotExistException(ex) || isColumnFamilyNotExistException(ex) ) {
	      LOGGER.debug( "Keyspace/Column Family does not exist yet" );
      }
      else {
        throw ex;
      }
    }
  }
    /**
     * Checks if the given exception results from a keyspace not existing.
     *
     * @param ex
     * @return
     */
    public static boolean isKeyspaceNotExistException( HInvalidRequestException ex )
    {
      return isExceptionWhyMatchingPattern( ex, /^Keyspace .* does not exist$/ );
    }
  
    /**
     * Checks if the given exception indicates that a pending schema update has not been
     * replicated to all nodes.
     *
     * @param ex
     * @return
     */
    public static boolean isClusterSchemaNotSyncedException( HInvalidRequestException ex )
    {
      return isExceptionWhyMatchingPattern( ex, /^Cluster schema does not yet agree$/ );
    }
 
    public static boolean isExistingColumnFamilyException( HInvalidRequestException ex ) 
    {
      return isExceptionWhyMatchingPattern( ex, /.*is already defined in that keyspace.$/ );
    }   
    
  
	  static boolean isColumnFamilyNotExistException( HInvalidRequestException ex ) {
	    return isExceptionWhyMatchingPattern( ex, /^CF is not defined in that keyspace.$/ );
	  }
   
    static boolean isPoolsDownException( HectorException ex ) {
      return ex.message && ex.message =~ /.*All host pools marked down.*/;
    } 
    
    /**
     * Checks if the 'why' property of the exception matches the given pattern. Also, looks at the root cause
     * of the exception to work around any lost exception translation from ExceptionTranslater.
     *
     * @param ex
     * @param pattern
     * @return
     */
    public static boolean isExceptionWhyMatchingPattern( HInvalidRequestException ex, String pattern ) {
      if( LOGGER.isDebugEnabled() ) { LOGGER.debug( "isExceptionWhyMatchingPattern: pattern=${pattern}, why=${ex.why}, cause.why=${ex?.cause.why}", ex); }
      return ex.why =~ pattern || (ex.cause && ex.cause instanceof InvalidRequestException && ex.cause.why =~ pattern);
    }
  
}
