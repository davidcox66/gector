package org.gector.db

import me.prettyprint.hector.api.exceptions.HectorException


import org.slf4j.Logger
import org.slf4j.LoggerFactory

/**
 * A template to enclose a block of functionality which accesses Cassandra where you anticipate 
 * failures to connect to a database instance. This will capture any exception which indicates that
 * Cassandra is temporarily down. It will log the first occurrence of this error but subsequent calls
 * will ignore any of these sort of exceptions. The idea is having code which will be robust enough
 * to tolerate the outage while not filling up logs with errors while waiting for it to again
 * become available.
 * 
 * @author david
 * @since Jan 4, 2012
 */
class GDownTemplate {

  private static final Logger LOGGER = LoggerFactory.getLogger( GDownTemplate );
  
  private boolean down;
 
  /**
   * Calls the given closure, trapping any host down exceptions. If the call fails because of the 
   * cassandra cluster is down, cassandra will be marked as down. The host down exception will
   * not leave this method invocation. All other exceptions will propagate out of invoke().
   * 
   * @param closure
   * @return true if the call succeeded without a cassandra host down exception
   */
  boolean invoke( Closure closure ) {
      try {
        closure.call();
        down = false;
      }
	  catch( HectorException ex ) {
        checkDown( ex );
	  }
      return !down;
  }

  /**
   * Only calls the given closure if the last communication with cassandra was 
   * successful (without a host down exception)
   * @param closure
   * @return true if the host is up
   */
  boolean invokeIfNotDown( Closure closure ) {
      if( !down ) {
          return invoke( closure );
      }    
      return false;
  }
  
  boolean isDown() {
      return down;
  }

  /** 
   * Checks if the supplied exception is one indicating the cassandra host(s) are down. If so,
   * an exception is logged only if the previous communication was successful.  
   * @param ex
   * @return
   */
  boolean checkDown( HectorException ex ) {
	  if( GExceptionHelper.isPoolsDownException(ex) ) {
		  if( !down ) {
		    LOGGER.error( "checkDown: database is down, ignoring exceptions for now until it is back up...", ex );
		    down = true; 
		  }
	  }
      else {
          throw ex;
      }
      return down;
  } 
  
}
