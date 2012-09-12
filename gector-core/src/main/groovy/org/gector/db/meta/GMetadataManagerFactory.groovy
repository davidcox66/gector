package org.gector.db.meta

import org.slf4j.Logger
import org.slf4j.LoggerFactory

class GMetadataManagerFactory {

    private static final Logger LOGGER = LoggerFactory.getLogger( GMetadataManagerFactory );
    
    public static GMetadataManager create( String meta ) {
        GMetadataManager ret = null;
	    switch( meta ) {
	      case GMetadataManager.BASIC:
	        ret = new GBasicMetadataManager();
	        break;
	      case GMetadataManager.FULL:
	        ret = new GFullMetadataManager();
	        break;
	      default:
	        ret = new GAutoMetadataManager();
	        break;
	    }  
        if( LOGGER.isDebugEnabled() ) { LOGGER.debug( "create: meta=${meta}, ret=${ret}") }
        return ret;
    }
}
