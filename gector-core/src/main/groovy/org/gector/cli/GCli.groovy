package org.gector.cli

import org.slf4j.Logger
import org.slf4j.LoggerFactory

import org.gector.db.GCluster
import org.gector.db.GHelper
import org.gector.db.meta.GMetadataManager;
import org.gector.db.trans.GTransactionManager
import org.gector.db.trans.GTransactionTemplate

class GCli {

    public static void main( String[] args ) {
       try {
           String clusterName = null;
           String keyspaceName = null;
           String host = null;
           int port = 0;
           
           String meta = null;
           boolean debug = false;
           boolean fine = false;
           boolean trace = false;
           boolean grapes = false;
            
           int i=0; 
           for( ; i < args.length ; i++ ) {
               if( args[i].startsWith("--") ) {
                   if( args.length == i+1 ) {
                       usage();
                   }
                   
                   if( args[i] == '--host' )  {
                       host = args[++i];
                   }
                   else if( args[i] == '--port' ) {
                       port = args[++i] as int;
                   }
                   else if( args[i] == '--cluster' ) {
                      clusterName = args[++i];
                   }
                   else if( args[i] == '--meta' ) {
                      meta = args[++i];
                   }
                   else if( args[i] == '--keyspace' ) {
                      keyspaceName = args[++i];
                   }
                   else if( args[i] == '--trace' ) {
                       trace = true;
                   }
                   else if( args[i] == '--debug' ) {
                       debug = true;
                   }
                   else if( args[i] == '--fine' ) {
                       fine = true;
                   }
                   else if( args[i] == '--grapes' ) {
                       grapes = true;
                   }
               }
               else { 
                   break;
               }
           } 
           
           
           // Enable more loggers in logback.xml 
           // We want to do this before touching any classes which use logging and trigger log initialization
           System.setProperty( "root.level", debug || fine ? "DEBUG" : "WARN" );
           if( fine ) {
               System.setProperty( "fine.level", "DEBUG" );
           }
            
           clusterName = clusterName ? clusterName : GHelper.getClusterName(GCluster.defaultClusterName);
           keyspaceName = keyspaceName ? keyspaceName : GHelper.getKeyspaceName( null );
           host = host ? host : GHelper.getHost();
           port = port ? port : GHelper.getPort();
           meta = meta ? meta : GMetadataManager.AUTO;
           
           if( i == args.length || !clusterName ) {
               usage();
           }

           // System.setProperty('cassandra.host', host);
           // System.setProperty('cassandra.port', "${port}" );
           
           Logger LOGGER = LoggerFactory.getLogger( GCli );

           if( trace ) {           
               GTransactionManager.setTracingEnabled( true );
           }
           
           if( LOGGER.isDebugEnabled() ) { LOGGER.debug( "clusterName=${clusterName}, keyspaceName=${keyspaceName}, host=${host}, port=${port}, meta=${meta}" ); }
           
           GCluster cluster = new GCluster( clusterName, host, port, meta );
           Binding binding = new Binding();
           binding.setVariable( 'LOGGER', LOGGER );
           binding.setVariable( 'logger', LOGGER );
           binding.setVariable( 'cluster', cluster );
           if( keyspaceName ) {
               binding.setVariable( 'keyspace', cluster[keyspaceName] );
           }
          
           if( grapes ) { 
	           def lgr = new org.apache.ivy.util.DefaultMessageLogger(4);
	           groovy.grape.Grape.instance.ivyInstance.loggerEngine.setDefaultLogger( lgr );
	           org.apache.ivy.util.Message.setDefaultLogger( lgr );
           }
           
	       // GroovyScriptEngine engine = new GroovyScriptEngine( ".", new GroovyClassLoader(ClassLoader.getSystemClassLoader()) );
	       GroovyScriptEngine engine = new GroovyScriptEngine( ".", new GroovyClassLoader() );
           
           for( ; i < args.length ; i++ )  {
               String scriptName = args[i];
               GTransactionTemplate.execute{ 
                   if( LOGGER.isDebugEnabled() ) { LOGGER.debug( "processing script: ${scriptName}" ) }
			       engine.run( scriptName, binding );
               }
           }  
       } 
       catch( Exception ex ) {
           ex.printStackTrace();
           System.exit( 1 );
       }
    }
   
    private static final usage() {
        System.err.println( 'Usage: GCli [--trace] [{--debug | --fine}] [--grapes] [--cluster <cluster name>] [--host <host>] [--port <port>] [--meta <meta manager class>] [--keyspace <keyspace>] script...');
        System.exit( 1 );
    }
}
