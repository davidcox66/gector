package org.gector.test;

import me.prettyprint.cassandra.service.ThriftCfDef
import me.prettyprint.cassandra.service.ThriftKsDef
import me.prettyprint.hector.api.Cluster
import me.prettyprint.hector.api.ddl.ColumnDefinition
import me.prettyprint.hector.api.ddl.ColumnFamilyDefinition
import me.prettyprint.hector.api.ddl.ColumnType
import me.prettyprint.hector.api.ddl.ComparatorType
import me.prettyprint.hector.api.ddl.KeyspaceDefinition
import me.prettyprint.hector.api.exceptions.HInvalidRequestException
import me.prettyprint.hector.api.factory.HFactory

import org.apache.cassandra.config.ConfigurationException
import org.apache.cassandra.config.DatabaseDescriptor
import org.apache.cassandra.db.Table
import org.apache.cassandra.service.EmbeddedCassandraService
import org.apache.commons.lang.StringUtils
import org.apache.poi.hssf.record.formula.functions.T
import org.apache.thrift.transport.TTransportException
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import org.gector.db.GException

class EmbeddedCassandra
{

   static final Logger LOGGER = LoggerFactory.getLogger( EmbeddedCassandra );
 
    private static final int CASSANDRA_RETRIES = 30;
    private static final String EMBEDDED_CONF = "/cassandra-conf/cassandra.yaml";
    private static final String CLUSTER_NAME = "Test Cluster";
    private static final int MAX_SCHEMA_RETRIES = 30;
   
    private static final String SYSTEM_TABLE = "system";
        
    private static EmbeddedCassandraService cassandra;

    public static void setEmbeddedEnabled( boolean enabled ) {
        setStandalone( !enabled );
    }
    
    public static boolean isEmbeddedEnabled() {
        return !Boolean.parseBoolean(System.getProperty("cassandra.standalone"));
    }
    public static void setStandalone( boolean enabled ) {
        System.setProperty("cassandra.standalone", ""+enabled );
    }

    public static String getHost() {
        return System.getProperty( "cassandra.host", "127.0.0.1" );
    }
   
    public static int getPort() {
        return Integer.parseInt( System.getProperty("cassandra.port", "9160") );
    }
    
    public static double getReadRepairChance() {
        String readRepairChanceStr = System.getProperty( "cassandra.read_repair_chance", "1.0");
        return Double.parseDouble(readRepairChanceStr);
    }

    public static int getReplicationFactor() {
        String replication = System.getProperty("cassandra.replication");
        return StringUtils.isNotEmpty(replication) ? Integer.parseInt(replication) : 1;
    }

    public static String getStrategyClass() {
        return System.getProperty("cassandra.strategy.class");
    }
   
    public static Map<String,String> getStrategyOptions() {
        String prop = System.getProperty( "cassandra.strategy.options" );
        if( StringUtils.isEmpty(prop) ) {
            return null;
        }
        Map<String,String> options = new HashMap<String,String>();
        String[] opts = prop.split( "," );
        for( String opt : opts ) {
            String[] dataCenterReplication = opt.split(":");
            if( dataCenterReplication.length != 2 ) {
                throw new GException( "Invalid strategy option: " + opt );
            }
            options.put( dataCenterReplication[0], dataCenterReplication[1] );
        }
        return options;
    }
    
    public static void setConfigurationLocation() {
        if (isEmbeddedEnabled()) {
            // Tell cassandra where the configuration files are.
            URL url = EmbeddedCassandra.class.getResource(EMBEDDED_CONF);
            if( url == null ) {
                throw new GException( "Could not find cassandra.yaml in the classpath");
            }
            System.setProperty("cassandra.config", url.toString() );
        }
    }

    public static boolean isConfigurationFound() {
        return EmbeddedCassandra.class.getResource(EMBEDDED_CONF) != null;
    }
    
    public static void waitForAvailable() throws RuntimeException {
        waitForAvailable(getOrCreateCluster());
    }

    public static void waitForAvailable(Cluster cluster) throws RuntimeException {
        // TODO: DCC Find a better way to wait for Casandra to start
        for (int count = 0; count < CASSANDRA_RETRIES; count++) {
            if (isAvailable(cluster)) {
                LOGGER.info("waitForAvailable: cassandra started");
                return;
            }
            try {
                Thread.sleep(1000);
            } catch (Exception ex) {
                LOGGER.debug("waitForAvailable: interrupted while waiting for start", ex);
            }
        }

        throw new GException("Timed out waiting for Cassandra to start");
    }

    public static boolean isAvailable() {
        return isAvailable(getOrCreateCluster());
    }

    public static boolean isAvailable(Cluster cluster) {
        try {
            cluster.describeKeyspace(Table.SYSTEM_TABLE);
            String name = cluster.getName();
            LOGGER.info("isAvailable: cassandra cluster '{}' is running", name);
            return true;
        } catch (Exception ex) {
            LOGGER.info("isAvailable: ignoring exception ", ex);
            return false;
        }
    }

    /**
     * Set embedded cassandra up and spawn it in a new thread.
     * 
     * <p>
     * NOTE: This code has been copied to product-service and product-core. REV-77 should allow this code to be shared
     * in those other projects.
     * </p>
     * 
     * @throws ConfigurationException
     * @throws IOException
     * @throws TTransportException
     */
    synchronized public static void start() throws ConfigurationException, IOException, TTransportException {
        if (isEmbeddedEnabled()) {
            setConfigurationLocation();

            if (cassandra == null) {
                // loadSchemaFromYaml();
                // clean();
                cassandra = startDaemonAndWait();
                dropAllKeyspaces(getOrCreateCluster());
            }
        }
    }

    /*
     * private static void clean() throws IOException { final CassandraServiceDataCleaner cleaner = new
     * CassandraServiceDataCleaner(); cleaner.prepare(); CommitLog.instance.resetUnsafe(); }
     * 
     * private static void loadSchemaFromYaml() throws ConfigurationException { // Load the schema from Yaml for (final
     * KSMetaData ksMetaData : DatabaseDescriptor.readTablesFromYaml()) { for (final CFMetaData cfMetaData :
     * ksMetaData.cfMetaData().values()) { CFMetaData.map(cfMetaData); }
     * DatabaseDescriptor.setTableDefinition(ksMetaData, DatabaseDescriptor.getDefsVersion()); } }
     */

    private static EmbeddedCassandraService startDaemonAndWait() throws TTransportException, IOException {
        EmbeddedCassandraService service = startDaemon();
        Cluster cluster = getOrCreateCluster();
        waitForAvailable(cluster);
        LOGGER.info("startDaemonAndWait: cassandra finished starting");
        return service;
    }

    private static EmbeddedCassandraService startDaemon() throws TTransportException, IOException {
        LOGGER.info("startDaemon: starting");
        EmbeddedCassandraService service = new EmbeddedCassandraService();
        service.start();
        return service;
    }

    public static void dropAllKeyspaces() {
        dropAllKeyspaces(getOrCreateCluster());
    }

    public static void dropAllKeyspaces(Cluster cluster) {
        List<KeyspaceDefinition> keyspaces = cluster.describeKeyspaces();
        for (KeyspaceDefinition keyspace : keyspaces) {
            if (!SYSTEM_TABLE.equals(keyspace.getName())) {
                LOGGER.info("dropAllKeyspaces: dropping '{}'", keyspace.getName());
                cluster.dropKeyspace(keyspace.getName());
            }
        }
    }

    public static void dropKeyspace(final Cluster cluster, final String keyspace) {
        withSchemaSynchronization( cluster, new Runnable() {
            public void run() {
                try {
                    LOGGER.info("dropKeyspace: dropping '{}'", keyspace);
                    cluster.dropKeyspace(keyspace);
                } catch (HInvalidRequestException ex) {
                    if (isKeyspaceNotExistException(ex)) {
                        LOGGER.info("Keyspace does not exist yet");
                    } else {
                        throw ex;
                    }
                }
            }
        } );
    }

    public static void createKeyspace(final Cluster cluster, final String keyspace) {
        LOGGER.info("createKeyspace: creating keyspace '{}'", keyspace);
        withSchemaSynchronization( cluster, new Runnable() {
            public void run() {
                
                KeyspaceDefinition ksDefinition = HFactory.createKeyspaceDefinition(keyspace);
                ThriftKsDef tKsDef = (ThriftKsDef)ksDefinition;
                
                tKsDef.setReplicationFactor( getReplicationFactor() );
                
                String strategy = getStrategyClass();
                if( StringUtils.isNotEmpty(strategy) ) {
                    Map<String,String> options = getStrategyOptions();
                    if( options == null ) {
                        throw new GException( "Must set cassandra.strategy.options");
                    }
                    LOGGER.info( "createKeyspace: setting strategy=" + strategy + ", options=" + options );
                    tKsDef.setStrategyClass( strategy );
                    tKsDef.setStrategyOptions(options);
                }
                cluster.addKeyspace(ksDefinition);
            }
        } );
    }

    public static void initializeKeyspace(final String keyspace) {
        initializeKeyspace(getOrCreateCluster(), keyspace);
    }

    public static void initializeKeyspace(Cluster cluster, final String keyspace) {
        try {
            dropKeyspace(cluster, keyspace);
            createKeyspace(cluster, keyspace);
        } catch (RuntimeException ex) {
            LOGGER.error("Error initializing keyspace", ex);
            throw ex;
        }
    }

    private static boolean isKeyspaceNotExistException(HInvalidRequestException ex) {
        return "Keyspace does not exist.".equals(ex.getWhy());
    }

    public static void initializeKeyspaceAndBasicColumnFamilies(final String keyspace, final String[] columnFamilies,
            final String[] superColumnFamilies) {
        Cluster cluster = getOrCreateCluster();

        try {
            initializeKeyspace(cluster, keyspace);
            createColumnFamilies(cluster, keyspace, columnFamilies, ComparatorType.UTF8TYPE);
            createSuperColumnFamilies(cluster, keyspace, superColumnFamilies, ComparatorType.UTF8TYPE,
                    ComparatorType.UTF8TYPE);
        } catch (RuntimeException ex) {
            LOGGER.error("Error initializing keyspace and column families: keyspace '" + keyspace + "'", ex);
            throw ex;
        }
    }

    public static void createColumnFamilies(String keyspace, String[] columnFamilies, ComparatorType comparator) {
        createColumnFamilies(getOrCreateCluster(), keyspace, columnFamilies, comparator);
    }

    public static void createColumnFamilies(Cluster cluster, String keyspace, String[] columnFamilies,
            ComparatorType comparator) {
        for (String columnFamily : columnFamilies) {
            createColumnFamily(cluster, keyspace, columnFamily, comparator);
        }
    }

    public static void createColumnFamily(String keyspace, String columnFamily, ComparatorType comparator) {
        createColumnFamily(getOrCreateCluster(), keyspace, columnFamily, comparator);
    }

    public static void createColumnFamily(final Cluster cluster, final String keyspace, final String columnFamily,
            final ComparatorType comparator) {
        LOGGER.info("createColumnFamily: creating column family '{}' with comparator '{}'", columnFamily, comparator);
        withSchemaSynchronization( cluster, new Runnable() {
            public void run() {
                ColumnFamilyDefinition cfDefinition = HFactory.createColumnFamilyDefinition(keyspace, columnFamily, comparator,
                        new ArrayList<ColumnDefinition>());
                cfDefinition.setReadRepairChance( getReadRepairChance() );
                cluster.addColumnFamily(cfDefinition);
            }
        } );
    }

    public static void createSuperColumnFamilies(String keyspace, String[] superColumnFamilies,
            ComparatorType comparator, ComparatorType subComparator) {
        createSuperColumnFamilies(getOrCreateCluster(), keyspace, superColumnFamilies, comparator, subComparator);
    }

    public static void createSuperColumnFamilies(Cluster cluster, String keyspace, String[] superColumnFamilies,
            ComparatorType comparator, ComparatorType subComparator) {
        for (String superColumnFamily : superColumnFamilies) {
            createSuperColumnFamily(cluster, keyspace, superColumnFamily, comparator, subComparator);
        }
    }

    public static void createSuperColumnFamily(String keyspace, String superColumnFamily, ComparatorType comparator,
            ComparatorType subComparator) {
        createSuperColumnFamily(getOrCreateCluster(), keyspace, superColumnFamily, comparator, subComparator);
    }

    public static void createSuperColumnFamily(final Cluster cluster, final String keyspace, final String superColumnFamily,
            final ComparatorType comparator, final ComparatorType subComparator) {
        LOGGER.info("createSuperColumnFamily: creating column family '${superColumnFamily}' with comparator '${comparator}' and subcomparator '${subComparator}'" );
        // Hector interface forgot access to key data, so have to refer to Thrift implementation directly :-(
        withSchemaSynchronization( cluster, new Runnable() {
            public void run() {
                ThriftCfDef cfDefinition = (ThriftCfDef) HFactory.createColumnFamilyDefinition(keyspace, superColumnFamily,
                        comparator, new ArrayList<ColumnDefinition>());
                cfDefinition.setReadRepairChance( getReadRepairChance() );
                cfDefinition.setColumnType(ColumnType.SUPER);
                cfDefinition.setSubComparatorType(subComparator);
                cluster.addColumnFamily(cfDefinition);
            }
        } );
    }

    public static Cluster getOrCreateCluster() {
        setConfigurationLocation();
        String name = CLUSTER_NAME;
        int port = 9160;
        InetAddress address = null;
        if( isEmbeddedEnabled() && isConfigurationFound() ) {
            name = DatabaseDescriptor.getClusterName();
            address = DatabaseDescriptor.getListenAddress();
            port = DatabaseDescriptor.getRpcPort();
        }
        else {
           LOGGER.warn( "getOrCreateCluster: could not find cassandra config, using default localhost/9160");
           try {
            // address = InetAddress.getLocalHost();
            address = InetAddress.getByName( getHost() );
            port = getPort();
           }
           catch( Exception ex ) {
               LOGGER.error( "getOrCreateCluster: could not get localhost address", ex );
               throw new GException( "Unable to get localhost address", ex );
           }
        }
        LOGGER.info( "getOrCreateCluster: host=" + address.getHostAddress() + ", port=" + port );
        return HFactory.getOrCreateCluster(name, address.getHostAddress() + ":" + port);
    }

  public static void withSchemaSynchronization( Cluster cluster, Runnable closure ) 
  {
    waitForAllSchemaVersionsSynchronized( cluster );
    closure.run();
    waitForAllSchemaVersionsSynchronized( cluster );
  }
  
  public static void waitForAllSchemaVersionsSynchronized( Cluster cluster ) 
  {
    try {
        for( int i=0 ; i < MAX_SCHEMA_RETRIES ; i++ ) {
          if( isAllSchemaVersionsSynchronized(cluster) ) {
            LOGGER.debug( "waitForAllSchemaVersionsSynchronized: versions are synchronized");
            return;
          }
          Thread.sleep( 1000 );  
        }
    }
    catch( InterruptedException ex ) {
        LOGGER.warn( "waitForAllSchemaVersionsSynchronized: interrupted while waiting for schema synchronization, giving up", ex );
        throw new GException( "Interrupted while waiting for schema synchronization", ex );
        
    }
    LOGGER.warn( "waitForAllSchemaVersionsSynchronized: gave up waiting for schema synchronization");
    throw new GException( "Not all schema versions synchronized" );
    
  } 
  
  public static boolean isAllSchemaVersionsSynchronized( Cluster cluster ) 
  {
    Map<String,List<String>> versions = cluster.describeSchemaVersions();
    int count=0;
    for( Map.Entry<String,List<String>>  entry : versions.entrySet() ) {
        String schemaUid = entry.getKey();
        List<String> hosts = entry.getValue();
        if( schemaUid == "UNREACHABLE" ) {
            LOGGER.warn( "isAllSchemaVersionsSynchronized: nodes are down, skipping schema count: " + hosts );
        }
        else {
            count++;
        }
    }
    if( LOGGER.isDebugEnabled() ) { LOGGER.debug( "isAllSchemaVersionsSynchronized: versions=" + versions + ", count=" + count ); }
    return count == 1; 
  } 

}
