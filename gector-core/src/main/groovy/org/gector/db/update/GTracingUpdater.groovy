package org.gector.db.update

import me.prettyprint.hector.api.Serializer
import me.prettyprint.hector.api.beans.HColumn
import me.prettyprint.hector.api.beans.HSuperColumn
import me.prettyprint.hector.api.mutation.MutationResult

import org.slf4j.Logger
import org.slf4j.LoggerFactory


class GTracingUpdater<K>
implements GUpdater<K> {
    
    static final Logger LOGGER = LoggerFactory.getLogger( GTracingUpdater );
    
    private GUpdater<K> delegate;
    private List<String> mutations = new ArrayList<String>();
    private PrintStream stream;
    
    GTracingUpdater( GUpdater<K> delegate ) {
        this( delegate, System.err );
    }
    GTracingUpdater( GUpdater<K> delegate, PrintStream stream ) {
        if( LOGGER.isDebugEnabled() ) { LOGGER.debug( "GTracingUpdater: tracing updater=${delegate}" ); }
        this.delegate = delegate;
        this.stream = stream;
    }

    MutationResult insert(final K key, final String cf, final HColumn c) {
        addMutation( "insert ${cf}{${key}}{${c.name}} = ${c.value}");
        return delegate.insert( key, cf, c );
    }
    MutationResult insert(final K key, final String cf, final HSuperColumn superColumn) {
        StringBuilder builder = new StringBuilder( "insert ${cf}{${key}}{${superColumn.name}} {" );
        int i=0;
        superColumn.columns.each{ HColumn c ->
            if( i++ > 0 ) {
                builder.append( ", ");
            }
            builder.append( "{${c.name}} = {${c.value}}");
        }
        builder.append( "}");
        addMutation( builder.toString() );

        return delegate.insert( key, cf, superColumn );
    }

    MutationResult delete(final K key, final String cf ) {
        addMutation( "delete ${cf}{${key}}" );
        return delegate.delete( key, cf );
    }

    MutationResult delete(final K key, final String cf, final Object columnName, final Serializer nameSerializer) {
        addMutation( "delete ${cf}{${key}}{${columnName}}" );
        return delegate.delete( key, cf, columnName, nameSerializer );
    }
    MutationResult delete(final K key, final String cf, final Object columnName, final Serializer nameSerializer, long clock) {
        addMutation( "delete ${cf}{${key}}{${columnName}} (millis=${clock})" );
        return delegate.delete( key, cf, columnName, nameSerializer, clock );
    }

    MutationResult subDelete(final K key, final String cf, final Object supercolumnName, final Object columnName, final Serializer sNameSerializer, final Serializer nameSerializer) {
        addMutation( "subDelete ${cf}{${key}{{${supercolumnName}}{${columnName}}" );
        return delegate.subDelete( key, cf, supercolumnName, columnName, sNameSerializer, nameSerializer );
    }
    MutationResult superDelete(K key, String cf, Object supercolumnName, Serializer sNameSerializer) {
        addMutation( "superDelete ${cf}{${key}}{${supercolumnName}}" );
        return delegate.superDelete( key, cf, supercolumnName, sNameSerializer );
    }

    MutationResult execute() {
        dumpMutations( "executing" );
        mutations.clear();
        return delegate.execute();
    }

    void discardPendingMutations() {
        dumpMutations( "discarding" );
        mutations.clear();
        delegate.discardPendingMutations();
    }

    private void addMutation( String mutation ) {
        if( LOGGER.isDebugEnabled() ) { LOGGER.debug( "addMutation: ${mutation}" ); }
        mutations.add( mutation );
    }
   
    private void dumpMutations( String action ) {
        int i=0;
        stream.println( action );
        stream.println( '-' * 70 );
        mutations.each{ String mutation ->
            stream.println( "TRACE: [${i}] : ${mutation}" );
            i++;
        }
        stream.println('');
    }
}
