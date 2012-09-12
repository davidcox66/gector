package org.gector.db

import java.nio.ByteBuffer

import java.nio.ByteBuffer

import me.prettyprint.cassandra.serializers.DoubleSerializer
import me.prettyprint.cassandra.serializers.SerializerTypeInferer
import me.prettyprint.hector.api.Serializer
import me.prettyprint.hector.api.beans.ColumnSlice
import me.prettyprint.hector.api.beans.HColumn
import me.prettyprint.hector.api.beans.HSuperColumn
import me.prettyprint.hector.api.beans.SuperSlice

import org.slf4j.Logger
import org.slf4j.LoggerFactory

import org.gector.db.meta.GMetadataManager

/**
 * Centralizes a few low-level operations so that we may have better control over the functionality
 * or address limitations/bugs in how they are implemented.
 * 
 * @author david
 * @since Mar 29, 2011
 */
class GHelper
{
  private static final Logger LOGGER = LoggerFactory.getLogger(GHelper);

  /**
   * Use in cases where you want to insert empty data such as when using column names as values
   */
  static final byte[] EMPTY = new byte[0];

  static String getHost() {
      return getSystemPropertyOrEnv( 'cassandra.host', 'CASSANDRA_HOST', 'localhost' );
  } 
  
  static int getPort() {
      return getSystemPropertyOrEnv( 'cassandra.port', 'CASSANDRA_PORT', '9160' ) as int;
  } 
 
  static String getClusterName( String defaultValue ) {
      return getSystemPropertyOrEnv( 'cassandra.cluster', 'CASSANDRA_CLUSTER', defaultValue );
  } 
  
  static String getKeyspaceName( String defaultValue ) {
      return getSystemPropertyOrEnv( 'cassandra.keyspace', 'CASSANDRA_KEYSPACE', defaultValue );
  } 

  static String getMetadataName() {
      return getSystemPropertyOrEnv( 'cassandra.metadata', 'CASSANDRA_METADATA', GMetadataManager.AUTO );
  }  
  
  static getSystemPropertyOrEnv( String systemPropertyName, String envName, String defaultValue ) {
      String value = System.getProperty( systemPropertyName );
      if( !value ) {
          value = System.getenv( envName );
      }
      return value ? value : defaultValue;
  }
  
  /**
   * Converts any object into a ByteBuffer using it's default serializer for the datatype
   * 
   * @param obj
   * @return
   */
  static ByteBuffer toByteBuffer( Object obj ) {
    return obj != null ? getSerializer(obj).toByteBuffer( obj ) : null;
  }
 
  /**
   * Converts a list of  object into a ByteBuffer using it's default serializer for the datatype
   * 
   * @param obj
   * @return
   */
  static List<ByteBuffer> toByteBufferList( List list ) {
    if( list ) {
      Serializer ser = getSerializer( list.iterator().next() ); 
      List ret = new ArrayList( list.size() );
      for( Object obj : list ) {
        list.add( ser.toByteBuffer(obj) );
      }
      return ret;
    }
    return null;
  }
  
  /**
   * Converts a list of  object into a ByteBuffer using it's default serializer for the datatype
   * 
   * @param obj
   * @return
   */
  static ByteBuffer[] toByteBufferArray( List list ) {
    if( list ) {
      Serializer ser = getSerializer( list.iterator().next() ); 
      ByteBuffer[] ret = new ByteBuffer[ list.size() ];
      int i=0;
      for( Object obj : list ) {
        ret[i++] = ser.toByteBuffer(obj);
      }
      return ret;
    }
    return null;
  }
  
  /**
   * Converts any object into a ByteBuffer, accounting for a bug in Hector that does not
   * return the appropriate Serializer for Double.
   *  
   * @param obj
   * @return
   */
  static Serializer getSerializer( Object obj ) {
    Serializer ret = null;
    if( obj != null ) {
      if( obj.getClass() == Double.class ) {
        ret = DoubleSerializer.get();
      }
      else {
	      ret = SerializerTypeInferer.getSerializer(obj);
      }
    }
    if( LOGGER.isDebugEnabled() ) {
      String serializerName = getSerializerName( ret );
      String className = getShortClassName(obj);
	    LOGGER.debug( "getSerializer: object=${obj}, class=${className}, serializer=${serializerName}" );
    }
    return ret; 
  }
  
  static Serializer getSerializer( Class cls ) {
    Serializer ret = null;
    if( cls != null ) {
      if( cls == Double.class ) {
        ret = DoubleSerializer.get();
      }
      else {
	      ret = SerializerTypeInferer.getSerializer(cls);
      }
    }
    if( LOGGER.isDebugEnabled() ) {
      String serializerName = getSerializerName( ret );
      String className = getShortClassName(cls);
	    LOGGER.debug( "getSerializer: class=${className}, serializer=${serializerName}" );
    }
    return ret; 
  }
 
  static String getShortClassName( Object obj ) {
    return obj ? getShortClassName( obj.getClass() ) : null;
  } 
  
  static String getShortClassName( Class cls ) {
    if( cls ) {
      String className = cls.getName();
      className = className.replaceAll( /^java\.lang\./, "" );
      return className;
    }
    return null;
  } 
  
  static String getSerializerName( Serializer serializer ) {
    if( serializer ) {
      String name = serializer.getClass().getName();
      name = name.replaceAll( /.*\./, "" );
      name = name.replaceAll( /Serializer/, "" );
      return name;
    }
    return null;
  }
 
  /**
   * Convert groovy GStrings to a java string when the are used as column names or values.
   *  
   * @param obj
   * @return
   */
  static Object filterGroovyValue( Object obj ) {
    if( obj instanceof GString ) {
      return obj.toString();
    }
    return obj;
  }
  
  static Object[] toArray( Iterable iterable ) {
    if( iterable != null ) {
      if( iterable instanceof Collection ) {
        return ((Collection)iterable).toArray();
      }
      else {
        List list = new ArrayList();
        for( Object val : iterable.iterator() ) {
          list.add( val );
        }
        return list.toArray();
      }
    }
  }
  
  static String toString( ColumnSlice slice, Class columnNameClass ) {
    return toString( slice, getSerializer(columnNameClass) );
  }
  
  static String toString( ColumnSlice slice, Serializer columnNameSerializer ) {
    if( slice != null ) {
      String str = toColumnString( slice.getColumns(), columnNameSerializer, null );
      return "ColumnSlice{ ${str} }";
    }
    return null;
  }
  
  static String toString( SuperSlice slice, Class superColumnNameClass, Class subColumnNameClass ) {
    return toString( slice, getSerializer(superColumnNameClass), getSerializer(subColumnNameClass) );
  }
  
  static String toString( SuperSlice slice, Serializer superColumnNameSerializer, Serializer subColumnNameSerializer ) {
    if( slice != null ) {
      String str = toSuperColumnString( slice.getSuperColumns(), superColumnNameSerializer, subColumnNameSerializer );
      return "SuperSlice{ ${str} }";
    }
    return null;
  }
  
  static String toString( HSuperColumn scol, Class superColumnNameClass, Class subColumnNameClass ) {
    return toString( scol, getSerializer(superColumnNameClass), getSerializer(subColumnNameClass) );
  }
  
  static String toString( HSuperColumn scol, Serializer superColumnNameSerializer, Serializer subColumnNameSerializer ) {
    if( scol != null ) {
      Object superName = getColumnName(scol,superColumnNameSerializer);
      StringBuilder builder = new StringBuilder( superName )
      if( subColumnNameSerializer != null ) {
        builder.append( '{')
        builder.append( toColumnString(scol.getColumns(),subColumnNameSerializer,null) );
        builder.append( '}');
      }
      return builder.toString();
    }
    return null;
  }
 
  static String toString( List columns, Class cls1, Class cls2 ) {
    return toString( columns, getSerializer(cls1), getSerializer(cls2) );
  }
  
  static String toString( List columns, Serializer serializer1, Serializer serializer2 ) {
    if( columns ) {
      if( columns.get(0) instanceof HColumn ) {
        return toStandardColumnString( columns, serializer1, serializer2 );
      }
      else {
        return toSuperColumnString( columns, serializer1, serializer2 );
      }
    }
    return null;
  }
  
  static String toStandardColumnString( List<HColumn> columns, Serializer columnNameSerializer, Serializer columnValueSerializer ) {
    if( columns != null ) {
        StringBuilder builder = new StringBuilder(); 
        int i=0;
	      for( HColumn col : columns ) {
	        Object name = getColumnName(col,columnNameSerializer);
	        if( i > 0 ) {
	          builder.append( ', ');
	        }
	        builder.append( "[${i}]={{${name}}" );
          if( columnValueSerializer ) {
            Object value = getColumnValue(col,columnValueSerializer);
            builder.append( "={${value}}");
          }
	        builder.append('}');
	        i++;
	      }
        return builder.toString();
    }
    return null;
  }
  
  static String toSuperColumnString( List<HSuperColumn> columns, Serializer superColumnNameSerializer, Serializer subColumnNameSerializer ) {
    if( columns != null ) {
      StringBuilder builder = new StringBuilder();
      int i=0;
      for( HSuperColumn scol : columns ) {
        if( i > 0 ) {
          builder.append( ", ");
        }
        String str = toString( scol, superColumnNameSerializer, subColumnNameSerializer );
        builder.append( "[${i}]={${str}}");
        i++;
      }
      return builder.toString();
    }
    return null;
  }
  
  
  static Object getColumnName( HSuperColumn scol, Serializer serializer ) {
    return fromByteBuffer(scol.getName(), serializer);
  }
  
  static Object getColumnName( HColumn col, Serializer serializer ) {
    return fromByteBuffer(col.getName(), serializer);
  }
  
  static Object getColumnValue( HColumn col, Serializer serializer ) {
    return fromByteBuffer(col.getValue(), serializer);
  }
 
  /**
   * Deal with annoyance in serializer moving the buffer pointer and leaving it there after serialization, making
   * the buffer unusable later.
   *  
   * @param buffer
   * @param serializer
   * @return
   */
  static Object fromByteBuffer( ByteBuffer buffer, Serializer serializer ) {
    Object ret = null;
    if( buffer != null ) {
      int pos = buffer.position();
      try {
	      ret = serializer.fromByteBuffer( buffer );
      }
      finally {
	      buffer.position( pos );
      }
    }
    return ret;
  }
  
  private static Class getClassOfRange( Object start, Object end ) {
    Class cls = null;
    if( start ) {
      return start.getClass();
    }
    else if( end ) {
      return end.getClass();
    }
    else {
      throw new GException( "Must specify either a start or end in the range to delete");
    }
  }  
  
}
