package org.gector.test

import org.junit.Test

import org.gector.db.GColumn
import org.gector.db.GColumnFamily
import org.gector.db.GKeyspace
import org.gector.db.GRow
import org.gector.db.trans.GTransactionTemplate

class StandardColumnFamilyTest extends BaseCassandraTest
{
  StandardColumnFamilyTest() {
  }

  @Test
  void testReadWriteAt() {
    Date dt = new Date();
    String key = ''+System.currentTimeMillis();
    byte[] bytes = new byte[ 3 ];
    bytes[0] = 1;
    bytes[1] = 2;
    bytes[2] = 3;
    UUID uuid = new UUID( 1, 2 );
   
    GTransactionTemplate.execute{ 
	    keyspace.withColumnFamily('TestColumnFamily') { GColumnFamily cf ->
	      cf[key]['testString'] << 'My Test String';
	      cf[key]['testInt'] << (int)123;
	      cf[key]['testLong'] << (long)1234;
	      cf[key]['testDouble'] << (double)12345;
	      cf[key]['testBoolean'] << true;
	      cf[key]['testDate'] << bytes;
	      cf[key]['testUuid'] << uuid;
	    }
    }
    
    keyspace.withColumnFamily('TestColumnFamily') { GColumnFamily cf ->
      assertEquals( 'My Test String', cf[key]['testString'].getString() );
      assertEquals( (int)123, cf[key]['testInt'].getInt() );
      assertEquals( (long)1234, cf[key]['testLong'].getLong() );
      assertEquals( (double)12345, cf[key]['testDouble'].getDouble() );
      assertEquals( true, cf[key]['testBoolean'].getBoolean() );
      assertEquals( uuid, cf[key]['testUuid'].getUUID() );
      assertArrayEquals( bytes, cf[key]['testDate'].getByteArray() );
      
      cf[key].query{ GRow row ->
	      assertEquals( 'My Test String', row['testString'].getString() );
	      assertEquals( (int)123, row['testInt'].getInt() );
	      assertEquals( (long)1234, row['testLong'].getLong() );
	      assertEquals( (double)12345, row['testDouble'].getDouble() );
	      assertEquals( true, row['testBoolean'].getBoolean() );
	      assertEquals( uuid, row['testUuid'].getUUID() );
	      assertArrayEquals( bytes, row['testDate'].getByteArray() );
      }
    }
  }
  
  @Test
  void testReadWriteProperties() {
    Date dt = new Date();
    String key = ''+System.currentTimeMillis();
    byte[] bytes = new byte[ 3 ];
    bytes[0] = 1;
    bytes[1] = 2;
    bytes[2] = 3;
    UUID uuid = new UUID( 1, 2 );
   
    GTransactionTemplate.execute{ 
	    keyspace.withColumnFamily('TestColumnFamily') { GColumnFamily cf ->
	      cf[key].testString << 'My Test String';
	      cf[key].testInt << (int)123;
	      cf[key].testLong << (long)1234;
	      cf[key].testDouble << (double)12345;
	      cf[key].testBoolean << true;
	      cf[key].testDate << bytes;
	      cf[key].testUuid << uuid;
	    }
    }
    
    keyspace.withColumnFamily('TestColumnFamily') { GColumnFamily cf ->
      assertEquals( 'My Test String', cf[key].testString.getString() );
      assertEquals( (int)123, cf[key].testInt.getInt() );
      assertEquals( (long)1234, cf[key].testLong.getLong() );
      assertEquals( (double)12345, cf[key].testDouble.getDouble() );
      assertEquals( true, cf[key].testBoolean.getBoolean() );
      assertEquals( uuid, cf[key].testUuid.getUUID() );
      assertArrayEquals( bytes, cf[key].testDate.getByteArray() );
      
      cf[key].query{ GRow row ->
	      assertEquals( 'My Test String', row.testString.getString() );
	      assertEquals( (int)123, row.testInt.getInt() );
	      assertEquals( (long)1234, row.testLong.getLong() );
	      assertEquals( (double)12345, row.testDouble.getDouble() );
	      assertEquals( true, row.testBoolean.getBoolean() );
	      assertEquals( uuid, row.testUuid.getUUID() );
	      assertArrayEquals( bytes, row.testDate.getByteArray() );
      }
    }
  }

  @Test   
  void testMultiGet() {
    int num = 10;
    long startKey = System.currentTimeMillis();

    def range = (startKey..startKey+num); 
    GTransactionTemplate.execute{
	    keyspace.withColumnFamily( 'TestColumnFamilyMulti' ) { GColumnFamily cf ->
	      range.each{ key ->
	        cf[key]['testCol'] << "${key}" ;
	      }
	    }
    }
     
    keyspace.withColumnFamily( 'TestColumnFamilyMulti' ) { GColumnFamily cf ->
      def rows = cf.query( range ) { 
        assertNotNull( cf.queriedRows );
	      range.each{ key ->
	        assertEquals( "${key}", cf[key]['testCol'].getString() );
	      }
      } 
      assertEquals( "Number of results do no match", num, rows.count-1 ) ;
    } 
  }

  @Test 
  void testRowGet() {
    GTransactionTemplate.execute{
	    keyspace.withColumnFamily( 'TestColumnFamilyGet' ) { GColumnFamily cf ->
	        cf[5]['testCol1'] << "1" ;
	        cf[5]['testCol2'] << "2" ;
	    }
    }
    
    keyspace.withColumnFamily( 'TestColumnFamilyGet' ) { GColumnFamily cf ->
      def values = [];
      cf[5].query{ GRow row ->
        row.eachSliceColumn(String) { GColumn col ->
          values.add( col.getString() );
        }
      } 
      assertTrue( "Results does not contain '1'", values.contains('1') );
      assertTrue( "Results does not contain '2'", values.contains('1') );
    }
  } 
}
