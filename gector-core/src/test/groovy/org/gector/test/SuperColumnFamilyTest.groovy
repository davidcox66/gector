package org.gector.test

import org.junit.Test
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import org.gector.db.GColumn
import org.gector.db.GColumnFamily
import org.gector.db.GKeyspace
import org.gector.db.GRow
import org.gector.db.GSubColumn
import org.gector.db.trans.GTransactionTemplate

class SuperColumnFamilyTest extends BaseCassandraTest
{
  static final Logger logger = LoggerFactory.getLogger(SuperColumnFamilyTest);
  
  SuperColumnFamilyTest() {
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
   
    def supers = ['sup1', 'sup2'];
    GTransactionTemplate.execute{ 
	    keyspace.withColumnFamily('TestSuperColumnFamily') { GColumnFamily cf ->
	      supers.each{ String sup ->
		      cf[key][sup]['testString'] << 'My Test String 1';
		      cf[key][sup]['testInt'] << (int)123;
		      cf[key][sup]['testLong'] << (long)1234;
		      cf[key][sup]['testDouble'] << (double)12345;
		      cf[key][sup]['testBoolean'] << true;
		      cf[key][sup]['testDate'] << bytes;
		      cf[key][sup]['testUuid'] << uuid;
		    }
	    }
    }
    
    keyspace.withColumnFamily('TestSuperColumnFamily') { GColumnFamily cf ->
      supers.each{ String sup ->
	      assertEquals( 'My Test String 1', cf[key][sup]['testString'].getString() );
	      assertEquals( (int)123, cf[key][sup]['testInt'].getInt() );
	      assertEquals( (long)1234, cf[key][sup]['testLong'].getLong() );
	      assertEquals( (double)12345, cf[key][sup]['testDouble'].getDouble() );
	      assertEquals( true, cf[key][sup]['testBoolean'].getBoolean() );
	      assertEquals( uuid, cf[key][sup]['testUuid'].getUUID() );
	      assertArrayEquals( bytes, cf[key][sup]['testDate'].getByteArray() );
	      
	      cf[key][sup].query{ GColumn scol ->
		      assertEquals( 'My Test String 1', scol['testString'].getString() );
		      assertEquals( (int)123, scol['testInt'].getInt() );
		      assertEquals( (long)1234, scol['testLong'].getLong() );
		      assertEquals( (double)12345, scol['testDouble'].getDouble() );
		      assertEquals( true, scol['testBoolean'].getBoolean() );
		      assertEquals( uuid, scol['testUuid'].getUUID() );
		      assertArrayEquals( bytes, scol['testDate'].getByteArray() );
	      }
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
	    keyspace.withColumnFamily('TestSuperColumnFamily') { GColumnFamily cf ->
	      cf[key].sup1.testString << 'My Test String 2';
	      cf[key].sup1.testInt << (int)123;
	      cf[key].sup1.testLong << (long)1234;
	      cf[key].sup1.testDouble << (double)12345;
	      cf[key].sup1.testBoolean << true;
	      cf[key].sup1.testDate << bytes;
	      cf[key].sup1.testUuid << uuid;
	    }
    }
    
    keyspace.withColumnFamily('TestSuperColumnFamily') { GColumnFamily cf ->
      assertEquals( 'My Test String 2', cf[key].sup1.testString.getString() );
      assertEquals( (int)123, cf[key].sup1.testInt.getInt() );
      assertEquals( (long)1234, cf[key].sup1.testLong.getLong() );
      assertEquals( (double)12345, cf[key].sup1.testDouble.getDouble() );
      assertEquals( true, cf[key].sup1.testBoolean.getBoolean() );
      assertEquals( uuid, cf[key].sup1.testUuid.getUUID() );
      assertArrayEquals( bytes, cf[key].sup1.testDate.getByteArray() );
      
      cf[key].sup1.query{ GColumn scol ->
	      assertEquals( 'My Test String 2', scol.testString.getString() );
	      assertEquals( (int)123, scol.testInt.getInt() );
	      assertEquals( (long)1234, scol.testLong.getLong() );
	      assertEquals( (double)12345, scol.testDouble.getDouble() );
	      assertEquals( true, scol.testBoolean.getBoolean() );
	      assertEquals( uuid, scol.testUuid.getUUID() );
	      assertArrayEquals( bytes, scol.testDate.getByteArray() );
      }
    }
  }

  @Test   
  void testMultiGet() {
    int num = 10;
    long startKey = System.currentTimeMillis();
    def range = (startKey..startKey+num); 

    def supers = ['sup1', 'sup2'];
    GTransactionTemplate.execute{
	    keyspace.withColumnFamily( 'TestSuperColumnFamilyMulti' ) { GColumnFamily cf ->
	      range.each{ key -> 
		      supers.each{ String sup ->
	          cf[key][sup]['testCol'] << "${key}" ;
		      }
	      } 
	    } 
    }
    keyspace.withColumnFamily( 'TestSuperColumnFamilyMulti' ) { GColumnFamily cf ->
      def rows = cf.querySuper( range ) {
        assertNotNull( cf.queriedRows );
        range.each{ key ->
	        supers.each{ String sup ->
            assertEquals( "${key}", cf[key][sup]['testCol'].getString() );
	        }
        }
      }
      assertEquals( "Number of results do no match", num, rows.count-1 ) ;
    } 
  }
  
  @Test   
  void testInsertAndUpdateAndDelete() {
      GTransactionTemplate.execute{
        keyspace.withColumnFamily( 'TestSuperInsertAndUpdate' ) { GColumnFamily cf ->
	      cf['mykey1']['super1']['sub1'] << (int)123;
	      cf['mykey1']['super1']['sub2'] << (int)456;
        }
      } 
      
      keyspace.withColumnFamily( 'TestSuperInsertAndUpdate' ) { GColumnFamily cf ->
	      assertEquals( (int)123, cf['mykey1']['super1']['sub1'].getInt() );
	      assertEquals( (int)456, cf['mykey1']['super1']['sub2'].getInt() );
      }
       
      GTransactionTemplate.execute{
          keyspace.withColumnFamily( 'TestSuperInsertAndUpdate' ) { GColumnFamily cf ->
              cf['mykey1']['super1']['sub2'].delete();
          }
      }
      keyspace.withColumnFamily( 'TestSuperInsertAndUpdate' ) { GColumnFamily cf ->
	      assertEquals( null, cf['mykey1']['super1']['sub2'].getInt() );
	      assertEquals( (int)123, cf['mykey1']['super1']['sub1'].getInt() );
      }
      
      GTransactionTemplate.execute{
          keyspace.withColumnFamily( 'TestSuperInsertAndUpdate' ) { GColumnFamily cf ->
              cf['mykey1']['super1'].delete();
          }
      }
      
      keyspace.withColumnFamily( 'TestSuperInsertAndUpdate' ) { GColumnFamily cf ->
	      assertEquals( null, cf['mykey1']['super1']['sub1'].getInt() );
	      assertEquals( null, cf['mykey1']['super1']['sub2'].getInt() );
      }
  } 
  
  @Test 
  void testSuperRowGet() {
    GTransactionTemplate.execute{
	    keyspace.withColumnFamily( 'TestSuperColumnFamilyGet' ) { GColumnFamily cf ->
	        cf[5]['testCol1']['testSub1'] << "11" ;
	        cf[5]['testCol1']['testSub2'] << "12" ;
	        cf[5]['testCol2']['testSub1'] << "21" ;
	        cf[5]['testCol2']['testSub2'] << "22" ;
	    }
    }
    
      
    keyspace.withColumnFamily( 'TestSuperColumnFamilyGet' ) { GColumnFamily cf ->
      def values = [];
      cf[5].querySuper{ GRow row ->
        row.eachSliceColumn(String) { GColumn col ->
          logger.debug( "testSuperRowGet: column.name=${col.columnName}" );
          col.eachSubSliceColumn(String) { GSubColumn scol ->
          logger.debug( "testSuperRowGet: subcolumn.name=${scol.columnName}" );
            values.add( scol.getString() );
          } 
        }
      } 
      assertTrue( "Results does not contain '11'", values.contains('11') );
      assertTrue( "Results does not contain '12'", values.contains('12') );
      assertTrue( "Results does not contain '21'", values.contains('21') );
      assertTrue( "Results does not contain '22'", values.contains('22') );
    }
  } 
}
