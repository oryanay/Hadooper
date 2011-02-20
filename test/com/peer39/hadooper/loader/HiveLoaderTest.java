package com.peer39.hadooper.loader;

import static org.junit.Assert.*;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;

import org.junit.Before;
import org.junit.Test;

public class HiveLoaderTest
{
	private static String	driverName	= "org.apache.hadoop.hive.jdbc.HiveDriver";
	private static Connection con;

	@Before
	public void setup()
	{
		try
		{
			Class.forName( driverName );
			con = DriverManager.getConnection( "jdbc:hive://dev100.dev:10000/default", "", "" );
		}
		catch ( ClassNotFoundException e )
		{
			e.printStackTrace();
			fail("Class not found exception");
		}
		catch ( SQLException e )
		{
			e.printStackTrace();
			fail("sql exception");
		}
	}
	
	@Test
	public void testGetFiles() 
	{
		HiveLoader hive = new HiveLoader( con );
		try
		{
			ArrayList<String> ll = (ArrayList<String>) hive.getFiles( "/user/" );
			for( String filename : ll )
			{
				System.out.println(filename);
			}
		}
		catch ( SQLException e )
		{
			e.printStackTrace();
			assert(false);
		}
		
	}
}
