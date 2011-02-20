package com.peer39.hadooper.loader;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.fs.Path;
import org.junit.Test;

public class HadoopToolsTest
{

	@Test
	public void testLs()
	{
		try
		{
			HadoopTools hadoop = new HadoopTools( "hdfs://dev100.dev.peer39dom.com:54310" );
			ArrayList<Path> ls = (ArrayList<Path>) hadoop.ls( "/user/hive/warehouse/sample_data" );
			for( Path filename : ls )
			{
				System.out.println( filename.toString() );
			}
		}
		catch ( IOException e )
		{
			e.printStackTrace();
		}
	}

	@Test
	public void testLsr()
	{
		try
		{
			HadoopTools hadoop = new HadoopTools( "hdfs://dev100.dev.peer39dom.com:54310" );
			ArrayList<Path> lst = (ArrayList<Path>) hadoop.lsr(  "/user/hive/warehouse/sample_data" );
			for( Path files : lst )
			{
				System.out.println(files);
			}
		}
		catch ( IOException e )
		{
			e.printStackTrace();
		}
	}

}
