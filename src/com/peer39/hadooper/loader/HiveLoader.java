package com.peer39.hadooper.loader;

import java.io.File;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

import org.apache.commons.io.FileUtils;

public class HiveLoader
{
	Logger  logger = Logger.getLogger("HiveLoader");
	Connection con;
	
	
	public HiveLoader( Connection con )
	{
		super();
		this.con = con;
	}

	public void load( String filepath, String tablename ) throws SQLException
	{
		Statement stmt = con.createStatement();
		String sql = "load data local inpath '" + filepath + "' into table " + tablename;
	    logger.info( "Running: " + sql );
	    ResultSet res = stmt.executeQuery(sql);
	    while (res.next()) {
	    	logger.info ( res.getString(1) + "\t" + res.getString(2) );
	    }
	}
	
	public void scan(String path, String tablename, String... filetype) throws SQLException {
		ArrayList<File> files = (ArrayList<File>)FileUtils.listFiles(new File(path),filetype, true );
		for( File file : files )
		{
			if (!file.isDirectory())
			{
				load(file.getPath(),tablename);
			}
		}
	}
	
	public List<String> getFiles(String path) throws SQLException 
	{
		List<String> l = new ArrayList<String>();
		Statement stmt = con.createStatement();
		String sql = "dfs -ls " + path;
		ResultSet res = stmt.executeQuery(sql);
		
	    while (res.next()) {
	    	System.out.println("row:"+res.getRow());
	    	l.add( res.getString(0) );
	    }
	    return l;
	}
	
	
}
