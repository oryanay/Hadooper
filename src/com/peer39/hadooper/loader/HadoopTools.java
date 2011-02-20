package com.peer39.hadooper.loader;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class HadoopTools
{
	Configuration	conf	= new Configuration();
	String			conUrl;
	FileSystem		fs;

	public HadoopTools( String conUrl ) throws IOException
	{
		super();
		this.conUrl = conUrl;
		this.fs = FileSystem.get( URI.create( conUrl ), conf );
	}

	public FileStatus[] lsArray( String filepath ) throws IOException
	{
		FileStatus[] fStatus = fs.listStatus( new Path( filepath ) );
		return fStatus;
	}
	
	public List<Path> ls( String filepath ) throws IOException
	{
		ArrayList<Path> res = new ArrayList<Path>();
		FileStatus[] fStatus = lsArray( filepath );
		for( int i = 0; i < fStatus.length; i++ )
		{
			res.add( fStatus[i].getPath() );
		}
		return res;
	}

	public List<Path> lsr( String filepath ) throws IOException
	{
		ArrayList<Path> fileList = new ArrayList<Path>();
		FileStatus[] fStatus = lsArray( filepath );
		for( int i = 0; i < fStatus.length; i++ )
		{
			if( fStatus[i].isDir() )
			{
				listRecursively( fStatus[i], fileList );
			}
			else
			{
				fileList.add( fStatus[i].getPath() );
			}

		}
		
		return fileList;
	}

	public void listRecursively( FileStatus fdir, List<Path> files ) throws IOException
	{
		if( fdir.isDir() )
		{
			FileStatus[] fStatus = lsArray( fdir.getPath().toString() );
			for( int i = 0; i < fStatus.length; i++ )
			{
				listRecursively( fStatus[i], files );
			}
		}
		else
		{
			files.add( fdir.getPath() );
		}
	}
}
