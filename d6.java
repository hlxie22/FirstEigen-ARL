package MSQLDB;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DateFormat;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import javax.crypto.Cipher;
import javax.crypto.spec.SecretKeySpec;

import org.apache.spark.sql.functions.*;


import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.Encoders;


import com.csvreader.CsvWriter;
import com.mysql.jdbc.ResultSetMetaData;

import io.netty.util.concurrent.Future;

import org.apache.log4j.Logger;
import org.apache.commons.math3.stat.regression.OLSMultipleLinearRegression;
import org.apache.log4j.Level;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.codec.EncoderException;
import org.apache.commons.codec.language.Soundex;





public class d6 {
	static volatile String maxnumcol="";
	static volatile String results = "AB";
	static volatile String s = "";
	static volatile int c = 0;
	public static int numthread=800;
	public static int threshold=100;
	public static int fe=2;
	
	public static void main(String[] args) 
	{
		// TODO Auto-generated method stub
		
		Locale.setDefault(Locale.US); 
		Logger.getLogger("org").setLevel(Level.ERROR);
		Logger.getLogger("akka").setLevel(Level.ERROR);
		DecimalFormat df = new DecimalFormat("#.00");
		String tfs=System.getProperty("file.separator");
		
	/*			 
		String appConfigPath=args[0];
		
				 
		Properties appProps = new Properties();
		try {
			appProps.load(new FileInputStream(appConfigPath));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		*/
		long startTime = System.currentTimeMillis();
		String datatype="AB";
		String folderlocation="AB";
		String url = "AB";
		String scope = "AB";
		String db = "AB";
		String schema = "AB";
		String tables="AB";
		String uid = "AB";
		String pwd = "AB";
		
		String wh="AB";
		String profile="AB";
		
		
		
		datatype=args[0];
		if(datatype.matches("CSV||SAS"))
		{
			folderlocation=args[1];
			tables=args[2];
			scope = args[3];
			uid=args[6];
			results = args[8];
			profile="1";
		}
		if(datatype.matches("MSSQL||ORACLE"))
		{
			url = args[1];
			scope = args[3];
			db = args[4];
			schema = args[2];
			tables=args[5];
			uid = args[6];
			pwd = args[7];
			results = args[8];
			profile="1";
			
			if(url.contains(":"))
			{
				
			}
			else
			{
				if(datatype.equals("MSSQL"))
				{
					url=url+":"+1433;
				}
				if(datatype.equals("ORACLE"))
				{
					url=url+":"+1521;
				}
				
			}
			
		}
		
		if(datatype.equals("SNOWFLAKE"))
		{
			url = args[1];
			scope = args[3];
			db = args[4];
			schema = args[2];
			tables=args[5];
			uid = args[6];
			pwd = args[7];
			results = args[8];
			wh= args[9];
			profile="1";
		}
	
		/*
		numthread=Integer.parseInt(args[11]);
		int mode=Integer.parseInt(args[12]);
		int numcore=Integer.parseInt(args[13]);
		System.out.println("Num threads: "+numthread);
		*/
		
		/*
		String datatype=appProps.getProperty("datatype");
		String folderlocation=appProps.getProperty("folder");
		
		
		String url = appProps.getProperty("url");
		
		String scope = appProps.getProperty("scope");
		String db = appProps.getProperty("db");
		String schema = appProps.getProperty("schema");
		String tables=appProps.getProperty("tables");
		
		String uid = appProps.getProperty("uid");
		
		String pwd = appProps.getProperty("pwd");
		String results = appProps.getProperty("results");
		*/
		
		//  System.out.println("Connected to Database");
		
		NumberFormat formatter = new DecimalFormat("#0.00");     
//		//  System.out.println(formatter.format(4.0));

		
//		String tmpmode="local["+numcore+"]";

		SparkSession sc = SparkSession
				  .builder()
				  .appName("Arya - Automated Rules Discovery").master("local[*]").config("spark.driver.memory", "30g").config("spark.debug.maxToStringFields","200")
				  .config( "spark.driver.host", "localhost" )
				  .config("spark.scheduler.mode", "FAIR")
				  .config("spark.scheduler.listenerbus.eventqueue.size","100000")
				  .config("spark.sql.broadcastTimeout",  "36000")
				  .getOrCreate();
		
	try {
		
		//	sc.conf.set("spark.sql.shuffle.partitions", 6)
	//	sc.conf().set("spark.executor.memory", "7g");
		
		String appConfigPath="license.txt";
		Path wiki_path=Paths.get(appConfigPath);
		
		byte[] wikiArray = Files.readAllBytes(wiki_path);
		
		String strKey="This is Angsuman";
		
		SecretKeySpec skeyspec=new SecretKeySpec(strKey.getBytes(),"Blowfish");
		Cipher cipher=Cipher.getInstance("Blowfish");
		cipher.init(Cipher.DECRYPT_MODE, skeyspec);
		byte[] decrypted=cipher.doFinal(wikiArray);
		String strData=new String(decrypted);
		String[] sst=strData.split(",");
		String sst1=sst[sst.length-1];
//		System.out.println(sst1);
		
		DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
		Date date1 = new Date();
//		System.out.println(dateFormat.format(date1));
		
        Date date2 = dateFormat.parse(sst1);
        int mmm=0;
      
        if (date2.compareTo(date1) > 0) 
        {
//        	System.out.println(" This is not expired yet");
        	mmm=0;
        }
        else
        {
        	System.out.println(" License has expired - Please contact info@pricchaa.com ");
        	mmm=1;
        }
		
		
		
		if(mmm==0)
		{
		
	
		List<String> tablelist = new ArrayList<String>();
		String sdbURL="";
		
		if(datatype.equals("MSSQL"))
		{
		
		DriverManager.registerDriver(new com.microsoft.sqlserver.jdbc.SQLServerDriver());
				
		String dbURL = "jdbc:sqlserver://"+url+";DatabaseName="+db+";user="+uid+";password="+pwd;
		sdbURL="jdbc:sqlserver://"+url+";DatabaseName="+db;
		Connection conn = DriverManager.getConnection(dbURL);
				
		Statement stmt = conn.createStatement();
		stmt.setFetchSize(1000);
		
		
		

		
		if (conn != null) {
		
		}
				
		
		stmt.setFetchSize(1000);
	
		String sql = "SELECT TABLE_NAME\n" + 
				"FROM INFORMATION_SCHEMA.TABLES\n" + 
				"WHERE TABLE_TYPE = 'BASE TABLE' AND TABLE_CATALOG='"+db+"' AND TABLE_SCHEMA='"+schema+"'";
		//res = stmt.executeQuery(sql);
		// show tables
				
		ResultSet res = stmt.executeQuery(sql);
		
		
		
		
		if(scope.equals("ALL"))
		{
			while(res.next())
			{
				tablelist.add(res.getString(1));
			}
		}
		else
		{
			String[] tbl =tables.split(",");
			
			for(int nn=0;nn<tbl.length;nn++)
			{
				if(!tbl[nn].equals(""))
				{
					tablelist.add(tbl[nn]);
				}
			}
			
		}
		
		}
		
		
		if(datatype.equals("ORACLE"))
		{
		
		DriverManager.registerDriver(new oracle.jdbc.driver.OracleDriver());
				
		String dbURL = "jdbc:oracle:thin:@"+url+":"+schema;
		sdbURL=dbURL;
//		System.out.println(dbURL);
		Connection conn =  DriverManager.getConnection(dbURL, uid, pwd);	
		
				
		Statement stmt = conn.createStatement();
		stmt.setFetchSize(1000);
	
	
		String sql = "SELECT table_name FROM user_tables";
		//res = stmt.executeQuery(sql);
		// show tables
				
		ResultSet res = stmt.executeQuery(sql);
		
		
		
		
		if(scope.equals("ALL"))
		{
			while(res.next())
			{
				tablelist.add(res.getString(1));
			}
		}
		else
		{
			String[] tbl =tables.split(",");
			
			for(int nn=0;nn<tbl.length;nn++)
			{
				if(!tbl[nn].equals(""))
				{
					tablelist.add(tbl[nn]);
				}
			}
			
		}
		
		}
		
		
		
		
		if(datatype.matches("CSV||SAS"))
		{
			
			String[] tbl =tables.split(",");
			
			for(int nn=0;nn<tbl.length;nn++)
			{
				if(!tbl[nn].equals(""))
				{
					tablelist.add(tbl[nn]);
				}
			}	
			
			
			
			
		}
		
		if(datatype.equals("SNOWFLAKE"))
		{
		
		DriverManager.registerDriver(new net.snowflake.client.jdbc.SnowflakeDriver());
				
		String dbURL = "jdbc:snowflake://"+url+"/?db="+db+"&user="+uid+"&password="+pwd;
//		String dbURL = "jdbc:sqlserver://"+url+":1433;DatabaseName="+db+";user="+uid+";password="+pwd;
//		sdbURL="jdbc:sqlserver://"+url+":1433;DatabaseName="+db;
		Connection conn = DriverManager.getConnection(dbURL);
				
		Statement stmt = conn.createStatement();
		stmt.setFetchSize(1000);
		
		
		

		
		if (conn != null) {
		
		}
				
		
		stmt.setFetchSize(1000);
	
		String sql ="show tables in "+db+" ."+ schema;
		//res = stmt.executeQuery(sql);
		// show tables
				
		ResultSet res = stmt.executeQuery(sql);
		
		
		
		
		if(scope.equals("ALL"))
		{
			while(res.next())
			{
				tablelist.add(res.getString(2));
			}
		}
		else
		{
			String[] tbl =tables.split(",");
			
			for(int nn=0;nn<tbl.length;nn++)
			{
				if(!tbl[nn].equals(""))
				{
					tablelist.add(tbl[nn]);
				}
			}
			
		}
		
		conn.close();
		}
		
		
		
		String directory = new File(".").getCanonicalPath();
		String fdir=directory+tfs+"rules1";
		File file = new File(fdir);
//		System.out.println(fdir);
		boolean isDirectoryCreated = file.mkdir();

		if (isDirectoryCreated) {
		      

		} else {
		       deleteDir(file);  // Invoke recursive method
		       file.mkdir();   
		     
		}
	
		
		fdir=directory+tfs+"results1";
		File file2 = new File(fdir);
//		System.out.println(fdir);
		isDirectoryCreated = file2.mkdir();

		if (isDirectoryCreated) {
			
		       
		} else {
	       deleteDir(file2);  // Invoke recursive method
	       file2.mkdir();       
		}
	
		fdir=directory+tfs+"Anomaly1";
		File file3 = new File(fdir);
//		System.out.println(fdir);
		isDirectoryCreated = file3.mkdir();

		if (isDirectoryCreated) {
			
		       
		} else {
		       deleteDir(file3);  // Invoke recursive method
		       file3.mkdir();       
		}
		
		
		
		
		int m=1;
		for(int kk=0;kk<tablelist.size();kk++)
		{
			try {
			
			//  System.out.println("******************************************");
			
			  System.out.println("Analyzing Table " + tablelist.get(kk));
				
			
			
			String f2=schema+"_"+tablelist.get(kk)+"_DQ_rules.csv";
			
			f2=directory+tfs+"rules1"+tfs+f2;
			//System.out.println(f2);
			  
		    FileWriter fw2 = new FileWriter(f2);
		    
			PrintWriter p2 = new PrintWriter(fw2);
			
			p2.println("DataQualityDomain,PrimaryColumn,RuleDescription,Threshold_RiskFactor");
			p2.flush();
			
		
			String f3=schema+"_"+tablelist.get(kk)+"_DQ_Results.csv";
			f3=directory+tfs+"results1"+tfs+f3;
		    FileWriter fw3 = new FileWriter(f3);
		    
			PrintWriter p3 = new PrintWriter(fw3);
			
			String fd1=schema+"_"+tablelist.get(kk)+"_DQ_Anomaly_";
			String fd=directory+tfs+"Anomaly1"+tfs+fd1;
			
			p3.println("DataQualityDomain,PrimaryColumn,RuleDescription,Status,Details,rCount");
			
			Dataset<Row> jdbcDF=null;
			
			
			if(datatype.equals("MSSQL"))
			{
			String tbl1=schema+".["+tablelist.get(kk).toString()+"]";
			
			String tbl= "(SELECT * FROM "+ tbl1 + "  WHERE (ABS(CAST( (BINARY_CHECKSUM (*)*RAND()) as int)) % 100) < 10 ) as tmp";
			
			

//			System.out.println(tbl);
		   jdbcDF = sc.read()
					  .format("jdbc")
					  .option("url", sdbURL)
					  .option("dbtable",tbl )
					  .option("user", uid)
					  .option("password", pwd)
					  .load();
		   
		   if(jdbcDF.count()<100000)
		   {
			   jdbcDF = sc.read()
						  .format("jdbc")
						  .option("url", sdbURL)
						  .option("dbtable",tbl1 )
						  .option("user", uid)
						  .option("password", pwd)
						  .load();
		   }
		   
		   
			}
			
			if(datatype.equals("ORACLE"))
			{
			String tbl1=tablelist.get(kk).toString();
			
			

			String tbl= "(SELECT * FROM "+ tbl1 + "  SAMPLE(5) )  tmp";

//			System.out.println(tbl);
		   jdbcDF = sc.read()
					  .format("jdbc")
					  .option("url", sdbURL)
					  .option("dbtable",tbl )
					  .option("user", uid)
					  .option("password", pwd)
					  .option("fetchsize", 120000)
					  .load();
		   
		   if(jdbcDF.count() > 100000)
		   {
			   double f1=100000.00/jdbcDF.count();
			   
			  
			   jdbcDF = jdbcDF.sample(false, f1);
		   }
		   else
		   {
			   int sample=99;
			   int totc=(int) (20*jdbcDF.count());
			   if(totc<=100000)
			   {
				  
			   }
			   else
			   {
				   sample=(100000*100)/totc;
				   
				   
			   }
			   
			   tbl= "(SELECT * FROM "+ tbl1 + "  SAMPLE("+sample+") )  tmp";
			   jdbcDF = sc.read()
						  .format("jdbc")
						  .option("url", sdbURL)
						  .option("dbtable",tbl )
						  .option("user", uid)
						  .option("password", pwd)
						  .option("fetchsize", 120000)
						  .load();
			   
		   }
		   
		   
		   
			}
			
			
			
			
			if(datatype.equals("SNOWFLAKE"))
			{
			String tbl1=schema+"."+tablelist.get(kk).toString()+"";
			
//			String tbl= "(SELECT * FROM "+ tbl1 + "  WHERE (ABS(CAST( (BINARY_CHECKSUM (*)*RAND()) as int)) % 100) < 10 ) as tmp";
			String tbl= "SELECT * FROM "+ tbl1 + "  sample (120000 rows)";
//			System.out.println(sdbURL);
//select * from testtable sample (10 rows);			

//			System.out.println(url);
			String[] tM=url.split("[.]");
		  
		   jdbcDF = sc.read()
					  .format("net.snowflake.spark.snowflake")
					  .option("sfurl", url)
					  .option("sfAccount", tM[0])
					  .option("sfDatabase",db)
					  .option("sfSchema", schema)
					  .option("query",tbl )
					  .option("sfWarehouse", wh)
					   .option("sfUser", uid)
					  .option("sfPassword", pwd)
					  .load();
		   
//		   jdbcDF.show();
		   
		   if(jdbcDF.count()<100000)
		   {
			   String tbl11="SELECT * FROM "+ tbl1 ;
			   jdbcDF = sc.read()
					   .format("net.snowflake.spark.snowflake")
					   .option("sfurl", url)
						  .option("sfAccount", tM[0])
						  .option("sfDatabase",db)
						  .option("sfSchema", schema)
						  .option("query",tbl11 )
						  .option("sfWarehouse", wh)
						   .option("sfUser", uid)
						  .option("sfPassword", pwd)
						  .load();
						  
		   }
		   
		   
			}
			
			if(datatype.equals("CSV"))
			{
			
			String tbl=folderlocation+tfs+tablelist.get(kk).toString();
			
	//		System.out.println(tbl);
			
			jdbcDF = sc.read().format("csv").option("header", "true").option("delimiter", uid).load(tbl);
			
			}
		
			if(datatype.equals("SAS"))
			{
			
			String tbl=folderlocation+tfs+tablelist.get(kk).toString();
			
//			System.out.println(tbl);
			
			jdbcDF = sc.read().format("com.github.saurfang.sas.spark").load(tbl);
			
			
			
			}
			
			System.out.println("DATA Loaded");
		    
		    jdbcDF.cache();
			
			
			
		   
		    
		    
			int reccount=(int) jdbcDF.count();
			
			double fraction=10000.0/reccount;
			 
			if(fraction >1)
			{
				fraction=1.0;
			}
			
			
			
			double fraction1=10000.0/reccount;
			
			if(fraction1>1)
			{
				fraction1=1.0;
			}
	
			double fraction2=10000.0/reccount;
			
			if(fraction2>1)
			{
				fraction2=1.0;
			}
			
			
			
			List<String> columnNames= new ArrayList<String>();
			
			List<String> primarycolList = new ArrayList<String>();
			List<String> secondarycolList= new ArrayList<String>();
			List<String> numericalRelationship= new ArrayList<String>();
			List<String> numericalRelationship2= new ArrayList<String>();
			List<String> DateRelationship= new ArrayList<String>();
			List<String> DateRelationshipFormat= new ArrayList<String>();
			List<String> AnomalyRelationship= new ArrayList<String>();
			List<String> Microsegment= new ArrayList<String>();
			List<String> Microsegment2= new ArrayList<String>();
			List<String> MicrosegmentFormat= new ArrayList<String>();
			List<String> nullcollist= new ArrayList<String>();
			List<String> nullthreshold= new ArrayList<String>();
			List<String> LengthRelationship= new ArrayList<String>();
			List<String> Microsegment2Format= new ArrayList<String>();
			List<String> NullColTarget= new ArrayList<String>();
			List<Double> NullColTargetP= new ArrayList<Double>();
			List<String> Microsegment5= new ArrayList<String>();
			List<String> Microsegment3= new ArrayList<String>();
			List<String> rangeRelationship2= new ArrayList<String>();
			List<String> rangeRelationship3= new ArrayList<String>();
			List<String> numericalRelationship3= new ArrayList<String>();
			List<String> stringcolumns=new ArrayList<String>();
			List<String> numbericcolumns=new ArrayList<String>();
			List<String> datecolumns=new ArrayList<String>();
			List<String> colTypes=new ArrayList<String>();
			List<String> s2types=new ArrayList<String>();
			List<String> NullColTargetType= new ArrayList<String>();
			List<String> NullColTargetFormat= new ArrayList<String>();
			List<String> NullColTarget2S= new ArrayList<String>();
			List<String> NullColTarget2D= new ArrayList<String>();
			List<String> NullColTarget2DType= new ArrayList<String>();
			List<String> NullColTarget2DFormat= new ArrayList<String>();
			List<String> NullColTarget2= new ArrayList<String>();
			List<String> LengthList= new ArrayList<String>();
			
			double maxnumucount=0.0;
			
			
			String col[] =jdbcDF.columns();
			int columnCount=col.length;
			
			String tmpcol="";
			
			for (int i = 0; i < col.length; i++ ) {
				  String name = "`"+col[i]+"`";
				  
				  tmpcol=tmpcol+"Cast( "+name+" as string)"+",";
				  
				  columnNames.add(name);
				  
				  
				}
			
			if(tmpcol.endsWith(","))
	   		{
				tmpcol = tmpcol.substring(0,tmpcol.length() - 1); 
	   		}
			
						
			
			jdbcDF.createOrReplaceTempView("S1");
			String tms1="Select "+ tmpcol + " from S1";
			Dataset<Row> jdbcDFS2A = sc.sql(tms1);
			
			String tmpcol1="";
			for (int i = 0; i < col.length; i++ ) {
				  String name = "`"+col[i]+"`";
				  
				  tmpcol1=tmpcol1+ "Case when "+name+"='NULL' then '' else "+name +"end as "+name+",";
				}
			
			if(tmpcol1.endsWith(","))
	   		{
				tmpcol1 = tmpcol1.substring(0,tmpcol1.length() - 1); 
	   		}
			
			
			jdbcDF.createOrReplaceTempView("S11");
//			jdbcDF.show();
			String tms2="Select "+ tmpcol1 + " from S11";
//			System.out.println(tms2);
			Dataset<Row> jdbcDFS2 = sc.sql(tms2);
			
			Dataset<Row> jdbcDFS1=jdbcDFS2.sample(false, fraction);
			
//			jdbcDF1.show();
			
			jdbcDFS1.cache();
			
			 String description="";
			
			Dataset<Row> jdbcDD1=jdbcDFS2.sample(false, fraction1);
			jdbcDD1.cache();
			
			Dataset<Row> jdbcDD2=jdbcDFS2.sample(false, fraction2);
			jdbcDD2.cache();
			
			int rc=(int) jdbcDD1.count();
			int rccount=(int) jdbcDFS1.count();
			
			
			ExecutorService executor = Executors.newFixedThreadPool(numthread);
			List<java.util.concurrent.Future<String>> futuresList = new ArrayList<>();
			
			for(int j=0;j<columnCount;j++)
			{
				String st1=columnNames.get(j);
				
				Callable<String> task = new metaRules(st1,jdbcDD1,sc, rc);
				java.util.concurrent.Future<String> future =  executor.submit(task);
				futuresList.add(future);

//				System.out.println("Running result for: "+st1);
				
			}
			
			for (java.util.concurrent.Future<String> future : futuresList) {
				String calculationResult = null;
				try {
					calculationResult = future.get();
				} catch (InterruptedException | ExecutionException e) {
					// ... Exception handling code ...
		        } 
				s2types.add(calculationResult);
		    }
			executor.shutdown();
			
//			System.out.println("Number returned "+s2types.size());
//			System.out.println("Total column "+columnNames.size());
			
			for (int j=0;j<columnNames.size();j++)
			{
//				System.out.println(""+columnNames.get(j));
			}
			
			for (int j=0;j<columnNames.size();j++)
			{
				if(s2types.get(j) != null)
				{
				String as2=s2types.get(j);
//				System.out.println(as2);
				String[] as3=as2.split("ADUTTA");
				String s2=as3[1];
				String colname=as3[0];
								
				}
			}
			
			
			for(int j=0;j<s2types.size();j++)
			{
				
				if(s2types.get(j) != null)
				{
	
				String as2=s2types.get(j);
	//			System.out.println(as2);
				String[] as3=as2.split("ADUTTA");
				String s2=as3[1];
				String colname=as3[0];
				
				System.out.println("Analyzing "+colname);
//				System.out.println("Analyzing "+s2.substring(0,1));
				colTypes.add(s2.substring(0,1));
				
//				System.out.println("Testing "+colname+" Type "+ s2.substring(0,1));
				
				String[] strp=s2.split("-");
							
				
				String Ctype=strp[0];
				String coltype="";
				
				if(Ctype.startsWith("D"))
				{
				coltype="D";
				}
				if(Ctype.startsWith("I"))
				{
					coltype="I";
					
				}
				if(Ctype.startsWith("F"))
				{
					coltype="F";
				}
				if(Ctype.startsWith("S"))
				{
					coltype="S";
					
				}
				if(Ctype.startsWith("E"))
				{
					coltype="E";
					
				}
				String colformat="";
				
				double nullc=0;
				double ucount=0;
				
				
				if(!s2.startsWith("D"))
				{
				 nullc=Double.parseDouble(strp[1]);
				 ucount=Double.parseDouble(strp[2]);
				}
				else
				{
					
//					System.out.println("In date " +s2);
					String[] strp1=s2.split(",");
					String strp2=strp1[1];
					String[] strp3=strp2.split("-");
					nullc=Double.parseDouble(strp3[1]);
					ucount=Double.parseDouble(strp3[2]);
					
//					System.out.println("Null c "+ nullc+" --"+ ucount);
					
					int ind=0;
					String strp4[]=strp1[0].split(">");
					
					for (int jj=0;jj<strp4.length;jj++)
					{
						String ss[]=strp4[jj].split("Count");
//						System.out.println(strp4[jj]);
						int icount=Integer.parseInt(ss[1]);
//						System.out.println("T1"+icount*100/(rccount*(100-nullc)/100));
						
						if(icount*100/(rccount*(100-nullc)/100)> 95.0 )
						{
						
						String dd[]=ss[0].split(":");
						colformat=dd[1];
						
						}
						
					}
				
					
					
					
					
				}
				
				
				
				
				int nullc1= (int) (nullc)+1;
				
				int ucount2=0;
				
				
			//	ucount2=getUniqueCount(colname,jdbcDD2,sc);
				
				ucount2=(int) (ucount*(100-nullc)*rc)/(100*100);
				
				
				
				int range=0;
				
				
			    
				
				
				if(nullc <20 )
				{
					
					
					 if(nullc<0.01 && ucount >99.9 && (Ctype.equals("S")||Ctype.equals("I")||Ctype.startsWith("D")))
					   {
						   primarycolList.add(colname);
						   p2.println("Uniqueness,"+colname.replaceAll("`", "")+",Can not be duplicate, ");
						   p2.flush();
						   if(results.equals("1"))
						   findduplicatecount(colname,jdbcDD2,sc,p3);
						   
						   
					   }
					   else
					   {
						   
						   secondarycolList.add(colname);
						   
						   
					   }
					
					
					
					
				}
				
				//Microsegment 
				
				
				
				
				// null check 
				
				if(nullc<=0.05)
				{
					 p2.println("Completeness,"+colname.replaceAll("`", "")+", Can not be Null, ");
					 p2.flush();
					 double threshold=0.0;
					 nullcollist.add(colname);
					 nullthreshold.add(Double.toString(threshold));
				}
				if(nullc>0.05 && nullc<=10)
				{
					
					 p2.println("Completeness,"+colname.replaceAll("`", "")+", Percent Null must be less than,"+nullc1 );
					 p2.flush();
					 
					 nullcollist.add(colname);
					 nullthreshold.add(Double.toString(nullc1));
				}
				
				
				String tbl2="reserved.csv";
				
//				jdbcDD1.show();
				Dataset<Row> jR = sc.read().format("csv").option("header", "true").load(tbl2);
				
				Row[] dataRows = (Row[]) jR.collect();
				
			
				int indr=0;
				
				for (Row row : dataRows) 
				
				{
					String ssst1=row.get(0).toString();
					if(colname.replaceAll("`", "").toUpperCase().startsWith(ssst1) || colname.replaceAll("`", "").toUpperCase().endsWith(ssst1))
					{
						indr=indr+1;
					}
					
				}
				
				if(nullc>5 && nullc<50 && indr==0 && (Ctype.startsWith("D")||Ctype.startsWith("S")||Ctype.startsWith("F")||Ctype.startsWith("I")))
				{
					NullColTarget.add(colname);
					if(Ctype.startsWith("D"))
					{
					NullColTargetType.add("D");
					String[] strp1=s2.split(":");
					
					String[] td1=strp1[1].split("Count");
			//		System.out.println(td1[0]);
					NullColTargetFormat.add(td1[0]);
					}
					if(Ctype.startsWith("I"))
					{
					NullColTargetType.add("I");
					NullColTargetFormat.add("None");
					}
					if(Ctype.startsWith("F"))
					{
					NullColTargetType.add("F");
					NullColTargetFormat.add("None");
					}
					if(Ctype.startsWith("S"))
					{
					NullColTargetType.add("S");
					NullColTargetFormat.add("None");
					}
					
					NullColTargetP.add(nullc);
					
					if(ucount2>=2 && ucount2<=10 && (Ctype.startsWith("S")||Ctype.startsWith("F")||Ctype.startsWith("I")) )
					{
						
						NullColTarget2S.add(colname);
					}
					else
					{
						NullColTarget2D.add(colname);
						NullColTarget2DType.add(coltype);
						NullColTarget2DFormat.add(colformat);
					}
					
				
						
					
					
					
					
				}
				
				//date relationship
				
				if(Ctype.startsWith("D"))
				{

					p2.println("Conformity,"+colname.replaceAll("`", "")+", Valid Format :" +colformat);
					p2.flush();
					
					if(primarycolList.contains(colname))
					{
						
					}
					else
					{
					DateRelationship.add(colname);
					DateRelationshipFormat.add(colformat);
					}
					
				}
				
				if(Ctype.startsWith("I") || Ctype.startsWith("S") || Ctype.startsWith("F"))
				{
					if(Ctype.startsWith("I"))
						p2.println("Conformity,"+colname.replaceAll("`", "")+", Valid Format: Integer" );
						
					if(Ctype.startsWith("S"))
						p2.println("Conformity,"+colname.replaceAll("`", "")+", Valid Format: AlphaNumeric" );
					if(Ctype.startsWith("F"))
						p2.println("Conformity,"+colname.replaceAll("`", "")+", Valid Format: Decimal" );
					
					p2.flush();
					
				}
				
			
				

				
				
				int indn=0;
				
			if(indr==0)
			{
				
				
			// Numerical Relationship	
			
				
				
				if(nullc<30 && ucount2>5 && ucount<=99.9  && (Ctype.equals("I") || Ctype.equals("F") ))
				{
				
				
					if(Ctype.equals("I"))
					{
						range=getRange(colname,jdbcDD2,sc);

					}
					
				 if(Ctype.equals("F"))
				 {
				 rangeRelationship3.add(colname);
	     		 indn=1;
				 }
				 if(Ctype.equals("I") && (ucount2 >50 || range > 10000))
				 {
				 rangeRelationship3.add(colname);
	     		 indn=1; 
				 }
			    
	    		}
				
				
				if(nullc<30 && ucount2>=5  && (Ctype.equals("I") || Ctype.equals("F") ))
				{
					
					numericalRelationship2.add(colname);
									
				}
				
				
				
				
				if(nullc<95 && ucount <90 && (Ctype.equals("I") || Ctype.equals("F") ) && indn==0)
				{
					
					rangeRelationship2.add(colname);
				
	
										
				}
				
			}
				
			
				
				if(nullc<30 && ucount2>0 && ucount2<100 && (Ctype.equals("S")||Ctype.equals("I")) && indr==0 && indn==0)
				   {
			
					
					  if(nullc<5 && ucount2 >=2 && ucount2<=50)
						{
							Microsegment.add(colname);
		
						}
			
					    Microsegment2.add(colname);
						Microsegment2Format.add(Ctype);
					
						 if(nullc<5 && ucount2 >=2 && ucount2<=10)
						 {
							 Microsegment3.add(colname);
						 }
				   }
				
				
				
				
				
				if(nullc<15 && ucount2>=0 && ucount<50 && strp[3].equals("Fixed") && (Ctype.equals("I") || Ctype.equals("S")))
				{
					
					p2.println("Conformity,"+colname.replaceAll("`", "")+", Length must be," +strp[4]);
					String lt=colname+":"+strp[4];
					LengthRelationship.add(lt);
					p2.flush();
					
					
				}
				
				if(nullc<15 && ucount2>=0 && ucount<50 && strp[3].equals("Variable") && (Ctype.equals("I") || Ctype.equals("S"))  )
				{
//					System.out.println(colname+"--variable "+ strp[5].split(":"));
					
					String[] cntStr=strp[5].split(":");
					int lendist=cntStr.length;
					int[] arr = new int[lendist];
					int[] si = new int[lendist];
					for(int i=0;i<lendist;i++)
					{
						String[] tmp=cntStr[i].split("Count");
						si[i]=Integer.parseInt(tmp[0]);
						arr[i]=Integer.parseInt(tmp[1]);
					}
					
					int first=0;
					int second=0;
					
			        for (int i = 0; i < lendist ; i++) 
			        { 
			            /* If current element is smaller than  
			            first then update both first and second */
			            if (arr[i] > first) 
			            { 
			                second = first; 
			                first = arr[i]; 
			            } 
			       
			            /* If arr[i] is in between first and  
			               second then update second  */
			            else if (arr[i] > second && arr[i] != first) 
			                second = arr[i]; 
			        } 
			        
			        int firstindex=0;
			        int secondindex=0;
					
					for(int i=0;i<lendist;i++)
					{
						if(arr[i]==first)
						{
							firstindex=i;
						}
						if(arr[i]==second  )
						{
							secondindex=i;
						}
					}
					
					
					
					
					String desiredlen="";
					
					int minlimit=(int) (0.99*rc);
					int slimit=(int)(0.01*rc);
					
//					System.out.println(colname+"--"+first+"--"+second);
					
					if(first+second>minlimit )
					{
						
								desiredlen=desiredlen+si[firstindex];
							
							if( second >slimit )
							{
								desiredlen=desiredlen+"-"+si[secondindex];
							}
						
							
						
					//	rule=rule+"Length rule: "+ desiredlen +"-";
						p2.println("Conformity,"+colname.replaceAll("`", "")+", Length must be, '" +desiredlen+"'");
						String lt=colname+":"+desiredlen;
						LengthRelationship.add(lt);
						p2.flush();
						
						}
						
				
					}
					
					
					
			
				
				
				
				
				
				
				
				
			}
				
				
			}
			
//			System.out.println("Total col types "+colTypes.size());
			// correlationship 
			
			
	//		
			
			
		
			String ff2=schema+"_"+tablelist.get(kk)+"_DQ_notes.csv";
			
			ff2=directory+tfs+"rules1"+tfs+ff2;
			//System.out.println(f2);
			  
		    FileWriter ffw2 = new FileWriter(ff2);
		    
			PrintWriter pp2 = new PrintWriter(ffw2);
			
			
			
			for (int i=0;i<numericalRelationship2.size();i++)
			{
//				System.out.println("Numerical relationship"+numericalRelationship2.get(i));
				pp2.println("Numerical relationship "+numericalRelationship2.get(i));
				
			}
			
			for (int i=0;i<numericalRelationship.size();i++)
			{
//			System.out.println("Numerical "+numericalRelationship.get(i));
			pp2.println("Numerical  "+numericalRelationship.get(i));
			}
			
			for (int i=0;i<Microsegment.size();i++)
			{
//				System.out.println("Microsegment"+Microsegment.get(i));
				pp2.println("Microsegment"+Microsegment.get(i));
				
			}
			
			for (int i=0;i<Microsegment2.size();i++)
			{
//			System.out.println("Drift"+Microsegment2.get(i));
			pp2.println("Drift"+Microsegment2.get(i));
			}
			
			for (int i=0;i<LengthRelationship.size();i++)
			{
//			System.out.println("Length "+LengthRelationship.get(i));
			pp2.println("Length "+LengthRelationship.get(i));
			}
			
			
			
			
			
			for (int i=0;i<NullColTarget2S.size();i++)
			{
//			System.out.println("NullColTarget2S "+NullColTarget2S.get(i));
				pp2.println("NullColTarget2S "+NullColTarget2S.get(i));
			}
			
			for (int i=0;i<NullColTarget2D.size();i++)
			{
//				System.out.println("NullColTarget2D "+NullColTarget2D.get(i));
				pp2.println("NullColTarget2D "+NullColTarget2D.get(i));
			}
			
			for (int i=0;i<rangeRelationship3.size();i++)
			{
//				System.out.println("RangeRelationship3 "+rangeRelationship3.get(i));
				pp2.println("RangeRelationship3 "+rangeRelationship3.get(i));
			}
			
			pp2.flush();
			pp2.close();
			
		
	
			
		
			
			
	     				
	 NullRules(Microsegment3, NullColTarget2S,  NullColTarget2D, NullColTarget2DType,NullColTarget2DFormat, jdbcDFS1, jdbcDFS1,  sc, p2,  p3, results );	

			
		ProfileRules1( columnNames,  colTypes, DateRelationship, DateRelationshipFormat, jdbcDFS2,  sc, schema,tablelist.get(kk) );
		 ProfileRules2( Microsegment,  Microsegment2,  jdbcDFS2,  sc, schema,tablelist.get(kk) );
		 ProfileRules3( rangeRelationship3,  jdbcDFS2,  sc, schema,tablelist.get(kk) );
 //    		 SProfile(columnNames, jdbcDFS2,  sc,  schema,  tablelist.get(kk));
			
		 
//		 NullRules(Microsegment3, NullColTarget2S,  NullColTarget2D, NullColTarget2DType,NullColTarget2DFormat, jdbcDFS1, jdbcDFS1,  sc, p2,  p3, results );
     	
     	
     	 if(Microsegment.size()>0 && rangeRelationship3.size()==0 )
     		MicrorulesV3(columnNames, jdbcDFS1,jdbcDFS1, sc,  p2,  Microsegment);
     	 
		
     	if(DateRelationship.size()>0)
			findDateCorr(columnNames, jdbcDFS1,jdbcDFS1, sc,  p2, p3, DateRelationship,DateRelationshipFormat,results);
     	
     	
     	
     	
	     
     	if(fe ==1)
     	{
		
//     		NullRules(Microsegment, NullColTarget2S,  NullColTarget2D, NullColTarget2DType,NullColTarget2DFormat, jdbcDFS1, jdbcDFS1,  sc, p2,  p3, results );
     		
     		
     		/*
     		if(Microsegment.size()>0 && NullColTarget2S.size()>0 )
    			NullRules(Microsegment, NullColTarget2S,  jdbcDFS1, jdbcDFS1,  sc, p2,  p3 );
         	
         	if(Microsegment.size()>0 && NullColTarget2D.size()>0 )
    				NullRulesV2(Microsegment, NullColTarget2D, NullColTarget2DType,NullColTarget2DFormat, jdbcDFS1, jdbcDFS1,  sc, p2,  p3 );
     		
     		 if(Microsegment.size()>0)
			 	ComboRules(Microsegment,   jdbcDFS1,  jdbcDFS1,  sc, p2,  p3 );
     		
     		if(rangeRelationship3.size()>0 && Microsegment.size()>0 )
	     		MicrorulesV2( rangeRelationship3, Microsegment,  jdbcDFS1,  jdbcDFS1,  sc,  p2, p3);
		
     		
     		*/
     		
		 if(rangeRelationship2.size()>0)
				 Rangerules( rangeRelationship2,  jdbcDFS1,  jdbcDFS1,  sc,  p2, p3);
		 
		 if(numericalRelationship2.size()>1)
				findCorr(jdbcDFS1, jdbcDFS1,sc,  p2, p3, numericalRelationship2, results );
		 
		 if(DateRelationship.size()>0)
				findDateCorr(columnNames, jdbcDFS1,jdbcDFS1, sc,  p2, p3, DateRelationship,DateRelationshipFormat,results);
		
		 if(Microsegment2.size()>0 )
				findDrift(jdbcDFS1,jdbcDFS1, sc,  p2, p3, Microsegment2);
		 
	
		 if(results.equals("1"))
				evaluaterules(nullcollist,jdbcDFS1,sc,p3,nullthreshold,secondarycolList,primarycolList,LengthRelationship,Microsegment2);
		 
		 
		 
		 
			
     	}			
			
  //   		
			
			
	
			
	//		 eualityRules( columnNames,  colTypes, Microsegment5,jdbcDFS1,  sc, p2);
				
			
				
				
	
			
			
				
	
				
			
			
	//		ExecutorService executor = Executors.newFixedThreadPool(4);
	//		java.util.concurrent.Future<Integer> future =  executor.submit(task);
			
			
	//		java.util.concurrent.Future<Integer> future1 =  executor.submit(task2);
		
				/*
			Integer r1 = future.get();
			Integer r2 = future1.get();
			Integer r3 = future2.get();
			*/
			
			
				
			p2.close();
			p3.close();
			
			
			
			
			}
			catch(Exception e)
			{
				e.printStackTrace();
			}
		
			System.out.println("Completed Processing Table "+tablelist.get(kk));
		}
		String fdirrules=directory+tfs+"rules1";
		String fdirresults=directory+tfs+"results1";
		String pdirresults=directory+tfs+"profile1";
		System.out.println("Rules are  available in : " + fdirrules);
		System.out.println("Results are available in : " + fdirresults);
		System.out.println("Profiles are available in : " + pdirresults);
		
		
		File file4 = new File(fdirrules);
		File[] files = file4.listFiles();

	    for (File myFile: files) 
	    {
	    
	    String fname=myFile.getAbsolutePath();
	    
	    if(fname.endsWith("_DQ_rules.csv"))
	    {
	    
	    Dataset<Row> 	jdbcRules = sc.read().format("csv").option("header", "true").load(fname);
	    jdbcRules.createOrReplaceTempView("M1");
	    jdbcRules.show();
	    String S1="select DataQualityDomain, count(*) as countA from M1 group by DataQualityDomain";
	    Dataset<Row> 	jdbcRules1=sc.sql(S1);
	    
	    Row[] dataRows = (Row[]) jdbcRules1.collect();
	       
		 String t3="";
		 String t4="";
	    
		 System.out.println("Rule: Rules Summary for: " +myFile.getName());
		 
		 
    	   for (Row row : dataRows)
		  
		   {
			   
				   t3="Rule: Rule Type: "+row.getString(0)+"--> Number of Rules : "+row.getLong(1);
				   
				   System.out.println(t3);	   
		   }
	    
	    
	    	

    	   

   		String mt1="Select PrimaryColumn as CDE, Case when DataQualityDomain='Uniqueness' Then 1 else 0 end as Uniqueness, "
   				+ "Case when DataQualityDomain='Completeness' Then 1 else 0 end as Completeness, "
   				+ "Case when DataQualityDomain='Completeness-Advanced' Then 1 else 0 end as CompletnessAdv, "
   				+ "Case when DataQualityDomain='Conformity' Then 1 else 0 end as Conformity, "
   				+ "Case when DataQualityDomain='Consistency' Then 1 else 0 end as Consistency, "
   				+ "Case when DataQualityDomain='Currency' Then 1 else 0 end as Currency, "
   				+ "Case when DataQualityDomain='Drift' Then 1 else 0 end as Drift , "
   				+ "Case when DataQualityDomain='Accuracy' Then 1 else 0 end as Accuracy , "
   				+ "Case when DataQualityDomain='Reasonability' Then 1 else 0 end as Reasonability, "
                + "Case when DataQualityDomain='Validity' Then 1 else 0 end as Validity from M1 ";
   		
   		
   		Dataset<Row> jdbcDFM1=sc.sql(mt1);
//   		jdbcDFM1.show();
   		
   		jdbcDFM1.createOrReplaceTempView("T2");
   		String mt2="Select CDE, sum(uniqueness) , sum(Completeness), sum(CompletnessAdv) , sum(Conformity), sum(Consistency), sum(Currency),sum(Drift),sum(Accuracy),sum(Reasonability),sum(validity) from T2 group by CDE";
   		Dataset<Row> jdbcDFM2=sc.sql(mt2);
   		
   		Row[] dataRows2 = (Row[]) jdbcDFM2.collect();
	       
//   		jdbcDFM2.show(); 	    
		
		 
		 
   	   for (Row row : dataRows2)
		  
		   {
			   
			String	   mt3="Rule2,"+row.getString(0)+","+row.getLong(1)+","+row.getLong(2)+","+row.getLong(3)+","+row.getLong(4)+","+row.getLong(5)+","+row.getLong(6)+","+row.getLong(7)+","+row.getLong(8)+","+row.getLong(9)+","+row.getLong(10);
				   
				   System.out.println(mt3);	   
		   }
    	   
    	   
	    }
	    	
	    }
		
		
		
		if(results.equals("1"))
		{
			File file5 = new File(fdirresults);
			File[] files2 = file5.listFiles();
	
		    for (File myFile: files2) 
		    {
		    String fname=myFile.getAbsolutePath();
		    System.out.println(fname);
		    if(fname.contains("DQ_Results"))
		    {
		    Dataset<Row> 	jdbcRules = sc.read().format("csv").option("header", "true").load(fname);
//		    jdbcRules.show();
		    
		    if(jdbcRules.count()>1)
		    {
		    jdbcRules.createOrReplaceTempView("M1");
		    String st1="Failed";
		    String S1="select DataQualityDomain, Status, count(*) as countA from M1 group by DataQualityDomain, Status";
		    Dataset<Row> 	jdbcRules1=sc.sql(S1);
		   
		    
		    Row[] dataRows = (Row[]) jdbcRules1.collect();
		       
			 String t3="";
			 String t4="";
		    
			 System.out.println("Results: Results Summary for: " +myFile.getName() );
			 
			 
	    	   for (Row row : dataRows)
			  
			   {
				   
					   t3="Results: Result Type: "+row.getString(0)+"--> Status : "+row.getString(1)+" --> Number of Rules:"+row.getLong(2);
					   
					   System.out.println(t3);	   
			   }
	    	   
	    	   
	    	   
	    	  
	    	   
	    	   String mt1="Select PrimaryColumn as CDE, Case when DataQualityDomain='Uniqueness' Then rCount else 0 end as Uniqueness, "
	      				+ "Case when DataQualityDomain='Completeness' Then rCount else 0 end as Completeness, "
	      				+ "Case when DataQualityDomain='Completeness-Advanced' Then rCount else 0 end as CompletnessAdv, "
	      				+ "Case when DataQualityDomain='Conformity' Then rCount else 0 end as Conformity, "
	      				+ "Case when DataQualityDomain='Consistency' Then rCount else 0 end as Consistency, "
	      				+ "Case when DataQualityDomain='Currency' Then rCount else 0 end as Currency, "
	      				+ "Case when DataQualityDomain='Drift' Then rCount else 0 end as Drift , "
	      				+ "Case when DataQualityDomain='Accuracy' Then rCount else 0 end as Accuracy , "
	      				+ "Case when DataQualityDomain='Reasonability' Then rCount else 0 end as Reasonability, "
	                    + "Case when DataQualityDomain='Validity' Then rCount else 0 end as Validity from M1 ";
	    	   
	    	   
	    	   Dataset<Row> jdbcDFM1=sc.sql(mt1);
	    	   jdbcDFM1.show();
	      		jdbcDFM1.createOrReplaceTempView("T2");
	      		String mt2="Select CDE, sum(uniqueness) , sum(Completeness), sum(CompletnessAdv) , sum(Conformity), sum(Consistency), sum(Currency),sum(Drift),sum(Accuracy),sum(Reasonability),sum(validity) from T2 group by CDE";
	      		Dataset<Row> jdbcDFM2=sc.sql(mt2);
	      		
	      		Row[] dataRows2 = (Row[]) jdbcDFM2.collect();
	   	       
	   		 	    
	      		jdbcDFM2.show();
	   		 
	   		 
	      	   for (Row row : dataRows2)
	   		  
	   		   {
	   			   
	   			String	 mt3="Rule3,"+row.getString(0)+","+(int)row.getDouble(1)+","+(int)row.getDouble(2)+","+(int)row.getDouble(3)+","+(int)row.getDouble(4)+","+(int)row.getDouble(5)+","+(int)row.getDouble(6)+","+(int)row.getDouble(7)+","+(int)row.getDouble(8)+","+(int)row.getDouble(9)+","+(int)row.getDouble(10);
	   				   
	   				   System.out.println(mt3);	   
	   		   }
	       	   
	    	   
	    	   
		    }
		    
		    }
		    
		    
		    }
		}
	    
	    
	    
		System.out.println("Completed Processing - Your Results are now available");
		
		System.out.println(new Date());
		
		long endTime = System.currentTimeMillis();

		System.out.println("Completed Processing in " + df.format((endTime - startTime)/60000.0) + " minutes");
		
		}
		else
		{
			System.out.println("License Expired - Please contact support - Results will not be presented");
		}
		
	}
	catch(Exception e)
	{
		e.printStackTrace();
	}
	
	
	}
	
	
	public static String SSProfile(String col, Dataset<Row> jdbcDF, SparkSession sc)
	{
		String s="Not PHI/PII";
		
		String tbl2="pattern.csv";
		Dataset<Row> jdbcDF2 = sc.read().format("csv").option("header", "true").load(tbl2);
//		jdbcDF2.show();
		
		
Row[] dataRows = (Row[]) jdbcDF2.collect();
		
		List<String> group1 = new ArrayList<String>();
		List<String> group2 = new ArrayList<String>();
		List<String> group3 = new ArrayList<String>();
		List<String> group1n = new ArrayList<String>();
		List<String> group2n = new ArrayList<String>();
		List<String> group3n = new ArrayList<String>();
		
		for (Row row : dataRows) 
		
		{
			if(row.get(2).toString().equals("1"))
			{
				group1.add(row.get(1).toString());
				group1n.add(row.get(0).toString());
			}
			
			if(row.get(2).toString().equals("2"))
			{
				group2.add(row.get(1).toString());
				group2n.add(row.get(0).toString());
			}
			
			if(row.get(2).toString().equals("3"))
			{
				group3.add(row.get(1).toString());
				group3n.add(row.get(0).toString());
			}
			
			
		}
		
		String t1="";
		String t1a="";
		String t1s="";
		
		for (int i=0;i<group1.size();i++)
		{
//			System.out.println(group1.get(i).toString());
			
			t1= t1+"case when tmp rlike '"+ group1.get(i).toString()+"' then 1 else 0 end as "+ group1n.get(i)+" ,";
			t1a=t1a+group1n.get(i)+"+";
			t1s=t1s+"sum("+group1n.get(i)+") as "+group1n.get(i)+",";
		}
		if(t1.endsWith(","))
   		{
			t1 = t1.substring(0,t1.length() - 1); 
			t1s = t1s.substring(0,t1s.length() - 1);
   		}
		if(t1a.endsWith("+"))
   		{
			t1a = t1a.substring(0,t1a.length() - 1); 
   		}
		
		String t2="";
		String t2a="";
		String t2s="";
		for (int i=0;i<group2.size();i++)
		{
//			System.out.println(group2.get(i).toString());
			
			t2= t2+"case when tmp rlike '"+ group2.get(i).toString()+"' then 1 else 0 end as "+ group2n.get(i)+" ,";
			t2a=t2a+group2n.get(i)+"+";
			t2s=t2s+"sum("+group2n.get(i)+") as "+group2n.get(i)+",";
		}
		if(t2.endsWith(","))
   		{
			t2 = t2.substring(0,t2.length() - 1); 
			t2s = t2s.substring(0,t2s.length() - 1);
   		}
		if(t2a.endsWith("+"))
   		{
			t2a = t2a.substring(0,t2a.length() - 1); 
   		}
		String t3="";
		String t3a="";
		String t3s="";
		
		for (int i=0;i<group3.size();i++)
		{
//			System.out.println(group1.get(i).toString());
			
			t3= t3+"case when tmp rlike '"+ group3.get(i).toString()+"' then 1 else 0 end as "+ group3n.get(i)+" ,";
			t3a=t3a+group3n.get(i)+"+";
			t3s=t3s+"sum("+group3n.get(i)+") as "+group3n.get(i)+",";
		}
		if(t3.endsWith(","))
   		{
			t3 = t3.substring(0,t3.length() - 1); 
			t3s = t3s.substring(0,t3s.length() - 1);
   		}
		if(t3a.endsWith("+"))
   		{
			t3a = t3a.substring(0,t3a.length() - 1); 
   		}
		
		jdbcDF.createOrReplaceTempView("T1");
		
		
		String st1="Select "+col+" as tmp from T1";
		
		Dataset<Row> jdbcDF3 = sc.sql(st1);
		jdbcDF3.createOrReplaceTempView("T2");
		
		String st3="select "+t1s+","+t2s+","+t3s+" from T2";
		Dataset<Row> jdbcDF4 = sc.sql(st3);
		String col2[] =jdbcDF4.columns();
		int columnCount=col2.length;
		
		String ss="";
		if(jdbcDF4.count()>0)
		{
		
		Row[] dataRows1 = (Row[]) jdbcDF4.collect();
		
		
		for (Row row : dataRows1)
		{
		
	//		p4.println("Senstitive Data Pattern,Number_of_Records,Percentage");
		for(int i=0;i<columnCount;i++)
		{
		 
		if(row.getLong(i)>0)	
			ss=ss+col2[i]+",";
	
		}
	//	 System.out.println(s5);
		 //+df.format(100.0*row.getLong(0)/totalcount)
		}
		
		}
		
		if(ss.endsWith(","))
   		{
			ss = ss.substring(0,ss.length() - 1); 
			
   		}
		
		if(ss.length()!=0)
		{
			s=ss;
		}
		
		
		
		
		return s;
	}
	
	public static void SProfile(List<String> col, Dataset<Row> jdbcDF, SparkSession sc, String schema, String table)
	{
		String tfs=System.getProperty("file.separator");
		DecimalFormat df = new DecimalFormat("#.##");
		String directory="";
		try {
			directory = new File(".").getCanonicalPath();
		
		String fdir=directory+tfs+"senstive1";
		File file = new File(fdir);
//		System.out.println(fdir);
		boolean isDirectoryCreated = file.mkdir();

		if (isDirectoryCreated) {
		      

		} else {
		       deleteDir(file);  // Invoke recursive method
		       file.mkdir();   
		     
		}
		
		String tbl2="pattern.csv";
		Dataset<Row> jdbcDF2 = sc.read().format("csv").option("header", "true").load(tbl2);
//		jdbcDF2.show();
		
		Row[] dataRows = (Row[]) jdbcDF2.collect();
		
		List<String> group1 = new ArrayList<String>();
		List<String> group2 = new ArrayList<String>();
		List<String> group3 = new ArrayList<String>();
		List<String> group1n = new ArrayList<String>();
		List<String> group2n = new ArrayList<String>();
		List<String> group3n = new ArrayList<String>();
		
		for (Row row : dataRows) 
		
		{
			if(row.get(2).toString().equals("1"))
			{
				group1.add(row.get(1).toString());
				group1n.add(row.get(0).toString());
			}
			
			if(row.get(2).toString().equals("2"))
			{
				group2.add(row.get(1).toString());
				group2n.add(row.get(0).toString());
			}
			
			if(row.get(2).toString().equals("3"))
			{
				group3.add(row.get(1).toString());
				group3n.add(row.get(0).toString());
			}
			
			
		}
		
		String t1="";
		String t1a="";
		String t1s="";
		
		for (int i=0;i<group1.size();i++)
		{
//			System.out.println(group1.get(i).toString());
			
			t1= t1+"case when tmp rlike '"+ group1.get(i).toString()+"' then 1 else 0 end as "+ group1n.get(i)+" ,";
			t1a=t1a+group1n.get(i)+"+";
			t1s=t1s+"sum("+group1n.get(i)+") as "+group1n.get(i)+",";
		}
		if(t1.endsWith(","))
   		{
			t1 = t1.substring(0,t1.length() - 1); 
			t1s = t1s.substring(0,t1s.length() - 1);
   		}
		if(t1a.endsWith("+"))
   		{
			t1a = t1a.substring(0,t1a.length() - 1); 
   		}
		
		String t2="";
		String t2a="";
		String t2s="";
		for (int i=0;i<group2.size();i++)
		{
//			System.out.println(group2.get(i).toString());
			
			t2= t2+"case when tmp rlike '"+ group2.get(i).toString()+"' then 1 else 0 end as "+ group2n.get(i)+" ,";
			t2a=t2a+group2n.get(i)+"+";
			t2s=t2s+"sum("+group2n.get(i)+") as "+group2n.get(i)+",";
		}
		if(t2.endsWith(","))
   		{
			t2 = t2.substring(0,t2.length() - 1); 
			t2s = t2s.substring(0,t2s.length() - 1);
   		}
		if(t2a.endsWith("+"))
   		{
			t2a = t2a.substring(0,t2a.length() - 1); 
   		}
		String t3="";
		String t3a="";
		String t3s="";
		for (int i=0;i<group3.size();i++)
		{
//			System.out.println(group1.get(i).toString());
			
			t3= t3+"case when tmp rlike '"+ group3.get(i).toString()+"' then 1 else 0 end as "+ group3n.get(i)+" ,";
			t3a=t3a+group3n.get(i)+"+";
			t3s=t3s+"sum("+group3n.get(i)+") as "+group3n.get(i)+",";
		}
		if(t3.endsWith(","))
   		{
			t3 = t3.substring(0,t3.length() - 1); 
			t3s = t3s.substring(0,t3s.length() - 1);
   		}
		if(t3a.endsWith("+"))
   		{
			t3a = t3a.substring(0,t3a.length() - 1); 
   		}
		
		
		String f3=schema+"_"+table+"_Sensitive_Profile_Column.csv";
		f3=directory+tfs+"senstive1"+tfs+f3;
	    FileWriter fw3 = new FileWriter(f3);
	    
		PrintWriter p3 = new PrintWriter(fw3);
		
		String f4=schema+"_"+table+"_Sensitive_Profile_Row.csv";
		f4=directory+tfs+"senstive1"+tfs+f4;
	    FileWriter fw4 = new FileWriter(f4);
	    
		PrintWriter p4 = new PrintWriter(fw4);
		
		
		int totalcount=(int) jdbcDF.count();
		jdbcDF.createOrReplaceTempView("PT1");
		
		p3.println("Group1 Count, Group2 Count, Group3 Count, Number_of_Records,Percentage");
		p4.println("Senstitive Data Pattern,Number_of_Records,Percentage");
		
		jdbcDF.createOrReplaceTempView("T1");
		
		String tmpc="";
		

		for (int i = 0; i < col.size(); i++ ) {
			
			  tmpc=tmpc+col.get(i)+",";
	
			}
		
		if(tmpc.endsWith(","))
   		{
			
			tmpc = tmpc.substring(0,tmpc.length() - 1); 
   		}
		
		String tmpcol1="concat_ws('"+" ',"+tmpc+") as tmp";
		
		
		
		String tms1="Select "+ tmpcol1 + " from T1";
		Dataset<Row> jdbcDF3 = sc.sql(tms1);
		
		jdbcDF3.createOrReplaceTempView("T2");
		String st="Select "+t1+","+t2+","+t3+" from T2";
//		System.out.println(st);
		Dataset<Row> jdbcDF4 = sc.sql(st);
		
//		jdbcDF4.show();
		jdbcDF4.createOrReplaceTempView("T3");
		String st1="select *, "+t1a+" as group1 ,"+t2a+" as group2, "+t3a+ " as group3 from T3";
//		System.out.println(st1);
		Dataset<Row> jdbcDF5 = sc.sql(st1);
		
//		jdbcDF5.show();
		
		jdbcDF5.createOrReplaceTempView("T4");
		
		String st3="select "+t1s+","+t2s+","+t3s+" from T4";
		Dataset<Row> jdbcDF6 = sc.sql(st3);
		
//		jdbcDF6.show();
		
		String col2[] =jdbcDF6.columns();
		int columnCount=col2.length;
		
		if(jdbcDF6.count()>0)
		{
		
		Row[] dataRows1 = (Row[]) jdbcDF6.collect();
		
		
		for (Row row : dataRows1)
		{
		
	//		p4.println("Senstitive Data Pattern,Number_of_Records,Percentage");
		for(int i=0;i<columnCount;i++)
		{
		 String s5=col2[i]+","+row.getLong(i)+","+df.format(100.0*row.getLong(i)/totalcount);
		 p4.println(s5);
		 
		 String	 mt3="Srule,"+s5;
			   
//		 System.out.println(mt3);	
		}
	//	 System.out.println(s5);
		 //+df.format(100.0*row.getLong(0)/totalcount)
		}
		
		}
		
		
		
		String st4="select group1,group2, group3, count(*) from T4 group by group1,group2,group3 ";
		Dataset<Row> jdbcDF7 = sc.sql(st4);
		
//		jdbcDF7.show();
		
	
		
		if(jdbcDF7.count()>0)
		{
		
		Row[] dataRows1 = (Row[]) jdbcDF7.collect();
		
		
		for (Row row : dataRows1)
		{
		
	//		p4.println("Senstitive Data Pattern,Number_of_Records,Percentage");
		
		 String s6=row.get(0).toString()+","+row.get(1).toString()+","+row.get(2).toString()+","+row.getLong(3)+","+df.format(100.0*row.getLong(3)/totalcount);
		 p3.println(s6);
		 String	 mt3="Srule1,"+s6;
		   
	//	 System.out.println(mt3);
		}
	//	 System.out.println(s5);
		 //+df.format(100.0*row.getLong(0)/totalcount)
		}
		
		
		
		p4.close();
		p3.close();
		
	
		} catch (Exception e) 
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
	
	
	public static void ProfileRules3(List<String> col, Dataset<Row> jdbcDF, SparkSession sc, String schema, String table)
	{
		
		String tfs=System.getProperty("file.separator");
		
		DecimalFormat df = new DecimalFormat("#.##");
		String directory="";
		try {
			directory = new File(".").getCanonicalPath();
			String f3=schema+"_"+table+"_Numerical_Profile_Detail.csv";
			f3=directory+tfs+"profile1"+tfs+f3;
		    FileWriter fw3 = new FileWriter(f3);
		    
			PrintWriter p3 = new PrintWriter(fw3);
			p3.println("ColumnName,ColumnName,Correlation(>0.25)");
			
			
			jdbcDF.createOrReplaceTempView("PT1");
			int n=col.size();
			int[] arr = new int[n];
			
			for(int i=0;i<n;i++)
			{
				arr[i]=i+1;
				
			}
			
			 
			 String tmp2=  printCombination(arr, n, 2);
	 //    	 System.out.println(tmp2);

	    	 String test[]= tmp2.split(",");
	 		List<String> invoiceCalculationsResult = new ArrayList<String>();
			ExecutorService executor = Executors.newFixedThreadPool(numthread);
			List<java.util.concurrent.Future<String>> futuresList = new ArrayList<>();
	    	 
	    	 for (int i=0;i<test.length;i++)
			 {
				 if(! test[i].isEmpty())
				 {
					 String[]  tmp3=test[i].split("-");
					int x=Integer.parseInt(tmp3[1]) -1;
					int y=Integer.parseInt(tmp3[2]) -1;
					String st=col.get(x).toString();
					String st1=col.get(y).toString();
					Callable<String> task = new profileRules3(st, st1, jdbcDF,   sc,  p3);
					java.util.concurrent.Future<String> future =  executor.submit(task);
					futuresList.add(future);

	//				System.out.println("Running result for: "+st1);
					
					
					
					
					 
				 }
			 }
	    	 
	    	 for (java.util.concurrent.Future<String> future : futuresList) {
	 			String calculationResult = null;
	 			try {
	 				calculationResult = future.get();
	 			} catch (InterruptedException | ExecutionException e) {
	 				// ... Exception handling code ...
	 	        } 
	 		//	invoiceCalculationsResult.add(calculationResult);
	 	    }
	     
	 		executor.shutdown();
	    	 
	    	 
					 
	    	 p3.close();

		
		}
		catch(Exception e)
		{
			e.printStackTrace();
			
			
		}
		
		
		
		
	}
	
	public static void ProfileRules2(List<String> Microsegment, List<String> Microsegment2, Dataset<Row> jdbcDF, SparkSession sc, String schema, String table)
	{
		String tfs=System.getProperty("file.separator");
//		System.out.println(Microsegment.size());
//		System.out.println(Microsegment2.size());
		
		DecimalFormat df = new DecimalFormat("#.##");
		String directory="";
		try {
			directory = new File(".").getCanonicalPath();
		
		
		
		String f3=schema+"_"+table+"_Column_Profile_Detail.csv";
		f3=directory+tfs+"profile1"+tfs+f3;
	    FileWriter fw3 = new FileWriter(f3);
	    
		PrintWriter p3 = new PrintWriter(fw3);
		
		p3.println("ColumnName,ColumnValue,Count,Percentage");
		
		int totalcount=(int) jdbcDF.count();
		jdbcDF.createOrReplaceTempView("PT1");
		String st2="";
		List<String> invoiceCalculationsResult = new ArrayList<String>();
		ExecutorService executor = Executors.newFixedThreadPool(numthread);
		List<java.util.concurrent.Future<String>> futuresList = new ArrayList<>();
		
		for(int i=0;i<Microsegment.size();i++)
		{
			
			

			String tmpcol=Microsegment.get(i);
			st2=st2+tmpcol+",";
		
				
				
				Callable<String> task = new profileRules2(Microsegment.get(i).toString(), totalcount,  jdbcDF,   sc,  p3); 
				java.util.concurrent.Future<String> future =  executor.submit(task);
				futuresList.add(future);

	//			System.out.println("Running result for: "+Microsegment.get(i).toString());
				
				
			
			
			
			
		}
		
		
		for (java.util.concurrent.Future<String> future : futuresList) {
			String calculationResult = null;
			try {
				calculationResult = future.get();
			} catch (InterruptedException | ExecutionException e) {
				// ... Exception handling code ...
	        } 
		//	invoiceCalculationsResult.add(calculationResult);
	    }
    
		executor.shutdown();
		
		p3.close();
		
		String f4=schema+"_"+table+"_Column_Combination_Detail.csv";
		f4=directory+tfs+"profile1"+tfs+f4;
	    FileWriter fw4 = new FileWriter(f4);
	    
		PrintWriter p4 = new PrintWriter(fw4);
		
		
		
		
		
		if(st2.endsWith(","))
	   	   {
	   		  st2 = st2.substring(0,st2.length() - 1); 
	   	   }
		
		String st4=st2.replaceAll("`", "")+"Count,Percentage";
		p4.println(st4);
		
		String st3="Select "+st2+", count(*) as CountA from PT1 group by "+st2+" order by countA desc limit 20";
		Dataset<Row> jdbcDF3=sc.sql(st3);
//		jdbcDF3.show();
		String col[] =jdbcDF3.columns();
		int columnCount=col.length;
		
		if(jdbcDF3.count()>0)
		{
			Row[] dataRows1 = (Row[]) jdbcDF3.collect();
			
			
			
			for (Row row : dataRows1)
			{
			
			 String s5="";
			 
			 for(int j=0;j<columnCount-2;j++)
			 {
				 s5=s5+row.get(j).toString()+",";
			 }
			 
			 s5=s5+row.getLong(columnCount-1)+","+df.format(100.0*row.getLong(columnCount-1)/totalcount);
			 
		//	 String s5=tmpcol.replaceAll("`", "")+","+row.get(0).toString()+","+row.getLong(1)+","+df.format(100.0*row.getLong(1)/totalcount);
			 p4.println(s5);
		//	 System.out.println(s5);
			 //+df.format(100.0*row.getLong(0)/totalcount)
			}
			
		}
		
		
		
		
		p4.close();
		
		}
		catch(Exception e)
		{
			e.printStackTrace();
			
			
		}
		
		
		
		
	}
	
	public static void LengthRules(List<String> col, List<String> coldef,List<String> datecols,List<String> datecolsformat, Dataset<Row> jdbcDF, SparkSession sc, String schema, String table)
	{
		String tfs=System.getProperty("file.separator");
		DecimalFormat df = new DecimalFormat("#.##");
		String directory="";
		try {
			directory = new File(".").getCanonicalPath();
		
		String fdir=directory+tfs+"profile1";
		File file = new File(fdir);
//		System.out.println(fdir);
		boolean isDirectoryCreated = file.mkdir();

		if (isDirectoryCreated) {
		      

		} else {
	//	       deleteDir(file);  // Invoke recursive method
	//	       file.mkdir();   
		     
		}
		
		String f3=schema+"_"+table+"_Column_Profile.csv";
		f3=directory+tfs+"profile1"+tfs+f3;
	    FileWriter fw3 = new FileWriter(f3);
	    
		PrintWriter p3 = new PrintWriter(fw3);
		int totalcount=(int) jdbcDF.count();
		jdbcDF.createOrReplaceTempView("PT1");
		p3.println("ColumnName,DataType,Sensitive,TotalRecordCount,MissingValue,PercentageMissing,UniqueCount,MinLength,MaxLength,Mean,StdDev,Min,Max,99percentaile,75percentile,25percentile,1percentile");
		p3.flush();
//		jdbcDF.show();
//		System.out.println("In profile");
		
		List<String> invoiceCalculationsResult = new ArrayList<String>();
		ExecutorService executor = Executors.newFixedThreadPool(numthread);
		List<java.util.concurrent.Future<String>> futuresList = new ArrayList<>();
		
		
		for(int i=0;i<col.size();i++)
		{
			
			
			 
			Callable<String> task = new profileRules1(col.get(i), coldef.get(i), totalcount, jdbcDF,  datecols, datecolsformat,  sc,  p3, fe);
			java.util.concurrent.Future<String> future =  executor.submit(task);
			futuresList.add(future);

	//		System.out.println("Running result for: "+col.get(i));
			
			
		}  //forloop
		
		for (java.util.concurrent.Future<String> future : futuresList) {
			String calculationResult = null;
			try {
				calculationResult = future.get();
			} catch (InterruptedException | ExecutionException e) {
				e.printStackTrace();
	        } 
		//	invoiceCalculationsResult.add(calculationResult);
	    }
    
		executor.shutdown();
		
		

		p3.close();
		
		
		String s1="";
		String s3="";
//		directory = new File(".").getCanonicalPath();
		String f4=schema+"_"+table+"_Row_Profile.csv";
		f4=directory+tfs+"profile1"+tfs+f4;
	    FileWriter fw4 = new FileWriter(f4);
	    
		PrintWriter p4 = new PrintWriter(fw4);
		p4.println("Number_of_Columns_with_NULL,Number_of_Records,PercentageMissing");
		
		
		for(int i=0;i<col.size();i++)
		{
			s1=s1+ " Case when "+ col.get(i)+ " is null then 1  when trim("+col.get(i)+")='' then 1 else 0 end as "+col.get(i)+",";
			s3=s3+col.get(i)+"+";
		}
		
		
		 if(s1.endsWith(","))
	   	   {
	   		  s1 = s1.substring(0,s1.length() - 1); 
	   	   }
		
//		 System.out.println(s3);
		 if(s3.endsWith("+"))
			
	   	   {
	   		  s3 = s3.substring(0,s3.length() - 1); 
	   	   }
	
		 
		 
		String s2="select "+s1+" from PT1";
		Dataset<Row> jdbcDF4=sc.sql(s2);
		
//		jdbcDF4.show();
		jdbcDF4.createOrReplaceTempView("PT2");
		
		String s4="select "+s3+" as totcount from PT2";
		
		Dataset<Row> jdbcDF5=sc.sql(s4);
		
//		jdbcDF5.show();
		jdbcDF5.createOrReplaceTempView("PT3");
		
		String s6="select totcount, count(*) as countA from PT3 group by totcount order by totcount asc";
		
		Dataset<Row> jdbcDF6=sc.sql(s6);
		
//		jdbcDF6.show();
		
		if(jdbcDF6.count()>0)
		{
			Row[] dataRows1 = (Row[]) jdbcDF6.collect();
			
			
			for (Row row : dataRows1)
			{
			
			 
			String s8=row.get(0).toString()+","+row.getLong(1)+","+df.format(100.0*row.getLong(1)/totalcount);
				
 			 p4.println(s8);
 			 p4.flush();
	//		 System.out.println(s8);
			}
			
		}
		
		p4.close();
		
	
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
	
	
	public static void ProfileRules1(List<String> col, List<String> coldef,List<String> datecols,List<String> datecolsformat, Dataset<Row> jdbcDF, SparkSession sc, String schema, String table)
	{
		String tfs=System.getProperty("file.separator");
		DecimalFormat df = new DecimalFormat("#.##");
		String directory="";
		try {
			directory = new File(".").getCanonicalPath();
		
		String fdir=directory+tfs+"profile1";
		File file = new File(fdir);
//		System.out.println(fdir);
		boolean isDirectoryCreated = file.mkdir();

		if (isDirectoryCreated) {
		      

		} else {
	//	       deleteDir(file);  // Invoke recursive method
	//	       file.mkdir();   
		     
		}
		
		String f3=schema+"_"+table+"_Column_Profile.csv";
		f3=directory+tfs+"profile1"+tfs+f3;
	    FileWriter fw3 = new FileWriter(f3);
	    
		PrintWriter p3 = new PrintWriter(fw3);
		int totalcount=(int) jdbcDF.count();
		jdbcDF.createOrReplaceTempView("PT1");
		p3.println("ColumnName,DataType,Sensitive,TotalRecordCount,MissingValue,PercentageMissing,UniqueCount,MinLength,MaxLength,Mean,StdDev,Min,Max,99percentaile,75percentile,25percentile,1percentile");
		p3.flush();
//		jdbcDF.show();
//		System.out.println("In profile");
		
		List<String> invoiceCalculationsResult = new ArrayList<String>();
		ExecutorService executor = Executors.newFixedThreadPool(numthread);
		List<java.util.concurrent.Future<String>> futuresList = new ArrayList<>();
		
		
		for(int i=0;i<col.size();i++)
		{
			
			
			 
			Callable<String> task = new profileRules1(col.get(i), coldef.get(i), totalcount, jdbcDF,  datecols, datecolsformat,  sc,  p3, fe);
			java.util.concurrent.Future<String> future =  executor.submit(task);
			futuresList.add(future);

//			System.out.println("Running result for: "+col.get(i));
			
			
		}  //forloop
		
		for (java.util.concurrent.Future<String> future : futuresList) {
			String calculationResult = null;
			try {
				calculationResult = future.get();
			} catch (InterruptedException | ExecutionException e) {
				e.printStackTrace();
	        } 
		//	invoiceCalculationsResult.add(calculationResult);
	    }
    
		executor.shutdown();
		
		

		p3.close();
		
		
		String s1="";
		String s3="";
//		directory = new File(".").getCanonicalPath();
		String f4=schema+"_"+table+"_Row_Profile.csv";
		f4=directory+tfs+"profile1"+tfs+f4;
	    FileWriter fw4 = new FileWriter(f4);
	    
		PrintWriter p4 = new PrintWriter(fw4);
		p4.println("Number_of_Columns_with_NULL,Number_of_Records,PercentageMissing");
		
		
		for(int i=0;i<col.size();i++)
		{
			s1=s1+ " Case when "+ col.get(i)+ " is null then 1  when trim("+col.get(i)+")='' then 1 else 0 end as "+col.get(i)+",";
			s3=s3+col.get(i)+"+";
		}
		
		
		 if(s1.endsWith(","))
	   	   {
	   		  s1 = s1.substring(0,s1.length() - 1); 
	   	   }
		
//		 System.out.println(s3);
		 if(s3.endsWith("+"))
			
	   	   {
	   		  s3 = s3.substring(0,s3.length() - 1); 
	   	   }
	
		 
		 
		String s2="select "+s1+" from PT1";
		Dataset<Row> jdbcDF4=sc.sql(s2);
		
//		jdbcDF4.show();
		jdbcDF4.createOrReplaceTempView("PT2");
		
		String s4="select "+s3+" as totcount from PT2";
		
		Dataset<Row> jdbcDF5=sc.sql(s4);
		
//		jdbcDF5.show();
		jdbcDF5.createOrReplaceTempView("PT3");
		
		String s6="select totcount, count(*) as countA from PT3 group by totcount order by totcount asc";
		
		Dataset<Row> jdbcDF6=sc.sql(s6);
		
//		jdbcDF6.show();
		
		if(jdbcDF6.count()>0)
		{
			Row[] dataRows1 = (Row[]) jdbcDF6.collect();
			
			
			for (Row row : dataRows1)
			{
			
			 
			String s8=row.get(0).toString()+","+row.getLong(1)+","+df.format(100.0*row.getLong(1)/totalcount);
				
 			 p4.println(s8);
 			 p4.flush();
//			 System.out.println(s8);
			}
			
		}
		
		p4.close();
		
	
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
	public static void NullRulesV1(List<String> NullColTarget, Dataset<Row> jdbcDF, Dataset<Row> jdbcDFS, SparkSession sc,PrintWriter p2, PrintWriter p3 )
	{
		
		
//		System.out.println("Locating Conditional Completeness v2");
		
				
		/*
		for(int i=0;i<NullColTarget.size();i++)
		{
			
			if(!Microsegment5.contains(NullColTarget.get(i).toString()))
			{
				Microsegment5.add(NullColTarget.get(i).toString());
			}
			
			
			
		}*/
		List<String> invoiceCalculationsResult = new ArrayList<String>();
		ExecutorService executor = Executors.newFixedThreadPool(numthread);
		List<java.util.concurrent.Future<String>> futuresList = new ArrayList<>();
		
		for(int i=0;i<NullColTarget.size();i++)
		{
			String st=NullColTarget.get(i).toString();
			Callable<String> task = new nullRules2( st,  NullColTarget,  jdbcDF,  jdbcDFS,  sc, p2,  p3) ;
			java.util.concurrent.Future<String> future =  executor.submit(task);
			futuresList.add(future);

//			System.out.println("Conditional Completeness v2: "+st);
			
			
			
			
			
		
		} 
		
		for (java.util.concurrent.Future<String> future : futuresList) {
			String calculationResult = null;
			try {
				calculationResult = future.get();
			} catch (InterruptedException | ExecutionException e) {
				// ... Exception handling code ...
	        } 
		//	invoiceCalculationsResult.add(calculationResult);
	    }
    
		executor.shutdown();
		
	
		
		
		
		
		
	}
	
	public static void NullRules(List<String> Microsegment, List<String> NullColTargetS, List<String>  NullColTarget2D, List<String> NullColTarget2DType, List<String> NullColTarget2DFormat, Dataset<Row> jdbcDF, Dataset<Row> jdbcDFS, SparkSession sc,PrintWriter p2, PrintWriter p3, String results )
	{
		
		
		String ad="";
		String ad1="";
		String ad2="";
		List<String> colist = new ArrayList<String>();
		int m=0;
		
		
		
		int nn=0;
		int i=0;
		int j=0;
		int k=0;
		
		
		
		
		while(nn <3)
		{
			
			if(i < Microsegment.size())
			{
				
				if(!colist.contains(Microsegment.get(i)))
				{
				String st=Microsegment.get(i).toString();
				
				ad=ad+ "Case When trim(ifnull("+st+",''))='' then 'BLANK'   else "+st+" end as "+st+","; 
				String ss=st+":";
				ad1=ad1+"concat('"+ss+"',"+st+"),";
				
				colist.add(st);
				i=i+1;
				
				if(i==Microsegment.size())
				{
					nn=nn+1;
				}
				
				}
				
				
			}
			
			if (j<NullColTargetS.size())
			{
				
				if(!colist.contains(NullColTargetS.get(j)))
				{
					
			String st=NullColTargetS.get(j).toString();
			ad=ad+ "Case When trim(ifnull("+st+",''))='' then 'BLANK'   else "+st+" end as "+st+","; 
			String ss=st+":";
			
			ad1=ad1+"concat('"+ss+"',"+st+"),";	
			j=j+1;
			colist.add(st);
			if(j==NullColTargetS.size())
			{
				nn=nn+1;
			}
			
				}
				
				
				
			}
			
			if(k<NullColTarget2D.size())
			{
				
				if(!colist.contains(NullColTarget2D.get(k)))
				{
					
			String st=NullColTarget2D.get(k).toString();
			String stype=NullColTarget2DType.get(k).toString();
			String sformat=NullColTarget2DFormat.get(k).toString();
			
			if(stype.matches("S"))
			{
			ad=ad+"Case When trim(ifnull("+st+",''))='' then 'BLANK' else 'Present' end as "+st+",";
			}
			
			if(stype.matches("I||F"))
			{
			
			ad=ad+"Case When trim(ifnull("+st+",''))='' then 'BLANK'  when cast("+st+" as double) >0 then 'Positive' when cast("+st+" as double) ==0 then 'Zero' else 'Negative' end as "+st+",";	
				
			}
			if(stype.matches("D"))
			{
				String t1="datediff(TO_DATE(CAST(UNIX_TIMESTAMP(";
				String t2="') AS TIMESTAMP))  , current_date())  ";
				String t3="";
				t3=t1+st+", '"+sformat+t2;
				
				String s1=ad=ad+"Case When trim(ifnull("+st+",''))='' then 'BLANK'  when "+t3+" >0 then 'Future' when "+t3+"  ==0 then 'Current' else 'Past' end as "+st+",";	
				
				
			}
			
			String ss=st+":";
			
			ad1=ad1+"concat('"+ss+"',"+st+"),";	
			k=k+1;
			colist.add(st);
			
			if(k==NullColTarget2D.size())
			{
				nn=nn+1;
			}
			
			}
			
		}
		
		}
		
		
		
		
		
		
		if(ad.endsWith(","))
    	{
    	  ad = ad.substring(0,ad.length() - 1);
    	  ad1=ad1.substring(0,ad1.length() - 1);
    	  
    	}
		
		String tmpview1="T1";
		jdbcDF.createOrReplaceTempView(tmpview1);
		
		System.out.println(ad);
		
		String t1="Select "+ad+" from "+tmpview1;
		
		Dataset<Row> jdbcDF1=sc.sql(t1);
		jdbcDF1.show();
		
		jdbcDF1.createOrReplaceTempView("T2");
		

		
	    int xx=0;
	    int yy=0;
	    int cols=colist.size();
	    
	    List<String> invoiceCalculationsResult = new ArrayList<String>();
		ExecutorService executor = Executors.newFixedThreadPool(numthread);
		List<java.util.concurrent.Future<String>> futuresList = new ArrayList<>();
		
	    
	    
	    while(xx<cols)
	    {
	    	String ad4="";
	    	yy=0;
	    	System.out.println("i "+xx);
	    	
	    	String ad5="";
	    	while(yy<5 )
	    	{
	    		String sst=colist.get(xx);
	    		String sss=sst+":";
	    		ad4=ad4+"concat('"+sss+"',"+sst+"),";
	    		xx=xx+1;
	    		if(xx==colist.size())
	    		{
	    			yy=5;
	    		}
	    		
	    		yy=yy+1;
	    		ad5=ad5+sst+",";
	    		
	    		
	    	}
	    	
	    	
	    	if(xx==colist.size())
    		{
    			yy=10;
    		}
	    	
	    	if(ad4.endsWith(","))
	    	{
	    	  
	    	  ad4=ad4.substring(0,ad4.length() - 1);
	    	  
	    	}
	    	String t3="Select array("+ad4+") as items from T2";
    		Dataset<Row> jdbcDF2=sc.sql(t3);
    		jdbcDF2.cache();
	    	
    		Callable<String> task = new findAssociation( jdbcDF2,   sc, p2,  p3, colist, results, threshold) ;
		java.util.concurrent.Future<String> future =  executor.submit(task);
			futuresList.add(future);

			
			
		}
		
	
	for (java.util.concurrent.Future<String> future : futuresList) {
		String calculationResult = null;
		try {
			calculationResult = future.get();
		} catch (InterruptedException | ExecutionException e) {
			// ... Exception handling code ...
			e.printStackTrace();
        } 
		
    }
	executor.shutdown();
		
/*
		ExecutorService executor = Executors.newFixedThreadPool(1);
		Callable<Dataset<Row>> task = new findAssociation( jdbcDF2,   sc, p2,  p3, colist, results, threshold) ;
		java.util.concurrent.Future<Dataset<Row>> future =  executor.submit(task);
		try
		{
		future.get();
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
		executor.shutdown();
	*/	
		
		/*
		
		ExecutorService executor1 = Executors.newFixedThreadPool(1);
		Callable<String> task1 = new findPattern2( jdbcDF2, jdbcDFS, sc, p2,  p3,  threshold) ;
		java.util.concurrent.Future<String> future1 =  executor1.submit(task1);
		try
		{
		future1.get();
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
		executor1.shutdown();
		
		*/
		
		
		
		//		
				
				
		//		java.util.concurrent.Future<Integer> future1 =  executor.submit(task2);
			
		/*		
		 Integer r1 = future.get();
				
				
		List<String> invoiceCalculationsResult = new ArrayList<String>();
		ExecutorService executor = Executors.newFixedThreadPool(numthread);
		List<java.util.concurrent.Future<Dataset<Row>>> futuresList = new ArrayList<>();
		String results="1";
		
		
		java.util.concurrent.Future<String> future =  executor.submit(task);
		futuresList.add(future);
		
		for (java.util.concurrent.Future<String> future1 : futuresList) {
			String calculationResult = null;
			try {
				jdbcDF2 = future1.get();
			} catch (InterruptedException | ExecutionException e) {
				e.printStackTrace();
	        } 
		//	invoiceCalculationsResult.add(calculationResult);
	    }
    
		
		
		
	//		System.out.println("Locating Conditional Completeness ");
		
				
	*/

		
		
		
		
		
	}
	
	
	
	
	public static void NullRulesV2(List<String> Microsegment5, List<String> NullColTarget, List<String> NullColTargetType,List<String> NullColTargetFormat, Dataset<Row> jdbcDF, Dataset<Row> jdbcDFS, SparkSession sc,PrintWriter p2, PrintWriter p3)
	{
		
		
//		System.out.println("Locating Conditional Completeness ");
		
				
	
		List<String> invoiceCalculationsResult = new ArrayList<String>();
		ExecutorService executor = Executors.newFixedThreadPool(numthread);
		List<java.util.concurrent.Future<String>> futuresList = new ArrayList<>();
		
		for(int i=0;i<Microsegment5.size();i++)
		{
			String st=Microsegment5.get(i).toString();
			
			for(int j=0;j<NullColTarget.size();j++)
			{
				String st1=NullColTarget.get(j).toString();
				String st2=NullColTargetType.get(j).toString();
				String st3="None";
				if(NullColTargetType.get(j).toString() != null)
				{
					st3=NullColTargetType.get(j).toString();
				}
			
				if(!st1.equals(st))
				{
					
			Callable<String> task = new nullRules4( st, st1, st2, st3, jdbcDF,  jdbcDFS,  sc, p2,  p3, threshold) ;
			java.util.concurrent.Future<String> future =  executor.submit(task);
			futuresList.add(future);

			System.out.println("Conditional Completeness: "+st);
				}
			}
			
			
			
			
		
		} 
		
		for (java.util.concurrent.Future<String> future : futuresList) {
			String calculationResult = null;
			try {
				calculationResult = future.get();
			} catch (InterruptedException | ExecutionException e) {
				e.printStackTrace();
	        } 
		//	invoiceCalculationsResult.add(calculationResult);
	    }
    
		executor.shutdown();
		
	
		
		
		
		
		
	}
	
	public static void ComboRules(List<String> Microsegment5,  Dataset<Row> jdbcDF, Dataset<Row> jdbcDFS, SparkSession sc,PrintWriter p2, PrintWriter p3 )
	{
		
		
		int n=Microsegment5.size();
		
		if(n>1)
		{
	
			int[] arr = new int[n];
			for(int i=0; i<n;i++)
			{
				arr[i]=i+1;
				
			}
			
			 int m=2;
			
			 String tmp2=  printCombination(arr, n, m);
			 String test[]= tmp2.split(",");
	    	 
			 
			 List<String> invoiceCalculationsResult = new ArrayList<String>();
				ExecutorService executor = Executors.newFixedThreadPool(numthread);
				List<java.util.concurrent.Future<String>> futuresList = new ArrayList<>();
			 
			 
				for(int i=0;i<test.length;i++)
				{
					if(test[i].length()>3)
					{
					Callable<String> task = new ComboRules1( test[i], Microsegment5, jdbcDF,  jdbcDFS,  sc, p2,  p3, threshold) ;
					java.util.concurrent.Future<String> future =  executor.submit(task);
					futuresList.add(future);
					}
					
				}
				
				for (java.util.concurrent.Future<String> future : futuresList) {
					String calculationResult = null;
					try {
						calculationResult = future.get();
					} catch (InterruptedException | ExecutionException e) 
					{
					    e.printStackTrace();	
					} 
				//	invoiceCalculationsResult.add(calculationResult);
			    }
	    	 
				executor.shutdown();

				
		}
    	
   
	}
	
	
	public static void eualityRules(List<String> col, List<String> coldef,List<String> Microsegment, Dataset<Row> jdbcDF, SparkSession sc,PrintWriter p2)
	{
		
		List<String> scols = new ArrayList<String>();
		List<String> ncols = new ArrayList<String>();
		List<String> dcols = new ArrayList<String>();
		
		
		
		for(int i=0;i<col.size();i++)
		{
			if(coldef.get(i).equals("S"))
			{
				scols.add(col.get(i));
				System.out.println(col.get(i));
			}
			
			if(coldef.get(i).equals("I"))
			{
				ncols.add(col.get(i));
			}
			if(coldef.get(i).equals("F"))
			{
				ncols.add(col.get(i));
			}
			if(coldef.get(i).equals("D"))
			{
				dcols.add(col.get(i));
			}
			
			

				
			
		}
		
		
		seualityRules1(scols,Microsegment,jdbcDF,sc, p2);
		neualityRules1(ncols,jdbcDF,sc, p2);
		deualityRules1(dcols,jdbcDF,sc, p2);
		
		
		

	}
	
	
	
	public static void seualityRules1(List<String> col,List<String> Microsegment, Dataset<Row> jdbcDF, SparkSession sc,PrintWriter p2)
	{
		
		jdbcDF.createOrReplaceTempView("CM1");
//		jdbcDF.show();
		int totalcount=(int) jdbcDF.count();
		int n=col.size();
		List<String> LengthGroups = new ArrayList<String>();
		
		
		
		
		
		/*
		
		int[] arr = new int[n];
		for(int i=0; i<n;i++)
		{
			arr[i]=i+1;
			
		}
		
		 
		 String tmp2=  printCombination(arr, n, 2);
     	 System.out.println(tmp2);

    	 String test[]= tmp2.split(",");
    	 
    	 List<String> invoiceCalculationsResult = new ArrayList<String>();
 		ExecutorService executor = Executors.newFixedThreadPool(numthread);
 		List<java.util.concurrent.Future<String>> futuresList = new ArrayList<>();
    	 
    	 for (int i=0;i<test.length;i++)
		 {
    		 
    		 System.out.println("hi "+test[i]);
			 if(! test[i].isEmpty())
			 {
				 
				 
				 String[]  tmp3=test[i].split("-");
				 int x=Integer.parseInt(tmp3[1]) -1;
				int y=Integer.parseInt(tmp3[2]) -1;
				String st=col.get(x).toString();
				String st1=col.get(y).toString();
				System.out.println("Sending to "+st+"  "+st1);
				
				Callable<String> task = new seualityRules1( st,  st1,   jdbcDF,   sc,  p2,  Microsegment,  totalcount) ;
				java.util.concurrent.Future<String> future =  executor.submit(task);
				futuresList.add(future);

				
	
				/*
				String ss1="Select "+st+", "+st1+" from CM1 where ucase(trim("+st+"))=ucase(trim("+st1+"))";
		//		 System.out.println(ss1);
				 
				 Dataset<Row> sq = sc.sql(ss1);
				 int segcount=(int) sq.count();
				 
				 double segpe=segcount*100.00/totalcount;
				 
				 if(segpe > 99.999)
				 {
					 String s5="Equality,"+st.replaceAll("`", "")+", Column "+st.replaceAll("`","")+ " Must be equal to "+ st1.replaceAll("`","")+",";
					 System.out.println(s5);
					 p2.println(s5); 
					 p2.flush();
					 
				 }
				 
				 if(segpe>5 && segpe < 99.999 )
				 {
					 seualityRules2(st,st1,Microsegment,jdbcDF,sc, p2);
				 }
				 
				 
				 
			 }
			 
			 
			 
		 }
    	 
    	 for (java.util.concurrent.Future<String> future : futuresList) {
 			String calculationResult = null;
 			try {
 				calculationResult = future.get();
 			} catch (InterruptedException | ExecutionException e) {
 				// ... Exception handling code ...
 	        } 
 		//	invoiceCalculationsResult.add(calculationResult);
 	    }
     
 		executor.shutdown();
		
		*/
		
		
	}
	
	
	public static void seualityRules2(String st, String st1,List<String> Microsegment, Dataset<Row> jdbcDF, SparkSession sc,PrintWriter p2)
	{
		
		System.out.println("here");
		jdbcDF.createOrReplaceTempView("CM1");
		
		 for (int i=0;i<Microsegment.size();i++)
		 {
			 String micro=Microsegment.get(i);
			 String ss="Select "+Microsegment.get(i)+" as groupt, count(*) as countA from CM1 where ucase(trim("+st+"))=ucase(trim("+st1+")) group by "+Microsegment.get(i);
			 Dataset<Row> sq = sc.sql(ss);
			 sq.createOrReplaceTempView("BT2");
//			 sq.show();
			 
			 String ss1="Select "+Microsegment.get(i)+" as groupt, count(*) as countB from CM1 where ucase(trim("+st+")) !=ucase(trim("+st1+")) group by "+Microsegment.get(i);
			 Dataset<Row> sq1 = sc.sql(ss1);
//			 sq1.show();
			 sq1.createOrReplaceTempView("BT3");
			 
			 
				String s3="Select BT2.groupt, BT2.countA as countA, Case when BT3.countB  is null then 0 else BT3.countB end as countB from BT2 left join BT3 on BT2.groupt=BT3.groupt";
				Dataset<Row> sq2 = sc.sql(s3);
//	     		sq2.show();
					     		
	     		
				sq2.createOrReplaceTempView("BT4");
				
				
				
				
				String s4="Select groupt, countA, countB  from BT4 where countA > 100 and countA > 99*countB";
				Dataset<Row> sq3 = sc.sql(s4);
		//		System.out.println("h1");
				if(sq3.count()>0)
				{
				
				Row[] dataRows1 = (Row[]) sq3.collect();
				List<String> smlist= new ArrayList<String>();
				
				for (Row row : dataRows1)
				{
				
				 double tmp=100*(row.getLong(2)/row.getLong(1));
				 System.out.println("h2"+row.getLong(2));
				
				 String s5="Advanced-Equality,"+st.replaceAll("`", "")+" , IF "+micro+" ="+row.getString(0)+" then "+st+" AND "+st1+" Must be Equal ,"+ row.getLong(2);
				 p2.println(s5);
				 p2.flush();
//				 System.out.println(s5);
				}
				
				}
				
				String s5="Select groupt, countA, countB  from BT4 where countB> 100 and countB > 99*countA";
				Dataset<Row> sq4 = sc.sql(s5);
				if(sq4.count()>0)
				{
				
				Row[] dataRows2 = (Row[]) sq4.collect();
				
				
				for (Row row : dataRows2)
				{
				 double tmp=100*(row.getLong(1)/row.getLong(2));
				 String s6="Advanced-Equality,"+st.replaceAll("`", "")+" , IF "+micro+" ="+row.getString(0)+" then "+st+" And "+st1+" Should not be Equal ,"+row.getLong(1);
				 p2.println(s6);
				 p2.flush();
//				 System.out.println(s5);
				}
			 
				} 
			 
		 
				}
		
	}
	
	public static void deualityRules1(List<String> col,Dataset<Row> jdbcDF, SparkSession sc,PrintWriter p2)
	{
		
		jdbcDF.createOrReplaceTempView("CM1");
//		jdbcDF.show();
		int totalcount=(int) jdbcDF.count();
		int n=col.size();
		int[] arr = new int[n];
		for(int i=0; i<n;i++)
		{
			arr[i]=i+1;
			
		}
		
		 
		 String tmp2=  printCombination(arr, n, 2);
 //    	 System.out.println(tmp2);

    	 String test[]= tmp2.split(",");
    	 
    	 for (int i=0;i<test.length;i++)
		 {
			 if(! test[i].isEmpty())
			 {
				 String[]  tmp3=test[i].split("-");
				 int x=Integer.parseInt(tmp3[1]) -1;
				int y=Integer.parseInt(tmp3[2]) -1;
				String st=col.get(x).toString();
				String st1=col.get(y).toString();
				 
				 String ss1="Select "+st+", "+st1+" from CM1 where ucase(trim("+st+"))=ucase(trim("+st1+"))";
		//		 System.out.println(ss1);
				 
				 Dataset<Row> sq = sc.sql(ss1);
				 int segcount=(int) sq.count();
				 
				 double segpe=segcount*100.00/totalcount;
				 
				 if(segpe > 99.999)
				 {
					 String s5="Equality,"+st.replaceAll("`", "")+", Column "+st.replaceAll("`","")+ " Must be equal to "+ st1.replaceAll("`","")+",";
					 System.out.println(s5);
					 p2.println(s5); 
					 p2.flush();
					 
				 }
				 
			 }
			 
			 
			 
		 }
		
		
		
		
	}
	
	
	public static void neualityRules1(List<String> col,Dataset<Row> jdbcDF, SparkSession sc,PrintWriter p2)
	{
		
		jdbcDF.createOrReplaceTempView("CM1");
//		jdbcDF.show();
		int totalcount=(int) jdbcDF.count();
		int n=col.size();
		int[] arr = new int[n];
		for(int i=0; i<n;i++)
		{
			arr[i]=i+1;
			
		}
		
		 
		 String tmp2=  printCombination(arr, n, 2);
 //    	 System.out.println(tmp2);

    	 String test[]= tmp2.split(",");
    	 
    	 for (int i=0;i<test.length;i++)
		 {
			 if(! test[i].isEmpty())
			 {
				 String[]  tmp3=test[i].split("-");
				 int x=Integer.parseInt(tmp3[1]) -1;
				int y=Integer.parseInt(tmp3[2]) -1;
				String st=col.get(x).toString();
				String st1=col.get(y).toString();
				 
				 String ss1="Select cast("+st+" as double) as st2 , cast("+st1+" as double) as st3 from CM1 ";
		//		 System.out.println(ss1);
				 
				 Dataset<Row> sq = sc.sql(ss1);
				 sq.createOrReplaceTempView("CM2");
				 
				 String ss2="Select * from CM2 where st2=st3 ";
				 
				 Dataset<Row> sq1 = sc.sql(ss2);
				 int segcount=(int) sq1.count();
				 
				 double segpe=segcount*100.00/totalcount;
				 
				 if(segpe > 99.999)
				 {
					 String s5="Equality,"+st.replaceAll("`", "")+", Column "+st.replaceAll("`","")+ " Must be equal to "+ st1.replaceAll("`","")+",";
					 System.out.println(s5);
					 p2.println(s5); 
					 p2.flush();
					 
				 }
				 
			 }
			 
			 
			 
		 }
		
		
		
		
	}
	
	
	
	public static void eualityRules2(String col, String col1,Dataset<Row> jdbcDF, SparkSession sc,PrintWriter p2)
	{
		
		
		
		
	}
	
	
	public static void MicrorulesV2(List<String> NumericalRelationship,List<String> Microsegment, Dataset<Row> jdbcDF, Dataset<Row> jdbcDFS1, SparkSession sc, PrintWriter p2,PrintWriter p3)
	{
		
		System.out.println("Finding reasonability - Level 1");
		List<String> microsegmentdetails = new ArrayList<String>();
		ExecutorService executor = Executors.newFixedThreadPool(numthread);
		List<java.util.concurrent.Future<String>> futuresList = new ArrayList<>();

		
		for(int i=0;i<Microsegment.size();i++)
		{
			String st1=Microsegment.get(i).toString();
			
			Callable<String> task = new rangeRules4V(NumericalRelationship,st1,jdbcDF, jdbcDFS1,  sc, p2,threshold, fe);
			java.util.concurrent.Future<String> future =  executor.submit(task);
			futuresList.add(future);

			
			
			
		}
		
	
		for (java.util.concurrent.Future<String> future : futuresList) {
			String calculationResult = null;
			try {
				calculationResult = future.get();
			} catch (InterruptedException | ExecutionException e) {
				e.printStackTrace();
	        } 
			microsegmentdetails.add(calculationResult);
	    }
    
		executor.shutdown();
		
		int n=Microsegment.size();
		double[] mcount = new double[n];
		double[] mcountorg = new double[n];
		
		
			
		
			
			List<String> data=new ArrayList<String>();
			for(int k=0;k<microsegmentdetails.size();k++)
			{
				String ss2[]=microsegmentdetails.get(k).split(",");
				for(int kk=0;kk<ss2.length;kk++)
				{
					
					if(ss2[kk].length() !=0)
						data.add(ss2[kk]);
						
										
				}
					
				
			}
			
			Dataset<Row> df = sc.createDataset(data, Encoders.STRING()).toDF();
		    df.createOrReplaceTempView("TTT1");
		    String st2="Select split(value,'-')[0] as micro, split(value,'-')[1] as colname, cast (split(value,'-')[2] as double) as mbasis   from TTT1";
		    Dataset<Row> df1=sc.sql(st2);
		    df1.show();
		    df1.createOrReplaceTempView("TTT2");
		    
		    
		    if(fe==1)
		    {
			for(int j=0;j<NumericalRelationship.size();j++)
			{
				String st3="Select micro from TTT2 where colname='"+NumericalRelationship.get(j)+"' And mbasis <60.0 order by mbasis asc limit 3";
				System.out.println(st3);
				Dataset<Row> df2=sc.sql(st3);
				Row[] dataRows1 = (Row[]) df2.collect();
								
				long tmpc=0;
				String st4="";
				for (Row row : dataRows1)
				{
				st4=st4+row.getString(0)+"-";
				}
				
				String s5="Reasonability2,"+NumericalRelationship.get(j).replaceAll("`", "")+", "+st4;
				p2.println(s5);
				p2.flush();
				
			}
		    
		    }
		    else
		    {
		    	String st3="Select micro, avg(mbasis) as mbasis from TTT2 group by micro";
		    	Dataset<Row> df2=sc.sql(st3);
		    	df2.createOrReplaceTempView("TTT3");
		    	String st4="Select micro from TTT3 where mbasis <80 order by mbasis asc limit 3 ";
		    	Dataset<Row> df3=sc.sql(st4);
		    	
		    	Row[] dataRows1 = (Row[]) df3.collect();
		    	
		    	if(df3.count()>0)
		    	{
				
				
				
				for (Row row : dataRows1)
				{
				System.out.println("Microsegment "+row.getString(0));
				}
		    	}
		    	else
		    	{
		    		System.out.println("No Microsegment");
		    	}
				
				
		    	String st5="Select colname, avg(mbasis) as mbasis from TTT2 group by colname";
		    	Dataset<Row> df5=sc.sql(st5);
		    	df5.createOrReplaceTempView("TTT5");
		    	String st6="Select colname from TTT5 where mbasis <80 order by mbasis asc limit 5 ";
		    	Dataset<Row> df6=sc.sql(st6);
		    	
		    	Row[] dataRows2 = (Row[]) df6.collect();
		    	
		    	if(df6.count()>0)
		    	{
				
				
				
				for (Row row : dataRows2)
				{
				System.out.println("Anomaly Column "+row.getString(0));
				}
		    	}
		    	else
		    	{
		    		System.out.println("No Anomaly");
		    	}
		    	
		    	
		    }
		 
			
			
		
		
		
		
		    System.out.println("Completed: 20%");
		
		
		
		
	}
	
	
	public static void MicrorulesV3(List<String> columnNames, Dataset<Row> jdbcDF,Dataset<Row> jdbcDFS1, SparkSession sc, PrintWriter p2, List Microsegment)
	{
		
		DecimalFormat df = new DecimalFormat("#.##");

				
		System.out.println("Finding reasonability - Level 1");
		List<String> microsegmentdetails = new ArrayList<String>();
		ExecutorService executor = Executors.newFixedThreadPool(numthread);
		List<java.util.concurrent.Future<String>> futuresList = new ArrayList<>();
		
		
		for(int i=0;i<Microsegment.size();i++)
		{
			String st1=Microsegment.get(i).toString();
			System.out.println("Range1 "+st1);
			Callable<String> task = new rangeRules5V(st1,jdbcDF, jdbcDFS1,  sc, p2,threshold, fe);
			java.util.concurrent.Future<String> future =  executor.submit(task);
			futuresList.add(future);

			System.out.println("Running result for: "+st1);
			
			
		}
		
	
		for (java.util.concurrent.Future<String> future : futuresList) {
			String calculationResult = null;
			try {
				calculationResult = future.get();
			} catch (InterruptedException | ExecutionException e) {
				e.printStackTrace();
	        } 
			microsegmentdetails.add(calculationResult);
	    }
    
		executor.shutdown();
		
		
	
		List<String> data=new ArrayList<String>();
		for(int k=0;k<microsegmentdetails.size();k++)
		{
			
			if(microsegmentdetails.get(k).length() !=0)
					data.add(microsegmentdetails.get(k));
					
			
		}
		
		Dataset<Row> df1 = sc.createDataset(data, Encoders.STRING()).toDF();
	    df1.createOrReplaceTempView("TTT1");
	    String st2="Select split(value,'-')[0] as micro,  cast (split(value,'-')[1] as double) as mbasis   from TTT1";
	    Dataset<Row> df2=sc.sql(st2);
	    df2.show();
	    df2.createOrReplaceTempView("TTT2");
	    
	    
	    if(fe==1)
	    {
	    	
	    	
	    	
	    }
	    else
	    {
	    	String st3="Select micro, avg(mbasis) as mbasis from TTT2 group by micro";
	    	Dataset<Row> df3=sc.sql(st3);
	    	df3.createOrReplaceTempView("TTT3");
	    	String st4="Select micro from TTT3 where mbasis <80 order by mbasis asc limit 3 ";
	    	Dataset<Row> df4=sc.sql(st4);
	    	
	    	Row[] dataRows1 = (Row[]) df4.collect();
	    	
	    	if(df4.count()>0)
	    	{
			for (Row row : dataRows1)
			{
			System.out.println("Microsegment "+row.getString(0));
			}
	    	}
	    	else
	    	{
	    		System.out.println("No Microsegment");
	    	}
	    	
	    	
	   
			
	    	
	    	
	    }
	   
	     	   
	     	   
		
		
		
		
		
	}
	
	
	
	public static void Rangerules(List<String> NumericalRelationship, Dataset<Row> jdbcDF, Dataset<Row> jdbcDFS1, SparkSession sc, PrintWriter p2,PrintWriter p3)
	{
		
		System.out.println("Finding Range ");
		List<String> invoiceCalculationsResult = new ArrayList<String>();
		ExecutorService executor = Executors.newFixedThreadPool(numthread);
		List<java.util.concurrent.Future<String>> futuresList = new ArrayList<>();

		
		for(int i=0;i<NumericalRelationship.size();i++)
		{
			String st1=NumericalRelationship.get(i).toString();
			
			Callable<String> task = new rangeRules(st1,jdbcDF, jdbcDFS1,  sc, p2);
			java.util.concurrent.Future<String> future =  executor.submit(task);
			futuresList.add(future);

			System.out.println("Running result for: "+st1);
			
			
		}
		
	
		for (java.util.concurrent.Future<String> future : futuresList) {
			String calculationResult = null;
			try {
				calculationResult = future.get();
			} catch (InterruptedException | ExecutionException e) {
				// ... Exception handling code ...
	        } 
		//	invoiceCalculationsResult.add(calculationResult);
	    }
    
		executor.shutdown();
		
	}
	
	
	
	
	public static String getminmaxpercent(String col,Dataset<Row> jdbcDF, SparkSession sc)
	{
		Double s=0.0;
		
		jdbcDF.createOrReplaceTempView("AT1");
		String st1="select  count(*) as countA from AT1 group by "+col;
		Dataset<Row> jdbcDF1=sc.sql(st1);
//		jdbcDF1.show();
		jdbcDF1.createOrReplaceTempView("AT2");
		String st2="select  min(countA), max(countA) from AT2 where countA>100";
		Dataset<Row> jdbcDF2=sc.sql(st2);
//		jdbcDF2.show();
		Row[] dataRows1 = (Row[]) jdbcDF2.collect();
		int count2=0;
		int count3=0;
		for (Row row : dataRows1)
		{
			count2=(int) row.getLong(0);
			count3=(int) row.getLong(1);
		}
		
//		System.out.println("count2"+ count2);
//		System.out.println("count2"+ count3);
		
		s=(double) (count2*100.0/jdbcDF.count());
		double s1=(double) (count3*100.0/jdbcDF.count());
		
		String s3=s+"A"+s1;
		
		return s3;
		
		
	}
	
	public static void NullTargetrules(List<String> NullColTarget, Dataset<Row> jdbcDF, Dataset<Row> jdbcDFS1, SparkSession sc, PrintWriter p2,PrintWriter p3, List Microsegment5, List NullColTargetP)
	{
		System.out.println("Locating Conditional Completeness ");
		
		List<String> Microsegment6= new ArrayList<String>();
		
		/*
		for(int i=0;i<NullColTarget.size();i++)
		{
			
			if(!Microsegment5.contains(NullColTarget.get(i).toString()))
			{
				Microsegment5.add(NullColTarget.get(i).toString());
			}
			
			
			
		}*/
		List<String> invoiceCalculationsResult = new ArrayList<String>();
		ExecutorService executor = Executors.newFixedThreadPool(numthread);
		List<java.util.concurrent.Future<String>> futuresList = new ArrayList<>();
		
		for(int i=0;i<Microsegment5.size();i++)
		{
			String st=Microsegment5.get(i).toString();
			Callable<String> task = new nullColTarget( st,   jdbcDF,   sc,  p2,  NullColTarget,  NullColTargetP) ;
			java.util.concurrent.Future<String> future =  executor.submit(task);
			futuresList.add(future);

			System.out.println("Conditional Completeness: "+st);
			
			
			
			
			
			/*
//			System.out.println(Microsegment5.get(i).toString());
			String st=Microsegment5.get(i).toString();
			String minmaxpercent= getminmaxpercent(Microsegment5.get(i).toString(),jdbcDF,sc);
			String[] st5=minmaxpercent.split("A");
			double minpercent=Double.parseDouble(st5[0]);
			double maxpercent=Double.parseDouble(st5[1]);
			
			for(int j=0;j<NullColTarget.size();j++)
			{
				
				if(!NullColTarget.get(j).toString().equals(Microsegment5.get(i).toString()))
				{
				
				double Nullp=(double) NullColTargetP.get(j);
				int m=0;
				if(Nullp< 50)
				{
					if(Nullp > minpercent  && (100.0-Nullp) >maxpercent)
					{
						m=1;
					}
				}
				
				if(Nullp>= 50)
				{
					if(Nullp > maxpercent  && (100.0-Nullp) >minpercent)
					{
						m=1;
					}
				}
				
//				System.out.println("Null P "+ Nullp+"  -->"+ minpercent+" -->"+maxpercent);
				
				if(m==1)
				{
    			System.out.println("not skipping");
				String st1=NullColTarget.get(j).toString();
				jdbcDF.createOrReplaceTempView("BT1");
			//	CASE WHEN id = 1 OR id = 2 THEN "OneOrTwo" 
//				String s1="Select Case When "+st+" is null then 'BLANK' When "+st+"='' then 'BLANK'  else "+st+" end as groupt, count(*) as countA from T1 where "+st1+ " is null OR "+st1+"='' group by "+st;
				String s1="Select Case When trim(ifnull("+st+",''))='' then 'BLANK'  else "+st+" end as groupt, count(*) as countA from BT1 where trim(ifnull("+st1+",''))='' group by "+st;
				
//				System.out.println(s1);
				Dataset<Row> sq = sc.sql(s1);
//				sq.show();
								
				sq.createOrReplaceTempView("BT2");
				String s2="Select Case When trim(ifnull("+st+",''))='' then 'BLANK'  else "+st+" end as groupt, count(*) as countB from BT1 where trim(ifnull("+st1+",'')) !='' group by "+st;
//				System.out.println(s2);
				Dataset<Row> sq1 = sc.sql(s2);
				sq1.createOrReplaceTempView("BT3");
//				sq1.show();
				
				String s3="Select BT2.groupt, BT2.countA as countA, Case when BT3.countB  is null then 0 else BT3.countB end as countB from BT2 left join BT3 on BT2.groupt=BT3.groupt";
				Dataset<Row> sq2 = sc.sql(s3);
//	     		sq2.show();
				sq2.createOrReplaceTempView("BT4");
				
				String s4="Select groupt, countA, countB  from BT4 where countA > 100 and countA > 99*countB";
				Dataset<Row> sq3 = sc.sql(s4);
				if(sq3.count()>0)
				{
				
				Row[] dataRows1 = (Row[]) sq3.collect();
				List<String> smlist= new ArrayList<String>();
				
				for (Row row : dataRows1)
				{
				
				 double tmp=100*(row.getLong(2)/row.getLong(1));
				 String s5="Completeness-Advanced,"+st1.replaceAll("`", "")+" , IF "+st+" ="+row.getString(0)+" then "+st1+" Must be NULL ,"+row.getLong(2);
				 p2.println(s5);
				 p2.flush();
//				 System.out.println(s5);
				}
				
				
				}
				
				
				}
				else
				{
//					System.out.println("skipping");
				}
				
				}
				
				
			}
			
			*/
		} 
		
		for (java.util.concurrent.Future<String> future : futuresList) {
			String calculationResult = null;
			try {
				calculationResult = future.get();
			} catch (InterruptedException | ExecutionException e) {
				// ... Exception handling code ...
	        } 
		//	invoiceCalculationsResult.add(calculationResult);
	    }
    
		executor.shutdown();
		
	
		
		
		
		
		
		
	}
	
	
	public static void NullTargetrulesv2(ExecutorService es,List<String> NullColTarget, Dataset<Row> jdbcDF, Dataset<Row> jdbcDFS1, SparkSession sc, PrintWriter p2,PrintWriter p3, final String MicrosegmentTarget, List NullColTargetP, final int i)
	{
		System.out.println("Locating Conditional Completeness ");
		
		List<String> Microsegment6= new ArrayList<String>();
		
		
		
		Callable<String> callableObj = () -> {
		
			System.out.println(MicrosegmentTarget);
			String st=MicrosegmentTarget;
			String minmaxpercent= getminmaxpercent(MicrosegmentTarget,jdbcDF,sc);
			String[] st5=minmaxpercent.split("A");
			String BT1="";
			String BT2="";
			String BT3="";
			String BT4="";
			
			double minpercent=Double.parseDouble(st5[0]);
			double maxpercent=Double.parseDouble(st5[1]);
			
			for(int j=0;j<NullColTarget.size();j++)
			{
				
				if(!NullColTarget.get(j).toString().equals(MicrosegmentTarget))
				{
				
				double Nullp=(double) NullColTargetP.get(j);
				int m=0;
				if(Nullp< 50)
				{
					if(Nullp > minpercent  && (100.0-Nullp) >maxpercent)
					{
						m=1;
					}
				}
				
				if(Nullp>= 50)
				{
					if(Nullp > maxpercent  && (100.0-Nullp) >minpercent)
					{
						m=1;
					}
				}
				
				System.out.println("Null P "+ Nullp+"  -->"+ minpercent+" -->"+maxpercent);
				
				if(m==1)
				{
				System.out.println("not skipping");
				String st1=NullColTarget.get(j).toString();
				BT1=i+"BT1";
				jdbcDF.createOrReplaceTempView(BT1);
			//	CASE WHEN id = 1 OR id = 2 THEN "OneOrTwo" 
//				String s1="Select Case When "+st+" is null then 'BLANK' When "+st+"='' then 'BLANK'  else "+st+" end as groupt, count(*) as countA from T1 where "+st1+ " is null OR "+st1+"='' group by "+st;
				String s1="Select Case When trim(ifnull("+st+",''))='' then 'BLANK'  else "+st+" end as groupt, count(*) as countA from "+ BT1+" where trim(ifnull("+st1+",''))='' group by "+st;
				
				System.out.println(s1);
				Dataset<Row> sq = sc.sql(s1);
//				sq.show();
				
				 BT2=i+"BT2";
				sq.createOrReplaceTempView(BT2);
				String s2="Select Case When trim(ifnull("+st+",''))='' then 'BLANK'  else "+st+" end as groupt, count(*) as countB from "+ BT1 +" where trim(ifnull("+st1+",'')) !='' group by "+st;
				System.out.println(s2);
				Dataset<Row> sq1 = sc.sql(s2);
				 BT3=i+"BT3";
				sq1.createOrReplaceTempView(BT3);
//				sq1.show();
				
				String s3="Select "+BT2+".groupt, "+BT2+".countA as countA, Case when "+BT3+".countB  is null then 0 else "+ BT3+".countB end as countB from "+BT2+" left join "+BT3+" on "+BT2+".groupt="+BT3+".groupt";
				Dataset<Row> sq2 = sc.sql(s3);
//	     		sq2.show();
				 BT4=i+"BT4";
				sq2.createOrReplaceTempView(BT4);
				
				String s4="Select groupt, countA, countB  from "+BT4+" where countA > 100 and countA > 99*countB";
				Dataset<Row> sq3 = sc.sql(s4);
				if(sq3.count()>0)
				{
				
				Row[] dataRows1 = (Row[]) sq3.collect();
				List<String> smlist= new ArrayList<String>();
				
				for (Row row : dataRows1)
				{
					 double tmp=100*(row.getLong(2)/row.getLong(1));
				 String s5="Completeness-Advanced,"+st1.replaceAll("`", "")+" , IF "+st+" ="+row.getString(0)+" then "+st1+" Must be NULL ,"+row.getLong(2);
				 p2.println(s5);
				 p2.flush();
//				 System.out.println(s5);
				}
				
				
				}
				
				
				}
				else
				{
					System.out.println("skipping");
				}
				
				
				
				
				
				}
				
				
				
				
			}
			
			sc.catalog().dropTempView(BT1);
			sc.catalog().dropTempView(BT2);
			sc.catalog().dropTempView(BT3);
			sc.catalog().dropTempView(BT4);
			
			return "success";
			
		};
		
		
		java.util.concurrent.Future<String> f=es.submit(callableObj);
		 
		
		
		
		
		
		
		
	}
	
	
	
	public static void NullTargetrulesv1(List<String> NullColTarget, Dataset<Row> jdbcDF, Dataset<Row> jdbcDFS1, SparkSession sc, PrintWriter p2,PrintWriter p3, List Microsegment5, List NullColTargetP)
	{
		System.out.println("Locating Conditional Completeness -v2 ");
		
		List<String> Microsegment6= new ArrayList<String>();
		
		
		if(NullColTarget.size()>0)
		{
		
		
		
		for(int i=0;i<NullColTarget.size();i++)
		{
			
			if(!Microsegment6.contains(NullColTarget.get(i).toString()))
			{
				Microsegment6.add(NullColTarget.get(i).toString());
			}
			
			
			
		}
		
		
		
		int n=Microsegment6.size();
		int[] arr = new int[n];
		for(int i=0; i<n;i++)
		{
			arr[i]=i+1;
			
		}
		
		 int m=2;
		
		 String tmp2=  printCombination(arr, n, m);
		
//		 System.out.println(tmp2);
		 
		 String[]  tmp=tmp2.split(",");
		 
		
		 
		 List<String> invoiceCalculationsResult = new ArrayList<String>();
			ExecutorService executor = Executors.newFixedThreadPool(numthread);
			List<java.util.concurrent.Future<String>> futuresList = new ArrayList<>();
		 
		 for (int i=0;i<tmp.length;i++)
		 {
			 
			 if(! tmp[i].isEmpty())
			 {
				 String[]  tmp3=tmp[i].split("-");
				 int x=Integer.parseInt(tmp3[1]) -1;
				int y=Integer.parseInt(tmp3[2]) -1;
				String st=Microsegment6.get(x).toString();
				String st1=Microsegment6.get(y).toString();
				
				 Callable<String> task = new nullColTargetv1( st,  st1,   jdbcDF,   sc,  p2,  NullColTarget,  NullColTargetP) ;
					java.util.concurrent.Future<String> future =  executor.submit(task);
					futuresList.add(future);

					System.out.println("Running result for: "+st1);
				 

			 
			 }
			 
			 
			
		 }
		 
		 for (java.util.concurrent.Future<String> future : futuresList) {
				String calculationResult = null;
				try {
					calculationResult = future.get();
				} catch (InterruptedException | ExecutionException e) {
					// ... Exception handling code ...
		        } 
			//	invoiceCalculationsResult.add(calculationResult);
		    }
	    
			executor.shutdown();
		 
		 
			
		
		}
		
	}
	
	public static void evaluaterules(List col, Dataset<Row> jdbcDF, SparkSession sc, PrintWriter p3, List t, List scollist, List plist, List Llist, List Microsegment)
	{
		String tfs=System.getProperty("file.separator");
		System.out.println("Evaluating Data Quality");
	//	System.out.println("Length rules "+ Llist.size());
		int count=0;
		int totcount=0;
		int notnullcount=0;
		double nullper=0;
		String status="";
		jdbcDF.createOrReplaceTempView("T1");
		totcount=(int) jdbcDF.count();
		
		
		try
		{
			
		for(int i=0;i<col.size();i++)
		{
		String columnName=col.get(i).toString();
		String s2=columnName +" is not null AND "+ columnName +"!='' AND " + columnName+ "!='null' " ;
		String s1="Select *  from T1 where "+s2;
		Dataset<Row> sq1 = sc.sql(s1);
		notnullcount=(int) sq1.count();
		int nullcount=totcount-notnullcount;
		nullper=((totcount-notnullcount)*100.0)/totcount;
		double thres=Double.parseDouble(t.get(i).toString());
		
		
		   if(nullper<=thres)
		   {
			   status="Passed";
		   }
		   else
		   {
			   status="Failed";
			   String directory = new File(".").getCanonicalPath();
			   
			   String f2="_Completeness_"+columnName.replaceAll("`", "")+".csv";
			   Dataset<Row> sq2= jdbcDF.except(sq1);
			   f2=directory+tfs+"results1"+tfs+f2;
				sq2.coalesce(1).write().option("header", "true").csv(f2);
			   
		   }
			   
		  String   description="Number of Records with Null: ,"+nullcount;
		  p3.println("Completeness,"+columnName.replaceAll("`", "")+",Can not have more than "+thres+" percent null, "+status+","+description);
			
		
		
		}
		
		//secondary duplicates
		
		if(scollist.size()>0)
		{
		
		try
		{
			String t2="";
			for(int i=0;i<scollist.size();i++)
			{
			 t2=t2+scollist.get(i)+",";
			}
			
			 if(t2.endsWith(","))
	   	   {
	   		  t2 = t2.substring(0,t2.length() - 1); 
	   	   }
			
			 
			String s1="Select "+t2+" , count(*) as countA from T1 group by "+t2;
			Dataset<Row> sq1 = sc.sql(s1);
			
			sq1.createOrReplaceTempView("T2");
			
			
			String t4="Select * from T2 where countA >1"; 		
			
			Dataset<Row> sq2 = sc.sql(t4);
			count=(int) sq2.count();
			 if(count==0)
			   {
				   status="Passed";
			   }
			   else
			   {
				   status="Failed";
				   
				   String directory = new File(".").getCanonicalPath();
				   
				   String f2="_NonPrimayDups.csv";
				  
					f2=directory+tfs+"results1"+tfs+f2;
					sq2.coalesce(1).write().option("header", "true").csv(f2);
				   
			   }
				   
			  String   description="Number of duplicates: ,"+count;
			  p3.println("Uniqueness,Non Primary Columns,Can not be duplicate, "+status+","+description);
			
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
			
		}
		
	/*
		//primary duplicate 
		if(plist.size()>0)
		{
			
			
			String t2="";
			for(int i=0;i<plist.size();i++)
			{
			 t2=t2+plist.get(i)+",";
			}
			
			 if(t2.endsWith(","))
	   	   {
	   		  t2 = t2.substring(0,t2.length() - 1); 
	   	   }
			
			String s1="Select "+t2+" , count(*) as countA from T1 group by "+ t2;
			Dataset<Row> sq1 = sc.sql(s1);
			
			sq1.createOrReplaceTempView("T2");
			
			
			String t4="Select * from T2 where countA >1"; 		
			
			Dataset<Row> sq2 = sc.sql(t4);
			count=(int) sq2.count();
			 if(count==0)
			   {
				   status="Passed";
			   }
			   else
			   {
				   status="Failed";
			   }
				   
			  String   description="Number of duplicates: ,"+count;
			  p3.println("Uniqueness, Primary Columns,Can not be duplicate, "+status+","+description);
			
			
			
		}
		
		*/
		
		//Length rules
		
	//	System.out.println("Length rules "+ Llist.size());
		
		if(Llist.size()>0)
		{
			
			try
			{
			for(int i=0;i<Llist.size();i++)
			{
			String tmp=Llist.get(i).toString();
			String[] tmp1=tmp.split(":");
			String columnName=tmp1[0];
			String[] tmp2=tmp1[1].split("-");
			String s2="";
			if(tmp2.length==2)
			s2= "length("+columnName+") !="+tmp2[0]+ " AND length("+columnName+") !="+tmp2[1];
			if(tmp2.length==1)
			s2= "length("+columnName+") !="+tmp2[0];
			String s1="Select "+columnName+" from T1 where "+s2+" AND "+columnName+" is not null AND "+ columnName +"!='' AND " + columnName+ "!='null' AND" + columnName.trim()+ "!=''";
	//		System.out.print(s1);
			
			
			
			Dataset<Row> sq1 = sc.sql(s1);
//			sq1.show();
			count=(int) sq1.count();
			 if(count==0)
			   {
				   status="Passed";
			   }
			   else
			   {
				   status="Failed";
			   }
				   
			  String   description="Number of Non Conformed Values: ,"+count;
			  p3.println("Conformity,"+columnName.replaceAll("`", "")+",Must have length "+tmp1[1]+","+status+","+description);
			
			}
			}
			catch(Exception e)
			{
				e.printStackTrace();

			}
			
			
			
		}
		
	//	System.out.println("Drift Rules "+ Microsegment.size());
		
		if(Microsegment.size()>0)
		{
		
			for (int i=0;i<Microsegment.size();i++)
			{
				String tt1=" is not null AND "+ Microsegment.get(i) +"!='' AND " + Microsegment.get(i)+ "!='null' ";
				String t1="Select "+Microsegment.get(i)+", count(*) as counta from T1 where "+ Microsegment.get(i)+ tt1+ " group by "+Microsegment.get(i);
				Dataset<Row> sq = sc.sql(t1);
				sq.createOrReplaceTempView("T2");
				int repcount=(int) ((0.1*totcount)/100);
				
				Row[] dataRows1 = (Row[]) sq.collect();
				List<String> smlist= new ArrayList<String>();
				
				for (Row row : dataRows1)
				{
					smlist.add(row.getString(0));
				}
				
				String col2=(String) Microsegment.get(i);
				fuzzymatching(col2, smlist, p3);
				
				
				String t2="Select * from T2 where counta <="+repcount ;  
				Dataset<Row> sq1 = sc.sql(t2);
				
				int sqcount=(int) sq1.count();
				
				if(sqcount<10)
				{
				Row[] dataRows = (Row[]) sq1.collect();
			       
				 String t3="";
				 String t4="";
			    
		     	   for (Row row : dataRows)
				  
				   {
					   
						   t3=t3+row.getString(0)+"-";
					  
		     		   
		     		  
				   }
		     	   
		     	   if(t3.endsWith("-"))
		     	   {
		     		  t3 = t3.substring(0,t3.length() - 1); 
		     	   }
		     	   
		     	   
		     	   if(!t3.equals(""))
		     	   {
		     	   t3="Potential invalid values: " +t3;
		     	   status="Failed";
		     	   p3.println("Drift,"+Microsegment.get(i).toString().replaceAll("`", "")+","+status+","+t3+",");
		     	   }
				}

			
			}
			
			
			
		}
		
		
		
		
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
		
		
		
	}
	
	
	public static String fuzzymatching1(List vallist)
	{
		String result="";
		for(int i=0;i<vallist.size()-1;i++)
		{
			String t1=vallist.get(i).toString();
			String s1=vallist.get(i).toString();
			
			String misspelt=s1+"->";
			for (int j=i+1;j<vallist.size();j++)
			{
				
				String t2=vallist.get(j).toString();
				String s2=vallist.get(j).toString();
				t1=t1.toLowerCase();
				t2=t2.toLowerCase();
				t1 = t1.replaceAll("[^a-zA-Z0-9]", "");
				t2 = t2.replaceAll("[^a-zA-Z0-9]", "");
				double distance = StringUtils.getJaroWinklerDistance(s1, s2);
//				System.out.println(distance+" "+s1+" "+s2);
				if(distance>0.99)
				{
					misspelt=misspelt+s2+"-";
				}
				
			}
			
			if(misspelt.endsWith("-"))
	     	   {
				misspelt = misspelt.substring(0,misspelt.length() - 1);
			
				result=result+misspelt;
				
	     	   }
		
		}
		
		
		return result;
		
	}
	
	public static void fuzzymatching(String col, List vallist, PrintWriter p3)
	{
		for(int i=0;i<vallist.size()-1;i++)
		{
			String t1=vallist.get(i).toString();
			String s1=vallist.get(i).toString();
			String misspelt="";
			for (int j=i+1;j<vallist.size();j++)
			{
				String t2=vallist.get(j).toString();
				String s2=vallist.get(j).toString();
				t1=t1.toLowerCase();
				t2=t2.toLowerCase();
				t1 = t1.replaceAll("[^a-zA-Z0-9]", "");
				t2 = t2.replaceAll("[^a-zA-Z0-9]", "");
				double distance = StringUtils.getJaroWinklerDistance(s1, s2);
//				System.out.println(distance+" "+s1+" "+s2);
				if(distance>0.99)
				{
					misspelt=misspelt+s2+"-";
				}
				
			}
			
			if(misspelt.endsWith("-"))
	     	   {
				misspelt = misspelt.substring(0,misspelt.length() - 1);
				String status="Failed";
				misspelt="Potential similar values "+s1+":"+misspelt;
				
				p3.println("Drift,"+col.replaceAll("`", "")+","+misspelt+",");
	     	   }
	     	   
			
			
			
			
		}
	}
	
	public static void nonprimaryduplicates(List collist, Dataset<Row> jdbcDF, SparkSession sc, PrintWriter p3)
	{
	
	
		int count=0;
		try
		{
		jdbcDF.createOrReplaceTempView("T1");
		String t2="";
		for(int i=0;i<collist.size();i++)
		{
		 t2=t2+collist.get(i)+",";
		}
		
		 if(t2.endsWith("-"))
   	   {
   		  t2 = t2.substring(0,t2.length() - 1); 
   	   }
		
		String s1="Select "+t2+" , count(*) as countA from T1";
		Dataset<Row> sq1 = sc.sql(s1);
		
		sq1.createOrReplaceTempView("T2");
		
		
		String t4="Select * from T2 where countA >1"; 		
		
		Dataset<Row> sq2 = sc.sql(t4);
		count=(int) sq2.count();
		}
		catch (Exception e)
		{
			
		}
		
		String status="";
	  
	
	}
	   
	
	
	public static void findduplicatecount(String columnName, Dataset<Row> jdbcDF, SparkSession sc, PrintWriter p3)
	{
		String tfs=System.getProperty("file.separator");
		int count=0;
		try
		{
		jdbcDF.createOrReplaceTempView("T1");
		String s1="Select "+columnName+" , count(*) as countA from T1";
		Dataset<Row> sq1 = sc.sql(s1);
		
		sq1.createOrReplaceTempView("T2");
		
		
		String t4="Select * from T2 where countA >1"; 		
		
		Dataset<Row> sq2 = sc.sql(t4);
		count=(int) sq2.count();
		String status="";
		   if(count==0)
		   {
			   status="Passed";
		   }
		   else
		   {
			   status="Failed";
			  
			   String directory = new File(".").getCanonicalPath();
			   
			   String f2="_Dups.csv";
				
				f2=directory+tfs+"results1"+tfs+f2;
				sq2.coalesce(1).write().option("header", "true").csv(f2);
			  
				
		   }
		   String   description="Number of duplicates: ,"+count;
			p3.println("Uniqueness,"+columnName.replaceAll("`", "")+",Can not be duplicate, "+status+","+description);
			
		
		}
		catch (Exception e)
		{
			
		}
		
		
		   
	  
		
		
	}
	
	public static void findDrift(Dataset<Row> jdbcDF, Dataset<Row> jdbcDFS, SparkSession sc, PrintWriter p2,PrintWriter p3, List Microsegment )
	{
		
	
		
			List<String> invoiceCalculationsResult = new ArrayList<String>();
			ExecutorService executor = Executors.newFixedThreadPool(numthread);
			List<java.util.concurrent.Future<String>> futuresList = new ArrayList<>();
		 
		 
			for(int i=0;i<Microsegment.size();i++)
			{
				String st= Microsegment.get(i).toString();
				System.out.println(st);
				Callable<String> task = new findDrift(st, jdbcDF,  jdbcDFS,  sc, p2,  p3, threshold) ;
				java.util.concurrent.Future<String> future =  executor.submit(task);
				futuresList.add(future);
				
				
			}
			
			for (java.util.concurrent.Future<String> future : futuresList) {
				String calculationResult = null;
				try {
					calculationResult = future.get();
				} catch (InterruptedException | ExecutionException e) 
				{
				    e.printStackTrace();	
				} 
			//	invoiceCalculationsResult.add(calculationResult);
		    }
 	 
			executor.shutdown();

		
		
		
		
		
		
		
		
	}
	
	public static void findMicrosegmentgroup2(List<String> columnNames, Dataset<Row> jdbcDF,Dataset<Row> jdbcDFS1, SparkSession sc, PrintWriter p2, List Microsegment)
	{
		
		DecimalFormat df = new DecimalFormat("#.##");

		jdbcDF.createOrReplaceTempView("T1");
		List<String> MicrosegmentGroup= new ArrayList<String>();
		List<String> MicrosegmentCount= new ArrayList<String>();
		
		
		
		
		
	
		
	     int totrec=(int) jdbcDF.count();
	     
		 
		
		int n=Microsegment.size();
		int num=0;
	//	System.out.println(n);
		
		/*
		int[] arr = new int[n];
		for(int j=0; j<n;j++)
		{
			arr[j]=j+1;
			
		}
		
		String msegment="";
		*/
		String t2="";
		
		
		if(n>4)
		{
		
		
			double[] mcount = new double[n];
			double[] mcountorg = new double[n];
			
			
			for(int i=0;i<Microsegment.size();i++)
			{
				String t3="Select "+Microsegment.get(i)+", count(*) as countA from T1 group by "+Microsegment.get(i);
				Dataset<Row> sq1 = sc.sql(t3);
//				sq1.show();
				sq1.createOrReplaceTempView("T2");
				int reccount=(int) sq1.count();
				
				String t4="Select * from T2 where countA <= 10"; 		
				
				Dataset<Row> sq2 = sc.sql(t4);
				int singelreccount=(int) sq2.count();
							
				int indgroup=reccount-singelreccount;
				
				if(singelreccount==0)
				{
					singelreccount=1;
				}
				
				if(indgroup==0)
				{
					indgroup=1;
				}
				
				double t1=singelreccount*indgroup;
				
				
				
				mcount[i]=t1;
				mcountorg[i]=t1;
		
				
			}
			
					
			Arrays.sort(mcount);
			
			
	//		System.out.println(mcountorg.toString());
	//		System.out.println(mcount.toString());
			
			for(int i=0;i<mcountorg.length;i++)
			{
				if(mcount[0]==mcountorg[i])
				{
					t2=t2+Microsegment.get(i)+",";
				}
				if(mcount[1]==mcountorg[i])
				{
					t2=t2+Microsegment.get(i)+",";
				}
				if(mcount[2]==mcountorg[i])
				{
					t2=t2+Microsegment.get(i)+",";
				}
				if(mcount[3]==mcountorg[i])
				{
					t2=t2+Microsegment.get(i)+",";
				}
			}
			
			
			
		}
		else
		{
			num=Microsegment.size();
			for(int m=0;m<Microsegment.size();m++)
			{
				t2=t2+Microsegment.get(m)+",";
			}
			
			
		}
		
		if(t2.endsWith(","))
	   		{
		  t2 = t2.substring(0,t2.length() - 1); 
	   		}
		
		
		
		
		jdbcDFS1.createOrReplaceTempView("T1");
		int kk=1;
		String t1="Select "+t2+",  count(*) as counta from T1 group by "+t2;
		Dataset<Row> sq = sc.sql(t1);
		
		sq.createOrReplaceTempView("T2");
		
		String t3="Select * from T2 where counta > 100 ";
		
 		Dataset<Row> sq1 = sc.sql(t3);
		
//		sq1.show();	
		
		
		 Row[] adrow = (Row[]) sq1.collect();
	       
	     int maxrows=(int) sq1.count();
	       
	    
	     
	     String[] t5=t2.split(",");
	     String t6="";
	     num=t5.length;
	     
	    int mm=0;
	//    System.out.println("Num " +num);
	     
	     	   for (Row row : adrow)
			  
			   {
	     		   t6=" IF ";
	     		   for(int j=0;j<num;j++)
	     		   {
	     			  
	     			   t6=t6+t5[j]+"="+row.getString(j)+" AND ";
	     		   
	     		   }
				
	     		   if(t6.endsWith(" AND "))
	     	    	{
	     	    	  t6 = t6.substring(0,t6.length() - 5);
	     	    	}
	     		   
   				p2.println("Validity, Entire Record ,"+t6+ ",");
   				p2.flush();
     			 
				  
			   }
		
	     	   
	     	   
		
		
		
		
		
	}
	
	public static void findMicrosegmentgroup(List<String> columnNames, Dataset<Row> jdbcDF, Dataset<Row> jdbcDFS1, SparkSession sc, PrintWriter p2,PrintWriter p3, List Microsegment, List numericalRelationship, String maxnumcol, String results, String fd)
	{
		System.out.println("Locating Microsegments ");
		DecimalFormat df = new DecimalFormat("#.##");

		jdbcDFS1.createOrReplaceTempView("MGT1");
		List<String> MicrosegmentGroup= new ArrayList<String>();
		List<String> MicrosegmentCount= new ArrayList<String>();
		
//		jdbcDFS1.show();
		
		int r1=(int) jdbcDFS1.count();
		
		String aa1="";
		
		List<String> MicrosegmentBasis= new ArrayList<String>();
		
		for (int i=0;i<numericalRelationship.size();i++)
		{
			
			aa1=aa1+"max(cast("+numericalRelationship.get(i)+" as double)) - min(cast("+numericalRelationship.get(i)+" as double)),";
			
		}
		
		if(aa1.endsWith(","))
   		{
			aa1 = aa1.substring(0,aa1.length() - 1); 
   		}
	
		String aa2="Select "+aa1+ " from MGT1";
		
		Dataset<Row> spa1 = sc.sql(aa2);
//		spa1.show();
		
		Row[] adataRows = (Row[]) spa1.collect();
	       
	    
	     	   for (Row row : adataRows)
			  
			   {
	     		  for (int i=0;i<numericalRelationship.size();i++)
	     			{
	     			  double tmp65=row.getDouble(i)*r1;
	     		
	     		  MicrosegmentBasis.add(Double.toString(tmp65));
	     			}
			   }
		
		
		
		
	
		
	     int totrec=(int) jdbcDF.count();
	     
		 
		
		int n=Microsegment.size();
		int num=0;
	//	System.out.println(n);
		

		
	//	System.out.println(maxnumcol);
		
		int m=numericalRelationship.size();
		
		
		
		double[] mcount = new double[n];
		double[] mcountorg = new double[n];
		
		for(int i=0;i<n;i++)
		{
			mcount[i]=100.0;
			mcountorg[i]=100.0;
			
		}
		
		if(n>0)
		{
		
			List<String> invoiceCalculationsResult = new ArrayList<String>();
			ExecutorService executor = Executors.newFixedThreadPool(numthread);
			List<java.util.concurrent.Future<String>> futuresList = new ArrayList<>();
			
			for (int j=0;j<numericalRelationship.size();j++)
			{
				
				String numcol=numericalRelationship.get(j).toString();
				double mbasis=Double.parseDouble(MicrosegmentBasis.get(j));
				
				Callable<String> task = new microSegmentGroup( columnNames,  jdbcDF,  jdbcDFS1,  sc,  p2, p3,  Microsegment,  numericalRelationship,  maxnumcol,  results,  fd,  numcol,  mbasis); 
				java.util.concurrent.Future<String> future =  executor.submit(task);
				futuresList.add(future);
				
				
			}
			
			for (java.util.concurrent.Future<String> future : futuresList) {
				String calculationResult = null;
				try {
					calculationResult = future.get();
				} catch (InterruptedException | ExecutionException e) {
					// ... Exception handling code ...
		        } 
			//	invoiceCalculationsResult.add(calculationResult);
		    }
	    
			executor.shutdown();
			
			
		}
		
		
		
		
	}
	
	public static void findMicrosegmentgroupbk(List<String> columnNames, Dataset<Row> jdbcDF, Dataset<Row> jdbcDFS1, SparkSession sc, PrintWriter p2,PrintWriter p3, List Microsegment, List numericalRelationship, String maxnumcol, String results, String fd)
	{
		System.out.println("Locating Microsegments ");
		DecimalFormat df = new DecimalFormat("#.##");

		jdbcDFS1.createOrReplaceTempView("MGT1");
		List<String> MicrosegmentGroup= new ArrayList<String>();
		List<String> MicrosegmentCount= new ArrayList<String>();
		
//		jdbcDFS1.show();
		
		int r1=(int) jdbcDFS1.count();
		
		String aa1="";
		
		List<String> MicrosegmentBasis= new ArrayList<String>();
		
		for (int i=0;i<numericalRelationship.size();i++)
		{
			
			aa1=aa1+"max(cast("+numericalRelationship.get(i)+" as double)) - min(cast("+numericalRelationship.get(i)+" as double)),";
			
		}
		
		if(aa1.endsWith(","))
   		{
			aa1 = aa1.substring(0,aa1.length() - 1); 
   		}
	
		String aa2="Select "+aa1+ " from MGT1";
		
		Dataset<Row> spa1 = sc.sql(aa2);
//		spa1.show();
		
		Row[] adataRows = (Row[]) spa1.collect();
	       
	    
	     	   for (Row row : adataRows)
			  
			   {
	     		  for (int i=0;i<numericalRelationship.size();i++)
	     			{
	     			  double tmp65=row.getDouble(i)*r1;
	     		
	     		  MicrosegmentBasis.add(Double.toString(tmp65));
	     			}
			   }
		
		
		
		
	
		
	     int totrec=(int) jdbcDF.count();
	     
		 
		
		int n=Microsegment.size();
		int num=0;
	//	System.out.println(n);
		

		
	//	System.out.println(maxnumcol);
		
		int m=numericalRelationship.size();
		
		
		
		double[] mcount = new double[n];
		double[] mcountorg = new double[n];
		
		for(int i=0;i<n;i++)
		{
			mcount[i]=100.0;
			mcountorg[i]=100.0;
			
		}
		
		if(n>0)
		{
		
			
			
			for (int j=0;j<numericalRelationship.size();j++)
			{
				String t2="";
				double ll=0.0;
				double ll1=0.0;
				double ul1=0.0;
				double ul=0.0;
				jdbcDFS1.createOrReplaceTempView("MGT1");
			String t21="(percentile_approx(cast("+numericalRelationship.get(j)+" as double),0.99) - percentile_approx(cast("+numericalRelationship.get(j)+" as double),0.01)) as range";
			
			for(int i=0;i<Microsegment.size();i++)
			{
				String t3="Select "+Microsegment.get(i)+","+t21+", count(*) as countA from MGT1 group by "+Microsegment.get(i);
//				System.out.println(t3);
				Dataset<Row> sq1 = sc.sql(t3);
//				sq1.show();
				sq1.createOrReplaceTempView("MGT2");
				int reccount=(int) sq1.count();
				
				String t4="Select * from MGT2 where countA >0"; 		
				
				Dataset<Row> sq2 = sc.sql(t4);
				int singelreccount=(int) sq2.count();
							
				int indgroup=reccount-singelreccount;
				
				if(singelreccount==0)
				{
					singelreccount=1;
				}
				
				if(indgroup==0)
				{
					indgroup=1;
				}
				
				double t1=singelreccount;
				
//				String t5="Select avg(range*countA) from T2 where countA>10";
				String t5="Select sum(range*countA) from MGT2 ";
				Dataset<Row> sq3 = sc.sql(t5);
						
				Row[] dataRows1 = (Row[]) sq3.collect();
			       
			    double avgrange=0.0;  
			     
			     	   for (Row row : dataRows1)
					  
					   {
			     		   avgrange=row.getDouble(0);
					   }
						 
			   if(avgrange==0.0)
			   {
				   avgrange=1.0;
			   }
				
				
				
				double at2=(avgrange*100.0)/Double.parseDouble(MicrosegmentBasis.get(j));
				
 //      			System.out.println("Variation Test "+ numericalRelationship.get(j)+"-"+Microsegment.get(i)+ "distance "+ at2 );
				
				mcount[i]=at2;
				mcountorg[i]=at2;
				
				
		
				
			}
			
		
					
			
			Arrays.sort(mcount);
			
			int maxseg=mcount.length;
			if (maxseg > 3)
			{
				maxseg=3;
			}
			
			for (int jj=0;jj<maxseg;jj++)
			{
			for(int i=0;i<mcountorg.length;i++)
			{
				if(mcount[jj]==mcountorg[i] && mcount[jj]<90.0)
				{
					t2=t2+Microsegment.get(i)+",";
				}
				
			}
			}
			
			/*
				if(mcount[1]==mcountorg[i] && mcount[1]<90.0)
				{
					t2=t2+Microsegment.get(i)+",";
				}
				if(mcount[2]==mcountorg[i] && mcount[2]<90.0)
				{
					t2=t2+Microsegment.get(i)+",";
				}
				*/
			
			
			
			if(t2.endsWith(","))
	   		{
		     t2 = t2.substring(0,t2.length() - 1); 
	   		}
		
		
	//	System.out.println(t2);
		
		int kk=1;
		
		jdbcDF.createOrReplaceTempView("MGT1");
		int tmpcount=(int) jdbcDF.count();
		int tmpcount1=tmpcount/1000;
		String t1="";
		
		if(!t2.equals(""))
		{
		 t1="Select "+t2+", percentile_approx(cast("+numericalRelationship.get(j)+" as double),0.01) as lowerval, percentile_approx(cast("+numericalRelationship.get(j)+" as double),0.99) as upperval, count(*) as counta from MGT1 group by "+t2;
		}
		else
		{
			 t1="Select  percentile_approx(cast("+numericalRelationship.get(j)+" as double),0.01) as lowerval, percentile_approx(cast("+numericalRelationship.get(j)+" as double),0.99) as upperval, count(*) as counta from MGT1 ";
		}
		
	//	System.out.println(t1);

		Dataset<Row> sq = sc.sql(t1);
		
		sq.createOrReplaceTempView("MGT2");
		
//		sq.show();	
			
		String t3="Select * from MGT2 where counta >"+tmpcount1+" and lowerval is not null and upperval is not null ";
		
 		Dataset<Row> sq1 = sc.sql(t3);
 		sq1.createOrReplaceTempView("MGT3");
		
//		sq1.show();	
		int maxrows=(int) sq1.count();
		
		String t221="";
		String[] t23=t2.split(",");
		
		if(maxrows>1)
		{
		for(int ii=0;ii<t23.length;ii++)
		{
			t221=t221+"MGT1."+t23[ii]+"="+"MGT3."+t23[ii]+" AND ";
		}
		
		if(t221.endsWith(" AND "))
   		{
			t221 = t221.substring(0,t221.length() - 5); 
   		}
		}
				
		 Row[] adrow = (Row[]) sq1.collect();
		 
		 
	       
	     
	       
	     String t4=numericalRelationship.get(j)+"-"+sq.count()+"-";
	     
	     String[] t5=t2.split(",");
	     String t6="";
	     num=t5.length;
	     
	    int mm=0;
	//    System.out.println("Num " +num);
	     
	     	   for (Row row : adrow)
			  
			   {
	     		   t6=" IF ";
	     		   
	     		   if(maxrows>1)
	     		   {
	     		   for(int jj=0;jj<num;jj++)
	     		   {
	     			  
	     			   t6=t6+t5[jj]+"="+row.getString(jj)+" AND ";
	     		   
	     		   }
				
	     		  
	     		   }
	     		   
	     		  if(t6.endsWith(" AND "))
	     	    	{
	     	    	  t6 = t6.substring(0,t6.length() - 5);
	     	    	}
	     		   
	     		   if(t6.equals(" IF "))
	     		   {
	     			   t6=" IF ENTIRE RECORD ";
	     		   }
	     		   
	     		   ll1=0.0;
	     		   ul1=0.0;
	     		    if(maxrows>1 )
	     		    {
	     		     ll1=row.getDouble(num) ;
	     			 ul1=row.getDouble(num+1);
	     		    }
	     		    else
	     		    {
	     		    	ll1=row.getDouble(0) ;
		     			ul1=row.getDouble(1);
	     		    }
	     			
	     			
	     			
	     			
	     			double range=ul1-ll1;
	     			
	     			 ll=ll1 - 0.1*range;
	     			 ul=ul1 + 0.1*range;
	     			
	     			if(ul1<=0 && ll1<=0 && ul>0)
	     			{
	     				ul=0.0;
	     			}
	     			
	     			if(ul1>=0 && ll1>=0 && ll<0)
	     			{
	     				ll=0.0;
	     			}
	     			
	     			
	     			String lowerlimit= ll+"";
	     			String upperlimit=ul+"";
	     			
	     			
	     			String a1=numericalRelationship.get(j)+" will have the following range: ";
	     			String a2=" Lower_Limit:"+df.format(ll);
	     			String a3=" Upper_Limit:"+df.format(ul);
	     		   
	//     		    System.out.println(a3);
	     			
	     			
	     			
	     			if(maxrows >1)
		     		 {
	     		   p2.println("Validity, Entire Record ,"+t6+ ",");
	     		  p2.flush();
		     		 }
     			   p2.println("Reasonability,"+numericalRelationship.get(j).toString().replaceAll("`", "")+","+t6+ " Then "+ a1+a2+a3+",");
     			  p2.flush();
     			   
     		
     			   
     			   
     			   
     			   
			   } 
			
	     	  if(results.equals("1"))
			   {
	     		  
	     		 if(maxrows >1)
	     		 {
				 
				 jdbcDFS1.createOrReplaceTempView("MGT1");
				 
				 Dataset<Row> sq77 = sc.sql(t1);
				
				 sq77.createOrReplaceTempView("MGT2");
				 
				
				
		 		Dataset<Row> sq88 = sc.sql(t3);
		 		sq88.createOrReplaceTempView("MGT3");
				
				
				 String t31="(upperval-lowerval)";
				 String t32="(lowerval -0.1*"+t31+")";
				 String t33="(upperval +0.1*"+t31+")";
				 
				 String t22="Select MGT1.*,cast("+numericalRelationship.get(j)+" as double) as numtarget,MGT3.lowerval as lowerval, MGT3.upperval as upperval from MGT1,MGT3 where "+t221;
	//			 System.out.println(t22);
				 
				 Dataset<Row> sq2 = sc.sql(t22);
//				 sq2.show();
				 
				 
				 
				 sq2.createOrReplaceTempView("MGT4");
				 
				//String f3=schema+"_"+tablelist.get(kk)+"_DQ_Results.csv";
				
				String fd1=fd+numericalRelationship.get(j)+".csv";

				 
				 String t34="Select * from MGT4 where numtarget < "+t32+" OR numtarget >"+t33;
				 Dataset<Row> sq3 = sc.sql(t34);
				 
				 
//				     sq3.show();
				     
				  String status="Passed";
				  if(sq3.count()>0)
				  {
					  status="Failed";
					  sq3.coalesce(1).write().option("header", "true").csv(fd1);
					  
					  
				  }
				 
				  p3.println("Reasonability,"+numericalRelationship.get(j).toString().replaceAll("`", "")+", Must be within reasonable range,"+status+", Number of Failed Records: ,"+sq3.count());
				 
	     		 }
	     		 else
	     		 {
	     			 
	     			jdbcDFS1.createOrReplaceTempView("MGT1");
	     			
	     			 double range1=ul1-ll1;
	     			 double uul1=ul1+0.1*range1;
	     			 double lll1=ll1-0.1*range1;
	     			
					 
					 String t22="Select *,cast("+numericalRelationship.get(j)+" as double) as numtarget from MGT1";
					 
					 Dataset<Row> sq3 = sc.sql(t22);
					 sq3.createOrReplaceTempView("MGTT1");
					 
					 String tt22="Select * from MGTT1 where numtarget<"+lll1+" AND numtarget >"+uul1;
					 Dataset<Row> sq33 = sc.sql(tt22);
					String fd1=fd+numericalRelationship.get(j)+".csv";
//				     sq33.show();
				     String status="Passed";
					  if(sq33.count()>0)
					  {
						  status="Failed";
						  sq3.coalesce(1).write().option("header", "true").csv(fd1);
						  
					  }
				     p3.println("Reasonability,"+numericalRelationship.get(j).toString().replaceAll("`", "")+", Must be within reasonable range,"+status+", Number of Failed Records: ,"+sq33.count());
				     
	     			 
	     		 }  
	     		 
	     		 
	     		 
			   }
			
	     	//yes
		}
		
		
		
		
		
		
		
	
 			   
     			
		}   
		}
	     	   
	     	   
		
		public static void findMicrosegmentgroupbk(List<String> columnNames, Dataset<Row> jdbcDF, Dataset<Row> jdbcDFS1, SparkSession sc, PrintWriter p2,PrintWriter p3, List Microsegment, List numericalRelationship, String maxnumcol, String results)
		{
			
			DecimalFormat df = new DecimalFormat("#.##");

			jdbcDF.createOrReplaceTempView("T1");
			List<String> MicrosegmentGroup= new ArrayList<String>();
			List<String> MicrosegmentCount= new ArrayList<String>();
			
//			jdbcDF.show();
			
			String aa1="";
			
			List<String> MicrosegmentBasis= new ArrayList<String>();
			
			for (int i=0;i<numericalRelationship.size();i++)
			{
				
				aa1=aa1+"sum(cast("+numericalRelationship.get(i)+" as double)),";
				
			}
			
			if(aa1.endsWith(","))
	   		{
				aa1 = aa1.substring(0,aa1.length() - 1); 
	   		}
		
			String aa2="Select "+aa1+ " from T1";
			
			Dataset<Row> spa1 = sc.sql(aa2);
//			spa1.show();
			
			Row[] adataRows = (Row[]) spa1.collect();
		       
		    
		     	   for (Row row : adataRows)
				  
				   {
		     		  for (int i=0;i<numericalRelationship.size();i++)
		     			{
		     		
		     		  MicrosegmentBasis.add(Double.toString(row.getDouble(i)));
		     			}
				   }
			
			
			
			
			String ss1="Select avg(cast("+maxnumcol+" as double)) as avgtot from T1";
			
			
			Dataset<Row> sp1 = sc.sql(ss1);
//			sp1.show();
			
			Row[] dataRows = (Row[]) sp1.collect();
		       
		    double tavgrange=0.0;  
		     
		     	   for (Row row : dataRows)
				  
				   {
		     		   tavgrange=row.getDouble(0);
				   }
			
		     int totrec=(int) jdbcDF.count();
		     
			 
			
			int n=Microsegment.size();
			int num=0;
	//		System.out.println(n);
			
			/*
			int[] arr = new int[n];
			for(int j=0; j<n;j++)
			{
				arr[j]=j+1;
				
			}
			
			String msegment="";
			*/
			String t2="";
	//		System.out.println(maxnumcol);
			
			if(n>3)
			{
			
		
				double[] mcount = new double[n];
				double[] mcountorg = new double[n];
				String t21="(percentile_approx(cast("+maxnumcol+" as double),0.99) - percentile_approx(cast("+maxnumcol+" as double),0.01)) as range";
				
				for(int i=0;i<Microsegment.size();i++)
				{
					String t3="Select "+Microsegment.get(i)+","+t21+", count(*) as countA from T1 group by "+Microsegment.get(i);
	//				System.out.println(t3);
					Dataset<Row> sq1 = sc.sql(t3);
//					sq1.show();
					sq1.createOrReplaceTempView("T2");
					int reccount=(int) sq1.count();
					
					String t4="Select * from T2 where countA > 0"; 		
					
					Dataset<Row> sq2 = sc.sql(t4);
					int singelreccount=(int) sq2.count();
								
					int indgroup=reccount-singelreccount;
					
					if(singelreccount==0)
					{
						singelreccount=1;
					}
					
					if(indgroup==0)
					{
						indgroup=1;
					}
					
					double t1=singelreccount;
					
//					String t5="Select avg(range*countA) from T2 where countA>10";
					String t5="Select sum(range*countA) from T2 ";
					Dataset<Row> sq3 = sc.sql(t5);
							
					Row[] dataRows1 = (Row[]) sq3.collect();
				       
				    double avgrange=0.0;  
				     
				     	   for (Row row : dataRows1)
						  
						   {
				     		   avgrange=row.getDouble(0);
						   }
							 
				   if(avgrange==0.0)
				   {
					   avgrange=1.0;
				   }
					
					t1=(t1*avgrange)*(tavgrange/totrec);
					
	//				System.out.println("test -"+Microsegment.get(i) +"-"+ singelreccount +"-"+indgroup +"-"+ avgrange+"-"+tavgrange+"-"+totrec  );
					
					mcount[i]=avgrange;
					mcountorg[i]=avgrange;
			
					
				}
				
						
				Arrays.sort(mcount);
				
				
				
				for(int i=0;i<mcountorg.length;i++)
				{
					if(mcount[0]==mcountorg[i])
					{
						t2=t2+Microsegment.get(i)+",";
					}
					if(mcount[1]==mcountorg[i])
					{
						t2=t2+Microsegment.get(i)+",";
					}
					if(mcount[2]==mcountorg[i])
					{
						t2=t2+Microsegment.get(i)+",";
					}
					
				}
			
			
			}
			else
			{
				num=Microsegment.size();
				for(int m=0;m<Microsegment.size();m++)
				{
					t2=t2+Microsegment.get(m)+",";
				}
				
				
			}
			
			if(t2.endsWith(","))
		   		{
			  t2 = t2.substring(0,t2.length() - 1); 
		   		}
			
			
	//		System.out.println(t2);
			
			
			
			int kk=1;
			
			jdbcDF.createOrReplaceTempView("T1");
			int tmpcount=(int) jdbcDF.count();
			int tmpcount1=tmpcount/2000;
			
			for (int i=0;i<numericalRelationship.size();i++)
			{
			
			String t1="Select "+t2+", percentile_approx(cast("+numericalRelationship.get(i)+" as double),0.01) as lowerval,"+"percentile_approx(cast("+numericalRelationship.get(i)+" as double),0.99) as upperval, count(*) as counta from T1 group by "+t2;
			
	//		System.out.println(t1);

			Dataset<Row> sq = sc.sql(t1);
			
			sq.createOrReplaceTempView("T2");
			
//			sq.show();
			
			String t3="Select * from T2 where counta >"+tmpcount1+" and lowerval is not null and upperval is not null ";
			
	 		Dataset<Row> sq1 = sc.sql(t3);
	 		sq1.createOrReplaceTempView("T3");
			
//			sq1.show();	
			
			String t21="";
			String[] t23=t2.split(",");
			
			for(int ii=0;ii<t23.length;ii++)
			{
				t21=t21+"T1."+t23[ii]+"="+"T3."+t23[ii]+" AND ";
			}
			
			if(t21.endsWith(" AND "))
	   		{
				t21 = t21.substring(0,t21.length() - 5); 
	   		}
			
			
			
			 
			
			
			 Row[] adrow = (Row[]) sq1.collect();
		       
		     int maxrows=(int) sq1.count();
		       
		     String t4=numericalRelationship.get(i)+"-"+sq.count()+"-";
		     
		     String[] t5=t2.split(",");
		     String t6="";
		     num=t5.length;
		     
		    int mm=0;
	//	    System.out.println("Num " +num);
		     
		     	   for (Row row : adrow)
				  
				   {
		     		   t6=" IF ";
		     		   for(int j=0;j<num;j++)
		     		   {
		     			  
		     			   t6=t6+t5[j]+"="+row.getString(j)+" AND ";
		     		   
		     		   }
					
		     		   if(t6.endsWith(" AND "))
		     	    	{
		     	    	  t6 = t6.substring(0,t6.length() - 5);
		     	    	}
		     		   
		     		   	
		     		    double ll1=row.getDouble(num) ;
		     			double ul1=row.getDouble(num+1);
		     			double range=ul1-ll1;
		     			
		     			double ll=ll1 - 0.1*range;
		     			double ul=ul1 + 0.1*range;
		     			
		     			if(ul1<=0 && ll1<=0 && ul>0)
		     			{
		     				ul=0.0;
		     			}
		     			
		     			if(ul1>=0 && ll1>=0 && ll<0)
		     			{
		     				ll=0.0;
		     			}
		     			
		     			
		     			String lowerlimit= ll+"";
		     			String upperlimit=ul+"";
		     			
		     			
		     			String a1=numericalRelationship.get(i)+" will have the following range: ";
		     			String a2=" Lower_Limit:"+df.format(ll);
		     			String a3=" Upper_Limit:"+df.format(ul);
		     		   
	//	     		    System.out.println(a3);
		     			
		     			
		     			
		     			
		     		   p2.println("Validity, Entire Record ,"+t6+ ",");
	     			   p2.println("Reasonability,"+numericalRelationship.get(i).toString().replaceAll("`", "")+","+t6+ " Then "+ a1+a2+a3+",");
	     			  p2.flush();
	     			   

		     			
					  
				   }
		     	   
	 			   if(results.equals("1"))
	 			   {
	 				 
	 				 jdbcDFS1.createOrReplaceTempView("T1");
	 				 
	 				 Dataset<Row> sq77 = sc.sql(t1);
	 				
	 				 sq77.createOrReplaceTempView("T2");
	 				 
	 				
	 				
	 		 		Dataset<Row> sq88 = sc.sql(t3);
	 		 		sq88.createOrReplaceTempView("T3");
	 				
	 				
	 				 String t31="(upperval-lowerval)";
	 				 String t32="(lowerval -0.1*"+t31+")";
	 				 String t33="(upperval +0.1*"+t31+")";
	 				 
	 				 String t22="Select T1.*,cast("+numericalRelationship.get(i)+" as double) as numtarget,T3.lowerval as lowerval, T3.upperval as upperval from T1,T3 where "+t21;
	 				 Dataset<Row> sq2 = sc.sql(t22);
//	 				 sq2.show();
	 				 
	 				 
	 				 
	 				 sq2.createOrReplaceTempView("T4");
	 				 
	 				 
	 				 String t34="Select * from T4 where numtarget < "+t32+" OR numtarget >"+t33;
	 				 Dataset<Row> sq3 = sc.sql(t34);
//					 sq3.show();
					 
					 String status="Passed";
					 if(sq3.count()>0)
					 {
						 status="Failed";
					 }
					 
					 p3.println("Reasonability,"+numericalRelationship.get(i).toString().replaceAll("`", "")+", Must be within Valid Range,"+status+" Number of Failed Records: ,"+sq3.count());
					 
	 				   
	 			   }
	     			
		     	   
			}
		     	   
		
		
		
	}
	
	public static double findOptCorr1(String relations, List Microsegment, Dataset<Row> jdbcDF1, SparkSession sc, List numericalRelationship, String maxnumcol)
	{
		
		
		jdbcDF1.createOrReplaceTempView("T1");
    	String t2="";
		
		String test1[]= relations.trim().split("-");
		System.out.println("Anazlying Relationships");
		
		for(int i=1;i<test1.length;i++)
		{
			String tmp=test1[i];
	//		System.out.println(i+"--"+tmp);
			int tmp1=Integer.parseInt(tmp) -1;
			t2=t2+Microsegment.get(tmp1)+",";
			
		}
		
		if(t2.endsWith(","))
  	   		{
  		  t2 = t2.substring(0,t2.length() - 1); 
  	   		}
		
		String t21="(percentile_approx(cast("+maxnumcol+" as double),0.95) - percentile_approx(cast("+maxnumcol+" as double),0.05)) as range";
		
		
		String t3="Select "+t2+","+t21+", count(*) as countA from T1 group by "+t2;
		
	//	System.out.println(t3);
		
		Dataset<Row> sq1 = sc.sql(t3);
		
		sq1.createOrReplaceTempView("T2");
		int reccount=(int) sq1.count();
		
		String t4="Select * from T2 where countA <= 10"; 		
		
		Dataset<Row> sq2 = sc.sql(t4);
		int singelreccount=(int) sq2.count();
		
	
		
		
		
		int indgroup=reccount-singelreccount;
		
		double t1=indgroup*singelreccount;
		
		String t5="Select avg(range) from T2 where countA>10";
		Dataset<Row> sq3 = sc.sql(t5);
				
		Row[] dataRows = (Row[]) sq3.collect();
	       
	    double avgrange=0.0;  
	     
	    
	     
	     	   for (Row row : dataRows)
			  
			   {
	     		   avgrange=row.getDouble(0);
			   }
				 
	     		  
		
	//	System.out.println(t1 + avgrange );
		
		t1=t1*avgrange;
		
		return t1;
		
		
	}
	
		
	
	public static void findMicrosegment(List<String> columnNames, Dataset<Row> jdbcDF1, SparkSession sc, PrintWriter p2, List Microsegment, List numericalRelationship, String maxnumcol )
	{
		
		System.out.println("Locating Microsegments ");
		
		Dataset<Row> jdbcDF=jdbcDF1.sample(false, 0.5);
		
		jdbcDF.createOrReplaceTempView("T1");
		
		
		String t2="";
		
		int num=Microsegment.size();
		
		for (int i=0;i<Microsegment.size();i++)
		{
			t2=t2+Microsegment.get(i).toString()+",";
		}
		
		 if(t2.endsWith(","))
		 {
   		  t2 = t2.substring(0,t2.length() - 1); 
		 }
		
		
		for (int i=0;i<numericalRelationship.size();i++)
			{
			
			String t1="Select "+t2+", percentile_approx(cast("+numericalRelationship.get(i)+" as double),0.05) as lowerval,"+"percentile_approx(cast("+numericalRelationship.get(i)+" as double),0.95) as upperval, count(*) as counta from T1 group by "+t2;
			
	
			Dataset<Row> sq = sc.sql(t1);
			
			sq.createOrReplaceTempView("T2");
			
			String t3="Select * from T2 where counta > 10 ";
			
			Dataset<Row> sq1 = sc.sql(t3);
	//		sq1.show();
				
			 Row[] dataRows = (Row[]) sq1.collect();
		       
		     int maxrows=(int) sq1.count();
		       
		     String t4=numericalRelationship.get(i)+"-"+sq.count()+"-";
		     
		     String[] t5=t2.split(",");
		     String t6=" IF ";
		     
		    
		     
		     	   for (Row row : dataRows)
				  
				   {
					 
		     		   for(int j=0;j<num;j++)
		     		   {
		     		   t6=t6+t5[j]+"="+row.getString(j)+" AND ";
		     		   }
					
		     		   if(t6.endsWith(" AND "))
		     	    	{
		     	    	  t6 = t6.substring(0,t6.length() - 5);
		     	    	}
		     		   
		     		   
		     		    double ll1=row.getDouble(num) ;
		     			double ul1=row.getDouble(num+1);
		     			double range=ul1-ll1;
		     			
		     			double ll=ll1 - 0.1*range;
		     			double ul=ul1 + 0.1*range;
		     			
		     			if(ul1<=0 && ll1<=0)
		     			{
		     				ul=0.0;
		     			}
		     			
		     			if(ul1>=0 && ll1>=0)
		     			{
		     				ll=0.0;
		     			}
		     			
		     			
		     			String lowerlimit= ll+"";
		     			String upperlimit=ul+"";
		     			
		     			
		     			String a1=numericalRelationship.get(i)+" will have the following range: ";
		     			String a2=" Lower_Limit:"+ll;
		     			String a3=" Upper_Limit:"+ul;
		     		   
		     		   
		     			
		     		   
//					  p2.println("Reasonability,"+numericalRelationship.get(i)+","+t6+ " Then "+ a1+a2+a2+",");
					  p2.println("Validity, Entire Record ,"+t6+ ",");
					  p2.flush();
				   }
		     	   
		     	   
			
			
			
			
		
			/*
			sq.createOrReplaceTempView("T2");
			
			String t2="Select avg(avgvar), avg(count) from T2";
			
			Dataset<Row> sq1 = sc.sql(t2);
			
			 Row[] dataRows = (Row[]) sq1.collect();
		       
		       int maxrows=(int) sq1.count();
		       
		       String t3=Microsegment.get(i)+"-"+sq.count()+"-";
		    
		     	   for (Row row : dataRows)
				  
				   {
					 t3=t3+row.getDouble(0)+"-"+row.getDouble(1);
		
				   }
		     	   
		     	   System.out.println(t3);
		*/
		}
		
		
		
		
	}
	
	
	public static void findSegCorr1(String relations, List<String> columnNames, Dataset<Row> jdbcDF, SparkSession sc, PrintWriter p2, List Microsegment, List NumericalReationship, String maxnumcol )
	{
		jdbcDF.createOrReplaceTempView("T1");
		
	//	System.out.println("Analyzing Correlations");
	
		String test1[]= relations.split("-");
		
		
		/*
		for (int i=0;i<DateRelationship.size();i++)
		{
			
			t3=t3+t1+DateRelationship.get(i)+", '"+DateRelationshipFormat.get(i)+t2+DateRelationship.get(i)+",";
			
		}
	
		if(t3.endsWith(","))
    	{
    	  t3 = t3.substring(0,t3.length() - 1);
    	}
		
		String t4="Select "+t3+ " from T1";
		//	System.out.println(t4);
			Dataset<Row> sq = sc.sql(t4);
		
		*/
		
	}
	
	
	
	public static void findDateCorr(List<String> columnNames, Dataset<Row> jdbcDF, Dataset<Row> jdbcDFS, SparkSession sc, PrintWriter p2, PrintWriter p3,List DateRelationship, List DateRelationshipFormat, String res )
	{
		System.out.println("Analyzing Timeliness");
		jdbcDF.createOrReplaceTempView("T1");
		
		String t1="datediff(TO_DATE(CAST(UNIX_TIMESTAMP(";
		String t2="') AS TIMESTAMP))  , current_date()) as ";
		String t3="";
		
		for (int i=0;i<DateRelationship.size();i++)
		{
			
			t3=t3+t1+DateRelationship.get(i)+", '"+DateRelationshipFormat.get(i)+t2+DateRelationship.get(i)+",";
			
			
			
		}
	
		if(t3.endsWith(","))
    	{
    	  t3 = t3.substring(0,t3.length() - 1);
    	}
		
		
		String t4="Select "+t3+ " from T1 ";
		System.out.println(t4);
		Dataset<Row> sq = sc.sql(t4);
	//	sq.show();
		
		
		double[] quants = {0.01,0.5,0.99};
		
		List<String> invoiceCalculationsResult = new ArrayList<String>();
		ExecutorService executor = Executors.newFixedThreadPool(numthread);
		List<java.util.concurrent.Future<String>> futuresList = new ArrayList<>();
		
		for (int i=0;i<DateRelationship.size();i++)
		{
		
		String st=DateRelationship.get(i).toString();
		String st1=DateRelationshipFormat.get(i).toString();
		
		Callable<String> task = new findDateCorr(columnNames, sq, jdbcDFS,  sc,  p2,  p3, st,  st1,  res ) ;
		java.util.concurrent.Future<String> future =  executor.submit(task);
		futuresList.add(future);

		System.out.println("Running result for: "+st);
		
		
		
		
		
		} //here
		
		for (java.util.concurrent.Future<String> future : futuresList) {
			String calculationResult = null;
			try {
				calculationResult = future.get();
			} catch (InterruptedException | ExecutionException e) {
				// ... Exception handling code ...
	        } 
		//	invoiceCalculationsResult.add(calculationResult);
	    }
    
		executor.shutdown();
		
		
		int n=DateRelationship.size();
		
		
		int[] arr = new int[n];
		for(int j=0; j<n;j++)
		{
			arr[j]=j+1;
			
		}
		
			
		 String tmp2=  printCombination(arr, n, 2);
    	 System.out.println(tmp2);
    	 String test[]= tmp2.split(",");
		
    	 try {
		       
	    	 
    		 	List<String> invoiceCalculationsResult1 = new ArrayList<String>();
    			ExecutorService executor1 = Executors.newFixedThreadPool(numthread);
    			List<java.util.concurrent.Future<String>> futuresList1 = new ArrayList<>();

	    	   for(int j=0;j<test.length;j++)
	    	   {
	    		   if(test[j].length()>3)
		    	   {
	    		   String test1[]= test[j].split("-");
	    		   System.out.println(test[j] +" tt1");
	    		   
	    		//	findDateCorr1(test[j],columnNames, sq, jdbcDFS,sc,  p2, p3,  DateRelationship, DateRelationshipFormat, res);
	    			   
	    		   
	    		   Callable<String> task = new findDateCorr1(test[j],columnNames, sq, jdbcDFS,sc,  p2, p3,  DateRelationship, DateRelationshipFormat, res);
	   			java.util.concurrent.Future<String> future =  executor1.submit(task);
	   			futuresList1.add(future);
	    			   			
	    				   }
		    	   }
	    	   
	    	   for (java.util.concurrent.Future<String> future : futuresList1) {
	   			String calculationResult1 = null;
	   			try {
	   				calculationResult1 = future.get();
	   			} catch (InterruptedException | ExecutionException e) {
	   				// ... Exception handling code ...
	   	        } 
	   		//	invoiceCalculationsResult.add(calculationResult);
	   	    }
	       
	   		executor1.shutdown();
	    	   
	    	   
	       
    	 		}
			catch (Exception e)
			{
				e.printStackTrace();
			}
		
		
		
		
		
	
		
	}
	
	
	public static void findDateCorr1(String relationship, List<String> columnNames, Dataset<Row> jdbcDF, Dataset<Row> jdbcDFS, SparkSession sc, PrintWriter p2,PrintWriter p3, List numericalRelationship, List DateRelationshipFormat,String res  )
	{
		
		int r=0;
		String test1[]= relationship.trim().split("-");
		System.out.println(relationship);
		int len=test1.length;
	
		
		jdbcDF.createOrReplaceTempView("T1");
		jdbcDF.show();
		
		
		   int csize=test1.length;
		  
	     
	       int a=Integer.parseInt(test1[1]) -1;
	       int b=Integer.parseInt(test1[2]) -1;
	       String t5="Select cast("+numericalRelationship.get(a).toString()+" as double), cast("+numericalRelationship.get(b).toString()+ " as double) from T1";
	       
	       Dataset<Row> jdbcDF2 = sc.sql(t5);
	       jdbcDF2.show();
	       jdbcDF2.createOrReplaceTempView("T3");
	       
	       String col1=numericalRelationship.get(a).toString().replaceAll("`", "");
	       String col2=numericalRelationship.get(b).toString().replaceAll("`", "");
	       double corrfactor=jdbcDF2.stat().corr(numericalRelationship.get(a).toString().replaceAll("`", ""), numericalRelationship.get(b).toString().replaceAll("`", ""));
	      System.out.println(corrfactor);
	       
	      if(corrfactor>0.3)
	      {
	    	  
	    	
	    	  String sq1="Select (`"+col1+"` - `"+col2+"`) as col3 from T3";
				
				Dataset<Row> sq2 = sc.sql(sq1);
//				sq2.show();
				sq2.createOrReplaceTempView("T31");
			
				String sq21="Select * from T31 where col3 is not null ";
				Dataset<Row> sq = sc.sql(sq21);
					
				double[] quants = {0.01,0.5,0.99};
				
				double[] quantiles = sq.stat().approxQuantile("col3", quants,0.0);
				
				double range1=quantiles[2]-quantiles[0];
				double ll=(quantiles[0] - 0.1*range1);
				double ul=quantiles[2] + 0.1*range1;
				
				if(quantiles[2]<=0 && quantiles[0]<=0)
				{
					ul=0.0;
				}
				
				if(quantiles[2]>=0 && quantiles[0]>=0)
				{
					ll=0.0;
				}
				
				
				String lowerlimit= ll+"";
				String upperlimit=ul+"";
				
				String a1=col1+"-"+col2+" must have ";
				String a2=" Lower_Limit:"+ll;
				String a3=" Upper_Limit:"+ul;
				
				
				p2.println("Consistency,"+col1.toString().replaceAll("`", "")+", Differences between "+a1+a2+a3+",");
				p2.flush();
				
				if(res.equals("1"))
				{
					jdbcDFS.createOrReplaceTempView("T6");
//					jdbcDFS.show();
					
					String cf1=DateRelationshipFormat.get(a).toString();
				    String cf2=DateRelationshipFormat.get(b).toString();
					
					
					String t1="datediff(TO_DATE(CAST(UNIX_TIMESTAMP(`"+col1+"`,'"+cf1+"') AS TIMESTAMP))  , TO_DATE(CAST(UNIX_TIMESTAMP(`"+col2+"`,'"+cf2+"') AS TIMESTAMP))";
	//				System.out.println(t1);
					
					
					
				
					String t6="Select `"+ col1+ "`,`"+col2+"`, "+ t1+ ") as daydiff from T6 ";
	//				System.out.println(t6);
					Dataset<Row> sq6 = sc.sql(t6);
					sq6.createOrReplaceTempView("T7");
//					sq6.show();
					
					
					
					
					String t7="Select * from T7 where daydiff <"+ll+" OR daydiff >"+ul;
	//				System.out.println(t7);
					Dataset<Row> sq7 = sc.sql(t7);
//					sq7.show();
					
					String status="Passed";
					if(sq7.count()>0)
						status="Failed";
					
					p3.println("Consistency,"+col1.replaceAll("`", "")+", Day Differences between "+a1+a2+a3+","+status+", Number of Invalid values:,"+sq7.count());
				
					
					
				}
				
				
	    	  
	      }
	       
	      
		
	}
	
	
	public static void findCorr( Dataset<Row> jdbcDF, Dataset<Row> jdbcDFS,SparkSession sc, PrintWriter p2,PrintWriter p3, List numericalRelationship, String results )
	{
		
	       
	    		 List<String> invoiceCalculationsResult = new ArrayList<String>();
	    			ExecutorService executor = Executors.newFixedThreadPool(numthread);
	    			List<java.util.concurrent.Future<String>> futuresList = new ArrayList<>();
		    	 
		    	for (int i=0;i<numericalRelationship.size();i++)
		    	{
    			   Callable<String> task = new findCorr2( numericalRelationship.get(i).toString(),  jdbcDF, jdbcDFS, sc,  p2,  p3, numericalRelationship, results, threshold ); 
    				java.util.concurrent.Future<String> future =  executor.submit(task);
    				futuresList.add(future);
		    		   
		    	}
		    			   
		    			   			
		    		
		    	   
		    	   for (java.util.concurrent.Future<String> future : futuresList) {
		   			String calculationResult = null;
		   			try {
		   				calculationResult = future.get();
		   			} catch (InterruptedException | ExecutionException e) {
		   				e.printStackTrace();
		   	        } 
		   		//	invoiceCalculationsResult.add(calculationResult);
		   	    }
		       
		   		executor.shutdown();
		    	
		       
	    	 }
				
		

	
	public static void findCorr1(String relationship, List<String> columnNames, Dataset<Row> jdbcDF, Dataset<Row> jdbcDFS,SparkSession sc, PrintWriter p2, PrintWriter p3,List numericalRelationship,String results )
	{
		
		
		
		int r=0;
		String test1[]= relationship.trim().split("-");
	//	System.out.println(relationship);
		int len=test1.length;
		
		
	
		jdbcDF.createOrReplaceTempView("T1");
		
		
		   int csize=test1.length;
		  
	       
	       String t2="";
	       String t3="";
	       
	  
	       for(int j=1; j<csize; j++)
           {
	    	   int tmp=Integer.parseInt(test1[j]) -1;
			   String name=(String) numericalRelationship.get(tmp);
			   if(j<csize-1)
			   {
			   t2=t2+name+",";
			   
			   t3= t3 + name +" is not null AND "+ name +"!='' AND " + name+ "!='null' " +" AND " + name+ "!=0  AND"   ;
			   }
			   else
			   {
				   t2=t2+name;
				   
				   t3= t3 + name +" is not null AND "+ name +"!='' AND " + name+ "!='null' " +" AND " + name+ "!=0" ;
			   }
			   
			   
			   
           }
	       
	       String t4="Select "+t2+" from T1 where "+t3;
      //     System.out.println(t4);
	     
	       
	       Dataset<Row> jdbcDF1 = sc.sql(t4);
	       jdbcDF1.createOrReplaceTempView("T2");
	       
	       int a=Integer.parseInt(test1[1]) -1;
	       int b=Integer.parseInt(test1[2]) -1;
	       String t5="Select cast("+numericalRelationship.get(a)+" as double), cast("+numericalRelationship.get(b)+ " as double) from T2";
	       
	       Dataset<Row> jdbcDF2 = sc.sql(t5);
//	       jdbcDF2.show();
	       jdbcDF2.createOrReplaceTempView("T3");
	       
	       String col1=numericalRelationship.get(a).toString().replaceAll("`", "");
	       String col2=numericalRelationship.get(b).toString().replaceAll("`", "");
	       double corrfactor=jdbcDF2.stat().corr(numericalRelationship.get(a).toString().replaceAll("`", ""), numericalRelationship.get(b).toString().replaceAll("`", ""));
	     
	      System.out.println(col1+corrfactor+col2);
	       
	      if(corrfactor>0.5)
	      {
	    	  
	    	  
	    	  
	    	  String sq1="Select (`"+col1+"` / `"+col2+"`) as col3 from T3";
	//    	  System.out.println(corrfactor+ sq1);
				Dataset<Row> sq = sc.sql(sq1);
				double[] quants = {0.001,0.5,0.999};
				
				double[] quantiles = sq.stat().approxQuantile("col3", quants,0.0);
				
				double range1=quantiles[2]-quantiles[0];
				double ll=(quantiles[0] - 0.005*range1);
				double ul=quantiles[2] + 0.005*range1;
				
				if(quantiles[2]<=0 && quantiles[0]<=0 && ul>0)
				{
					ul=0.0;
				}
				
				if(quantiles[2]>=0 && quantiles[0]>=0 && ll<0)
				{
					ll=0.0;
				}
				
				
				String lowerlimit= ll+"";
				String upperlimit=ul+"";
				
				String a1=col1+"/"+col2+" must have ";
				String a2=" Lower_Limit:"+ll;
				String a3=" Upper_Limit:"+ul;
				
				
				p2.println("Consistency,"+col1.replaceAll("`", "")+", Ratio between "+a1+a2+a3+",");
				p2.flush();
				if(results.equals("1"))
				{
					
					jdbcDFS.createOrReplaceTempView("T6");
					String t6="Select "+t2+" from T6 where "+t3; 
					Dataset<Row> jdbcDFS1 = sc.sql(t6);
				    jdbcDFS1.createOrReplaceTempView("T7");
				    String t7="Select cast("+numericalRelationship.get(a)+" as double), cast("+numericalRelationship.get(b)+ " as double) from T7";
				    Dataset<Row> jdbcDFS2 = sc.sql(t7);
				    jdbcDFS1.createOrReplaceTempView("T8");
				    
				    String t8="Select `"+col1+"`,`"+col2+"`, (`"+col1+"` / `"+col2+"`) as col3 from T8";
				    Dataset<Row> jdbcDFS3 = sc.sql(t8);
				    jdbcDFS3.createOrReplaceTempView("T9");
				    
				    String t9="Select * from T9 where col3 <"+ll+" OR col3 >"+ul;
				    Dataset<Row> jdbcDFS4 = sc.sql(t9);
//				    jdbcDFS4.show();
				    
				    String status="Passed";
				    if(jdbcDFS4.count()>0)
				    {
				    	status="Failed";
				    }
				    
				   p3.println("Consistency,"+col1.replaceAll("`", "")+", Ratio between "+a1+a2+a3+","+status+", Number of Failed Records: ,"+jdbcDFS4.count());   
				   p2.flush();
					
				}
				
				
				
	    	  
	      }
	       
	      
		
	}
	
	public static void findRelationship(List<String> columnNames, Dataset<Row> jdbcDF,Dataset<Row> jdbcDFS, SparkSession sc, PrintWriter p2,PrintWriter p3, List numericalRelationship, String results )
	{
		
		System.out.println("Relationship Deepdive");
		int n=numericalRelationship.size();
		
		if(n>1)
		{
			
		
	
		int[] arr = new int[n];
		for(int i=0; i<n;i++)
		{
			arr[i]=i+1;
			
		}
		
		 int m=4;
		 if(n<4)
		 {
			 m=n;
		 }
		 String tmp2=  printCombination(arr, n, m);
   // 	 System.out.println(tmp2);

    	 String test[]= tmp2.split(",");
    	 
    	 List<String> startingchar= new ArrayList<String>();
    	 HashSet noDupSet = new HashSet();
	    	 
	    
    	 for(int i=0;i<test.length;i++)
    	 {
    		 if(test[i].length()>3)
    		 {
    			 
    			 String test1[]= test[i].split("-");
    			 noDupSet.add(test1[1]);
    		
    		 }
    		 
    	 }
	    	 
	    	 
	    	 
	    	 
	    	 
	    
	    	 
	  //  	 System.out.println(noDupSet.size());
	    	 
	    	 for(Object stock : noDupSet){
	    		 startingchar.add( stock.toString());
	    		}
	    	 
		       
	    	 try {
		       for(int i=0;i < noDupSet.size(); i++)
		       {
		    	
		    //	   System.out.println("Looking at " +startingchar.get(i) );
		    	   int mcount=0;
		    	 
		    	   for(int j=0;j<test.length;j++)
		    	   {
		    		   if(test[j].length()>3)
			    	   {
		    		   String test1[]= test[j].split("-");
		    		   if(Integer.parseInt(test1[1])==Integer.parseInt(startingchar.get(i)))
		    				   {
		    			   			
		    			   			int s=findRelationship1(test[j],columnNames, jdbcDF,jdbcDFS, sc,  p2, p3, numericalRelationship, results);
		    			   			
		    			   			if(s==1)
		    			   			{
		    			   				break;
		    			   			}
		    			   			
		    				   }
			    	   }
		    	   }
		       }
		       
	    	 }
				catch (Exception e)
				{
					e.printStackTrace();
				}
		
		}
		
	}
	
	
	
	public static int findRelationship1(String relationship, List<String> columnNames, Dataset<Row> jdbcDF,Dataset<Row> jdbcDFS, SparkSession sc, PrintWriter p2,PrintWriter p3, List numericalRelationship, String results)
	{
		
		DecimalFormat df = new DecimalFormat("#.##");
		String tfs=System.getProperty("file.separator");
		int r=0;
		String test1[]= relationship.trim().split("-");
	//	System.out.println(relationship);
		int len=test1.length;
		for(int i=0;i<len;i++)
		{
	//		System.out.println(test1[i]);
		}
		
		
		jdbcDFS.createOrReplaceTempView("T1");
		
		OLSMultipleLinearRegression regression = new OLSMultipleLinearRegression();
		  
		
		   
		   int csize=test1.length;
		  
	       
	       String t2="";
	       String t3="";
	       
	  
	       for(int j=1; j<csize; j++)
           {
	    	   int tmp=Integer.parseInt(test1[j]) -1;
			   String name=(String) numericalRelationship.get(tmp);
			   if(j<csize-1)
			   {
			   t2=t2+"cast("+name+" as double),";
			   
			   t3= t3 + "cast("+name +" as double)" +" is not null"+" AND " ;
			   }
			   else
			   {
				   t2=t2+"cast("+name+" as double)";
				   
				   t3= t3 + "cast("+name +" as double)" +" is not null";
			   }
			   
			   
			   
           }
	       
	       String t4="Select "+t2+" from T1 where "+t3;
	 //      System.out.println(t4);
	       Dataset<Row> jdbcDF1 = sc.sql(t4);
	       Row[] dataRows = (Row[]) jdbcDF1.collect();
	       
	       int maxrows=(int) jdbcDF1.count();
	       double[] y = new double[maxrows];
		   double[][] xx = new double[maxrows][];
		   double[][] x= new double[maxrows][len];
	       List c2 = new LinkedList();
	       
	  //     jdbcDF1.show();
	       String col[] =jdbcDF1.columns();
			int numcol=col.length;
	       
			   int i=0;
	     	   for (Row row : dataRows)
			  
			   {
				  for(int j=0;j<numcol;j++) 
				  {
				  
	              x[i][j]=row.getDouble(j);
	           
				  }
	           
				  i++;
	           
	           }
		  
	     	  
		  
		    for (int mm=0;mm<maxrows;mm++)
		    {
		    	y[mm]=x[mm][0];
		    	
		    	double[] temp = new double[numcol-1];
		    	for(int s=0;s<numcol-1;s++)
		    	{
		    		temp[s]=x[mm][s+1];
		    	}
		    	xx[mm]=temp;
		    	
		    }
		   
		    
		    try {
		    regression.newSampleData(y, xx);
			double[] beta = regression.estimateRegressionParameters();       

			double[] residuals = regression.estimateResiduals();

			double[][] parametersVariance = regression.estimateRegressionParametersVariance();

			double regressandVariance = regression.estimateRegressandVariance();

			double rSquared = regression.calculateAdjustedRSquared();

			double sigma = regression.estimateRegressionStandardError();
			
	//		 System.out.println("relationship  "+ rSquared+ " "+sigma );

		   if(rSquared >0.97)
		   {
	//		   System.out.println("relationship  "+ rSquared+ " "+sigma );
		    	int tm1=Integer.parseInt(test1[1])-1;
		    	String eq="Equation  :" +numericalRelationship.get(tm1).toString().replaceAll("`", "")+"= " + df.format(beta[0]) ;
		    	
		    	String t6="Cast("+numericalRelationship.get(tm1).toString()+" as double),";
		    	String t61=numericalRelationship.get(tm1).toString();
		    	String t7="";
		    	
		    	t7="("+numericalRelationship.get(tm1).toString() +"-1*"+beta[0];
		    	
		    	
		    	String t8="";
		    
		    	
		    	for (int k=1;k<beta.length;k++)
		    	{
		    		if(beta[k] > 0.00001)
		    		eq=eq + "+"+ df.format(beta[k])+" * "+ numericalRelationship.get(Integer.parseInt(test1[k+1])-1).toString().replaceAll("`", "");		    		    		
		    		t6=t6+"Cast("+numericalRelationship.get(Integer.parseInt(test1[k+1])-1).toString()+" as double),";
		    		t8=t8+"-1*"+Double.toString(beta[k])+"*"+numericalRelationship.get(Integer.parseInt(test1[k+1])-1).toString();
		    		
		    	}
		    	
		    	Arrays.sort(beta);
		    	if(t8.endsWith("-"))
		    	{
		    	  t8 = t8.substring(0,t8.length() - 1);
		    	}
	//	    	System.out.println("Hi 98");
//		    	System.out.println("htiii " + t8);
		    //	t6="Select "+t6+"from T6";
		    	t8=t7+t8+") as diff";
		    	
	//	    	System.out.println(t6);
	//    	System.out.println(t8);
		    	
		    	
		    //	System.out.println("Equation found rsquare= "+ rSquared);
		    //	System.out.println( eq);
		    	
		    	if(eq.endsWith(","))
		    	{
		    	  eq = eq.substring(0,eq.length() - 1);
		    	}
		    	
		    	int tmp2=(int) (100- (rSquared*100));
		    	double tmp3=(1-rSquared);
		    	double tmp4=tmp3*2;
		    	double tmp5=sigma*3;
		    	
		    	String tmp="Accuracy,"+numericalRelationship.get(tm1).toString().replaceAll("`", "")+","+eq+", error margin "+ df.format(tmp5)  +" ";
		    	p2.println(tmp);
		    	r=1;
		    	
		    	if(results.equals("1"))
		    	{
		    		if(t6.endsWith(","))
			    	{
			    	  t6 = t6.substring(0,t6.length() - 1);
			    	}
		    	//	t6="Select "+t6+" from T6 where "+t61+" is not null AND "+ t61 +"!='' AND " + t61+ "!='null' " ;
		    		t6="Select "+t6+" from T6 " ;
//		    		System.out.println(t6);
		    		jdbcDFS.createOrReplaceTempView("T6");
		    		Dataset<Row> jdbcDFS1 = sc.sql(t6);
//		    		jdbcDFS1.show();
		    		jdbcDFS1.createOrReplaceTempView("T7");
		    		String t62="Select * from T7 where "+t61+ "!=0";
		    		
		    		
		    		Dataset<Row> jdbcDFS2 = sc.sql(t62);
//		    		jdbcDFS2.show();
		    		jdbcDFS2.createOrReplaceTempView("T8");
		    		
		    		String t81="Select * , "+t8+" from T6";
		    		
		    		Dataset<Row> jdbcDFS3 = sc.sql(t81);
//		    		jdbcDFS3.show();
		    		jdbcDFS3.createOrReplaceTempView("T9");
		    //		jdbcDFS3.coalesce(1).write().option("header", "true").csv("opttest");
		    		
		    		String t91="Select * from T9 where diff<-"+tmp5+" OR diff>"+tmp5;
		    		Dataset<Row> jdbcDFS4 = sc.sql(t91);
//		    		jdbcDFS4.show();
		    		jdbcDFS4.createOrReplaceTempView("T10");
		    		 String status="Passed";
					 if(jdbcDFS4.count()>0)
					 {
						 status="Failed";
					 }
		    		String tmp55="Accuracy,"+numericalRelationship.get(tm1).toString().replaceAll("`", "")+","+eq+","+status+", Number of Failed Records :,"+jdbcDFS4.count();
		    		p3.println(tmp55);
		    		String directory = new File(".").getCanonicalPath();
					   
					   String f2="_Accuracy.csv";
						
						f2=directory+tfs+"results1"+tfs+f2;
		    		jdbcDFS4.coalesce(1).write().option("header", "true").csv(f2);
		    	}
		    	
		    	
		    	
		   }
		    
		    }
		    catch(Exception e)
		    {
		    	e.printStackTrace(); 
		    	
		    }
		
		return r;
	}
	
	
	public static int getRange(String name, Dataset<Row> uc, SparkSession sc  )
	{
		
		uc.createOrReplaceTempView("T2");
//		int recocunt=(int) uc.count();
//		uc.show();
		
//		String t1 = " Select "+ name +"=, count(*) as countA from T2 where " + name +" is not null AND "+ name +"!='' AND " + name+ "!='null' group by "+name ;
		String t1 = " Select max(cast("+name+" as int))-min(cast("+name+" as int)) as range from T2  ";
//		System.out.println(t1);
		
		
		
		   Dataset<Row> jdbcDF1 = sc.sql(t1);
	//	   jdbcDF1.show();
		  
		   int range=0;
		   if(jdbcDF1.count()==1)
		   {
		   
		   Row[] dataRows1 = (Row[]) jdbcDF1.collect();
		   
			
			for (Row row : dataRows1)
			{
				try {
				range=row.getInt(0);
				}
				catch (Exception e)
				{
	//				System.out.println("Error found skipping");
					range=0;
					
				}
			}
		
		   }
		
		
		return range;
		
	}
		
	
	public static int getUniqueCount(String name, Dataset<Row> uc, SparkSession sc  )
	{
		
		uc.createOrReplaceTempView("T2");
		int recocunt=(int) uc.count();
//		uc.printSchema();
		
//		String t1 = " Select "+ name +", count(*) as countA from T2 where " + name +" is not null AND "+ name +"!='' AND " + name+ "!='null' group by "+name ;
		String t1 = " Select "+ name +", count(*) as countA from T2  group by "+name ;
	//	System.out.println(t1);
		
		
		Dataset<Row> uc1 = sc.sql(t1);
		
		
				
		int ucount=(int) uc1.count();
		
		
		return ucount;
		
	}
		
	
	
	public static String getTypeChar(String name, List<String> listUnique, int rc)
    {
		NumberFormat df = new DecimalFormat("#0.00");      	  	
		int m= listUnique.size();
		int numtype=0;
		int ftype=0;
		int specialtype=0;
		int stringtype=0;
		int dtype=0;
		Double uniquepercent=0.00;
		int uniquecount=0;
		int min=0;
		int max=0;
		int totallength=0;
		double avglegth=0;
		String lentype="Unknown";
		
		String ctype="";
		String ctype1="";
		
	//	System.out.println(listUnique.toString());
		
		if(m !=0)
		{
			
			min=listUnique.get(0).toString().length();
			max=listUnique.get(0).toString().length();
			HashSet noDupSet = new HashSet();
			HashSet noDupSet2 = new HashSet();
			List<String> defs= new ArrayList<String>();
			List<String> lens= new ArrayList<String>();
			
			for (int i=0;i<m;i++)
				{
				String t=listUnique.get(i).toString();
				String tempctype="S";
				noDupSet.add(t);
				
				
				int s=listUnique.get(i).toString().length();
				lens.add(Integer.toString(s));
				
				
				if (s<min)
				{
					min=s;
				}
				if(s>max)
				{
					max=s;
				}
				totallength=totallength+s;
				
				
	    		
				tempctype=determineType(t);
	    		
				
				if(tempctype.equals("I"))
				{
					numtype=numtype+1;
				}
				if(tempctype.equals("F"))
				{
					ftype=ftype+1;
				}
				
				if(tempctype.equals("S"))
				{
					stringtype=stringtype+1;
				}
				
					if(tempctype.startsWith("D"))
					{
						dtype=dtype+1;
						defs.add(tempctype);
						System.out.println(name+"-->"+tempctype);
					}
		
				}
		
		 uniquecount=noDupSet.size();
		 uniquepercent=(double) (noDupSet.size()*100.00/m);
		 avglegth=(double)totallength/m;
		
		ctype="E";
		
		if (numtype*100/m >70)
		{
			ctype="I";
			if(ftype*100>0)
			{
				ctype="F";
			}
		}
		
		if (dtype*100/m >70)
		{
			ctype="D";
			Set<String> distinct = new HashSet<>(defs);
			String tmp="";
			for (String s: distinct) {
				tmp=s+"Count"+Collections.frequency(defs, s)+":";
				}
			ctype=tmp;
		}
		if (stringtype*100.0/m >0)
		{
			ctype="S";
		}
		if (ftype*100/m >70)
		{
			ctype="F";
		}
		
		
		
		
		if(min==max)
		{
			 lentype="Fixed";
		}
		else
		{
			 lentype="Variable";
		}
		
		
		Set<String> distinct2 = new HashSet<>(lens);
		String tmp="";
		for (String s: distinct2) {
			tmp=tmp+s+"Count"+Collections.frequency(lens, s)+":";
			}
		ctype1=tmp;
		
		}
		else
		{
			ctype="E";
		}
		
		
		
		double nullcountpercent=(rc-m)*100.00/rc;
		
//		System.out.println(ctype);
//		System.out.println(rc);
//		System.out.println(m);
		
		String tmp2=ctype+"-"+df.format(nullcountpercent)+"-"+df.format(uniquepercent)+"-"+lentype+"-"+df.format(avglegth)+"-"+ctype1;
//		System.out.println(tmp2);
		
		return tmp2;
		
		// IF the null percentage is greater than 30% -- not a candidate for anything including null check else candidate for null check
		
		// Date - if null percentage is 1 then candidate for data range check
		
		
		// Float - if the null percentage is less than 10% then candidate for record anomaly and numeric profile 
		
		// String - if the null percentage is  zero then microsegment 
		
		
		
    }
	
	
	
	
	
	public static String determineType(String s)
	{
		String ctype="S";
		
		ctype=isNumeric(s);
		String dtype=isValidateDate(s);
		
		if(ctype.equals("I") )
		{
			if(dtype.equals("S"))
			{
				
			}
			else
			{
				ctype=dtype;
			}
		}
		
		if(ctype.equals("S") )
		{
			if(dtype.equals("S"))
			{
				
			}
			else
			{
				ctype=dtype;
			}
		}
		
		
		return ctype;
	}
	 
	public static String isNumeric(String str)  
	{  
	 String ctype="S";
		
	  try  
	  {  
		
	    double d = Double.parseDouble(str);  
	    if(d%1==0)
	    {
	    	ctype="I";
	    	
	    	
	    }
	    else
	    {
	    	ctype="F";
	    	
	    }
	    
	    
	  }  
	  catch(NumberFormatException nfe)  
	  {  
	    ctype="S";
	  }  
	  
	  return ctype;  
	}
	
	public static String isValidateDate(String s)
	{
		String ctype="S";
		String[] pdf = {"yyyyMMdd","yyyyMM","yyyy-MM","MMyyyy","MM-yyyy", "MM/dd/yyyy", "yyyy-MM-dd","MM-dd-yyyy","dd/MM/yyyy", "ddMMyyyy", "MMddyyyy","MMM-yy","yyyy-MM-dd","dd-MMM-yyyy","dd-MMM-yy","dd-MMM-yyyy"};
		
		int d=0;
		
		for (int i=0; i<pdf.length;i++)
		{
		
		SimpleDateFormat dateFormat = new SimpleDateFormat(pdf[i]);
		
        dateFormat.setLenient(false);
        
        try {
            dateFormat.parse(s);
            d=d+1;
            ctype="D:"+pdf[i];
            return ctype;
        } catch (Exception e) 
        {
            
        }
		}
		
		return ctype;
	}
	
	
	
	public static boolean isSpecial(String s) {
	    return (s == null) ? false : s.matches("[^A-Za-z0-9]");
	}
	
	

	
	
	
	public static void FindDuplicates(Dataset<Row> jdbcDF, SparkSession sc)
	{
		
		
		Dataset<Row> jdbcDF1=jdbcDF.sample(true, 0.05);
		
		
		
		List<String> primarycolList = new ArrayList<String>();
		List<String> secondarycolList= new ArrayList<String>();
		
		String col[] =jdbcDF1.columns();
		
		jdbcDF1.createOrReplaceTempView("T1");
		
		int reccount=(int) jdbcDF1.count();
		
		try {
		
		for(int i=0;i<col.length;i++)
		{
				
			String colname="`"+col[i]+"`";
	//		System.out.println(colname);
			//jdbcDF1.select(col[i]).distinct().show();
			
			String t1="Select  "+colname+" , count(*) as uniquecount from T1 where "+colname +" is not null AND "+ colname+"!='' AND " + colname+ "!='null' AND " + colname+ "!='NULL' group by "+colname;
			
						
	//		String t1="Select  "+colname+" , count(*) as uniquecount from T1 GROUP by  "+ colname;
	//		System.out.println(t1);
			Dataset<Row> uc = sc.sql(t1);
	//		uc.show();
			
			uc.createOrReplaceTempView("T2");
			String t2="Select sum(uniquecount) from T2";
			Dataset<Row> uc1 = sc.sql(t2);
	//		uc1.show();
			
						
			Row[] dataRows = (Row[]) uc1.collect();
			
//			uc1.show();
			
			if(uc1.count()>0)
			{
			long notnullcount=0;
		    for (Row row : dataRows) {
		    	notnullcount=(long) row.get(0);
		     			         
		    }
		    
		   int nullcount= (int) (reccount -notnullcount); 
		   int uniquecount=(int) uc.count();
		   double uniquepercent=(double)uniquecount*100.0/reccount;
		   double nullpercent=(double)nullcount*100.0/reccount;
		   
		    
		 //  System.out.println("Null Count "+nullcount);
		
		   if(nullpercent <95)
		   {
		   if(nullpercent<0.05 && uniquepercent >99.5)
		   {
			   primarycolList.add(colname);
			   
		   }
		   else
		   {
			   secondarycolList.add(colname);
		   }
		   }
			}
		   
		   
		   
		   
		  
		   

			
			
			
		
		}
		
	//	System.out.println(primarycolList);
		
		
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
		
		
		
		
		
		
		
		
		
	}
	
			public static String combinationUtil(int arr[], int data[], int start,
		            int end, int index, int r)
		{
		// Current combination is ready to be printed, print it
		
		
		String tmp1="";
		
		if (index == r)
		{
		String tmp="";
		for (int j=0; j<r; j++)
		{
		//    System.out.print(data[j]+"-");
		tmp=tmp+"-"+data[j];
		}
		
		        
		
		
		return tmp;
		}
		
		// replace index with all possible elements. The condition
		// "end-i+1 >= r-index" makes sure that including one element
		// at index will make a combination with remaining elements
		// at remaining positions
		for (int i=start; i<=end && end-i+1 >= r-index; i++)
		{
		data[index] = arr[i];
		
		
		tmp1=tmp1+","+ combinationUtil(arr, data, i+1, end, index+1, r);
		}
		
		return tmp1;
		}
		
		// The main function that prints all combinations of size r
		// in arr[] of size n. This function mainly uses combinationUtil()
		public static String printCombination(int arr[], int n, int r)
		{
		// A temporary array to store all combination one by one
		int data[]=new int[r];
		
		// Print all combination using temprary array 'data[]'
		return  combinationUtil(arr, data, 0, n-1, 0, r);
		}
		
		
		public static void deleteDir(File dir) {
		    File[] files = dir.listFiles();

		    for (File myFile: files) {
		        if (myFile.isDirectory()) {  
		            deleteDir(myFile);
		        } 
		        myFile.delete();

		    }
		}
		


	
}
	
		
		
		





