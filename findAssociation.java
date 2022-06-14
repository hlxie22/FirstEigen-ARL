package MSQLDB;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
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
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.fpm.FPGrowth;
import org.apache.spark.ml.fpm.FPGrowthModel;
import org.apache.spark.ml.regression.LinearRegression;
import org.apache.spark.ml.regression.LinearRegressionModel;
import org.apache.spark.ml.regression.LinearRegressionTrainingSummary;
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
import java.util.concurrent.Callable;
import java.net.HttpURLConnection;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.URL;
import java.net;
import java.io


public  class findAssociation implements Callable<String> {
	
	
	
    private   Dataset<Row> jdbcDF;
    private   Dataset<Row> jdbcDFS;
    private SparkSession sc;
    private   PrintWriter p2;
    private   PrintWriter p3;
    
    private List Microsegment;
    private String results;
    
	

	public findAssociation(Dataset<Row> jdbcDF,SparkSession sc, PrintWriter p2, PrintWriter p3,List Microsegment, String results, int threshold ) 
	{
				
		this.jdbcDF=jdbcDF;
		
		this.sc=sc;
		this.p2=p2;
		this.p3=p3;
		
		this.Microsegment=Microsegment;
		this.results=results;
	
	}
 

	public String  call() throws Exception 
	{
		
		String ss56="";
		
		
		
		
		FPGrowthModel model = new FPGrowth()
				  .setItemsCol("items")
				  .setMinSupport(0.1)
				  .setMinConfidence(0.99)
				  .setNumPartitions(60)
				  .fit(jdbcDF);
	
	//	Dataset<Row> jdbcDF3=model.freqItemsets();
	//	jdbcDF3.cache();
	//	jdbcDF3.show();
		
		
		
		
		Dataset<Row> jdbcDF3A=model.associationRules();
		jdbcDF3A.cache();
		jdbcDF3A.createOrReplaceTempView("TT2");
		
		String st1="Select antecedent , size(antecedent) as sizea, concat_ws('', consequent) as target from TT2 ";
		Dataset<Row> jdbcDF4=sc.sql(st1);
		jdbcDF4.createOrReplaceTempView("TTT3");
		
		String st2="select concat_ws('',antecedent) as antecedent ,target from TTT3 where sizea=1 ";
		Dataset<Row> jdbcDF5=sc.sql(st2);
		jdbcDF5.createOrReplaceTempView("TTT4");
		
		
	
		String st3="select * from TTT3 where  sizea=2 ";
		Dataset<Row> jdbcDF6=sc.sql(st3);
		jdbcDF6.createOrReplaceTempView("TTT5");
		
		
		
		String st4="Select TTT5.*, TTT4.antecedent as ad1 from TTT5 left join TTT4 on TTT5.target=TTT4.target and array_contains(TTT5.antecedent, TTT4.antecedent)";
		Dataset<Row> jdbcDF7=sc.sql(st4);
		jdbcDF7.createOrReplaceTempView("TTT6");
		
		
		String st5="Select * from TTT6 where ( !array_contains(antecedent, ad1) or ad1 is null)";
		Dataset<Row> jdbcDF8=sc.sql(st5);
		jdbcDF8.createOrReplaceTempView("TTT7");
		
		
		
		String st6="Select concat_ws(' AND ', antecedent) as antecendent, target from TTT7";
		Dataset<Row> jdbcDF9=sc.sql(st6);
		
		Dataset<Row> jdbcDF10=jdbcDF5.union(jdbcDF9);
		
		
		jdbcDF10.createOrReplaceTempView("TT1");
		
		String st7="select concat_ws('))OR((', collect_list(antecedent)), target from TT1 group by target ";
		Dataset<Row> jdbcDF11=sc.sql(st7);
		
	
		
	
		
		Row[] dataRows2 = (Row[]) jdbcDF11.collect();
//					jdbcDF6.show();
		for(Row row1:dataRows2)
		  {
			String stt1=row1.getString(1);
			String [] stt2=stt1.split(":");
			String stt3=stt2[0];
			String stt4=stt2[1];
			 System.out.println(row1.getString(1)+"-->"+row1.getString(0));
			 
			 String s8="Conditional-Relationship,"+stt3.replaceAll("`", "")+" , IF (("+row1.getString(0)+ " then ))"+stt3+" Must be ("+stt4+") ,";
			 p2.println(s8);
				p2.flush();
		  }
		
		
		/*
		
		
		jdbcDF4.show();
		
		String st2="select * from TTT3 where target rlike 'BLANK' and sizea=1 ";
		Dataset<Row> jdbcDF5=sc.sql(st2);
		
		jdbcDF5.show();
		System.out.println(jdbcDF5.count());
		
		jdbcDF5.createOrReplaceTempView("TTT4");
		
		String st3="select concat_ws('))OR((', collect_list(ad)), target from TTT4 group by target ";
		Dataset<Row> jdbcDF6=sc.sql(st3);
		
		jdbcDF6.show();
		System.out.println(jdbcDF6.count());
	
		
		Row[] dataRows2 = (Row[]) jdbcDF6.collect();
//					jdbcDF6.show();
		for(Row row1:dataRows2)
		  {
			 System.out.println(row1.getString(0));
			 
			  
		  }
		
		*/
		
		/*
		jdbcDF3.createOrReplaceTempView("B1");
		
				
		String tt1="Select items[0] as col1, items[1] as col2, freq as CountA from B1 where size(items)=2";
//		String t1="Select split(concat_ws(',',items), ',')[0] as col1, freq as CountA from BT1 where size(items)=3 and freq >100";
		
		Dataset<Row> jdbcDF4=sc.sql(tt1);
		jdbcDF4.createOrReplaceTempView("T1");
		
		
		String tt2="Select items[0] as col1, freq as CountB from B1 where size(items)=1";
//		String t1="Select split(concat_ws(',',items), ',')[0] as col1, freq as CountA from BT1 where size(items)=3 and freq >100";
		
		Dataset<Row> jdbcDF5=sc.sql(tt2);
		jdbcDF5.createOrReplaceTempView("T2");
		
		String tt3="Select T1.*, T2.countB from T1 left join T2 on  T1.col1=T2.col1";
		
		Dataset<Row> jdbcDF6=sc.sql(tt3);
		jdbcDF6.createOrReplaceTempView("T3");
	
		String tt4="Select *, countA*100.0/countB as peR, (countB-CountA) as abC from T3";
		Dataset<Row> jdbcDF7=sc.sql(tt4);
		jdbcDF7.createOrReplaceTempView("T4");
		
		String s5="Select * from T4 where peR > 99 and countA > 100 and col1 like 'C1' ";
		Dataset<Row> jdbcDF8=sc.sql(s5);
		jdbcDF8.show();
		
		
		
	
		
//		jdbcDF1.show();
				*/
		return ss56;
		
	}
    public void startServer(){
        String fromClient;
        String toClient;
 
        ServerSocket server = new ServerSocket(8080);
        System.out.println("wait for connection on port 8080");
 
        boolean run = true;
        while(run) {
            Socket client = server.accept();
            System.out.println("got connection on port 8080");
            BufferedReader in = new BufferedReader(new InputStreamReader(client.getInputStream()));
            PrintWriter out = new PrintWriter(client.getOutputStream(),true);
 
            fromClient = in.readLine();
            System.out.println("received: " + fromClient);
 
            if(fromClient.equals("Hello")) {
                toClient = "olleH";
                System.out.println("send olleH");
                out.println(toClient);
                fromClient = in.readLine();
                System.out.println("received: " + fromClient);
 
                if(fromClient.equals("Bye")) {
                    toClient = "eyB";
                    System.out.println("send eyB");
                    out.println(toClient);
                    client.close();
                    run = false;
                    System.out.println("socket closed");
                }
            }
        }
        System.exit(0);
    }


}




