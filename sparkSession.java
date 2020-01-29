import org.apache.spark.sql.SparkSession;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaStreamingContext;



public class sparkSession{
	
	private String Host;
	private int Port;
	private String HDFSPath;
	private List Header;
	private String Separator;
	private String Filter;
	private String TableRef;
	
	private sparkSession(Builder builder)
	{
		setHost(builder.Host);
		setPort(builder.Port);
		setHDFSPath(builder.HDFSPath);
		setHeader(builder.Header);
		setSeparator(builder.Separator);
		setFilter(builder.Filter);
	}
	
	public static Builder newSparkSession(){
		return new Builder();
	}
	public void setTableRef(String ref) {
		this.TableRef = ref;
	}
	public String getTableRef() {
		return this.TableRef;
	}
	public void setHost(String host) {
		this.Host=host;
	}
	public void setPort(int port) {
		this.Port=port;
	}
	public void setHDFSPath(String path) {
		this.HDFSPath=path;
	}
	public void setHeader(List header) {
		this.Header = header;
	}
	public void setSeparator(String separator) {
		this.Separator=separator;
	}
	public void setFilter(String filter) {
		this.Filter= filter;
	}
	public String getHost() {
		return this.Host;
	}
	public int getPort() {
		return this.Port;
	}
	public String getHDFSPath() {
		return this.HDFSPath;
	}
	public List getHeader() {
		return this.Header;
	}
	public String getSeparator() {
		return this.Separator;
	}
	public String getFilter() {
		return this.Filter;
	}
	public static final class Builder{
		private String Host;
		private int Port;
		private String HDFSPath;
		private List Header;
		private String Separator;
		private String Filter;
		private String TableRef;
		
		public Builder Host(String host)
		{
			this.Host =  host;
			return this;
		}
		public Builder TableRef(String ref)
		{
			this.TableRef = ref;
			return this;
		}
		public Builder Port(int port)
		{
			this.Port = port;
			return this;
		}
		public Builder HDFSPath(String hdfsPath)
		{
			this.HDFSPath = hdfsPath;
			return this;
		}
		public Builder Header(List header)
		{
			this.Header =  header;
			return this;
		}
		public  Builder Separator(String separator)
		{
			this.Separator = separator;
			return this;
		}
		public Builder Filter(String filter)
		{
			this.Filter =  filter;
			return this;
		}
	
		public void start() {
			
			System.out.println("starting process");
		    SparkConf conf= new SparkConf().setAppName("Java Spark").setMaster("local[*]");
		    SparkSession spark = SparkSession
		            .builder()
		            .config(conf)
		            .getOrCreate();
		    spark.sparkContext().setLogLevel("ERROR");
		    
		    StructField[] structFields = new StructField[Header.size()];
		    for (int i = 0; i<this.Header.size(); i++) {
		    	 org.apache.spark.sql.types.DataType dataType = DataTypes.StringType;
		    	 StructField structField = new StructField((String) Header.get(i), dataType, true, Metadata.empty());
		    	 structFields[i] = structField;
		    }
		   
		    StructType schema = new StructType(structFields);
		    
		    for (int i = 0; i<this.Header.size(); i++) {
		    	System.out.println(this.Header.get(i));
		    	String ref = this.Header.get(i).toString();
		    	schema.add(ref,"string");
		    	
		    }

		    
			 Dataset<Row> df = spark.read()
			    		.schema(schema)
			    		.option("mode","dropmalformed")
			    		.option("sep",Separator).format("org.apache.spark.sql.execution.datasources.csv.CSVFileFormat")
			    		.load("hdfs://"+Host+":"+Port+HDFSPath).filter(Filter);
		       
			    df.createOrReplaceTempView(TableRef);

			    df.show();
			    
		}
	
	}
	
	
	public static void main(String[] args) throws Exception {
		List<String> header = Arrays.asList("stock","date","OpenPrice","lowPrice","highPrice","closingPrice","volume");
		Builder sparksession = sparkSession.newSparkSession()
				.Host("127.0.0.1")
				.Port(9000)
				.HDFSPath("/user/xavier/US_Stocks")
				.Header(header)
				.Separator(",")
				.Filter("stock like '%A'")
				.TableRef("Stock");
		
		sparksession.start();
	   
	    
    }
}
