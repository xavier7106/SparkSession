# SparkSession
SparkSessionExtractFromHDFS

arguments are:
-SparkMaster local[*] ==> the spark master reference
-HDFShost 127.0.0.1 ==> the HADOOP Host
-port 9000 ==> the HADOOP port
-hdfsPath /user/xavier/US_Stocks   ==> HADOOP HDFS folder
-separator ,  ==> the file separator
-filter "stock like '%A'"   ==> the spark Filter
-tableName stock  ==> Spark dataset reference
-header "stock,date,OpenPrice,LowPrice,highPrice,closingPrice,volume" ==> the Spark dataset columns
