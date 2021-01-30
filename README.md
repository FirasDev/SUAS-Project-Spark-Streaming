Project - Spark Streaming
Realized by : 
		Hachem Ouasli
		Firas Ben Mbarek
		Mohamed Jridi

Under the supervision of :
		Mr. Constantine Pohl

Contents

Introduction
Datasets
Tools
Results
References

Introduction
Spark Streaming

Spark Streaming is an extension of the core Spark API that allows data engineers and data scientists to process real-time data from various sources including (but not limited to) Kafka, Flume, and Amazon Kinesis. This processed data can be pushed out to file systems, databases, and live dashboards.
Spark provides us with two ways of working with streaming data:
Spark Streaming
Structured Streaming (introduced with Spark 2.x)
From the Spark 2.x release onwards, Structured Streaming came into the picture. Built on the Spark SQL library, Structured Streaming is another way to handle streaming with Spark. This model of streaming is based on Dataframe and Dataset APIs. Hence, with this library, we can easily apply any SQL query (using the DataFrame API) on streaming data.

In this project we have opted to implement the Structured Streaming API hence the latest versions of pyspark ( Spark using python ) deprecated the DStream API.
In order to stream the data into Spark and treat it like real-time we have tried two different methods :
Dividing the dataset file into smaller parts and have Spark read from that folder
Use Kafka to produce data row by row and have it consumed by the API.
Kafka

“Apache Kafka is an open-source stream-processing software platform developed by the Apache Software Foundation, written in Scala and Java. The project aims to provide a unified, high-throughput, low-latency, publish-subscribe messaging system for handling real-time data feeds.”

II.   Datasets


Dataset ‘’ Political conflicts in Africa from 1997-2018 ‘’ 
	 	 	
The raw dataset presented here contains information on all conflicts that happened in Africa from 1997 to 2020 from ACLED in .xlsx format. ACLED collects the dates, actors, locations, fatalities, and types of all reported political violence and protest events across Africa.
Following our project’s use case the dataset has been converted to .csv format and cleaned to the following parameters ( the cleaning algorithm is presented with the files ) : 	 	 	
Size: 63.5MB
Access: Open
Quality: Clean with some missing values
Type: 	Organization
Columns: 17
Data columns :
ISO : country numeric ISO code
EVENT_DATE : data of the event yyyy-MM-dd
EVENT_TYPE : the type of the violent event from 6 types
SUB_EVENT_TYPE : the type of the sub-event 25 types
ACTOR1 : a named actor involved in the event
ACTOR2 : a named actor involved in the event
INTERACTION : A numeric code indicating the interaction between types of ACTOR1 and ACTOR2.
REGION : The region of the event
COUNTRY : The country in which the event took place
ADMIN 1 : The largest sub-national administrative region in which the event took place.
ADMIN 2 : The second largest sub-national administrative region in which the event took place.
LOCATION : The location in which the event took place.
SOURCE : The source(s)used to code the event.
NOTES : A short description of the event
FATALITIES : Number or estimate of fatalities due to event. These are frequently different across reports.


Dataset ‘’ Breast Cancer Winsconsin ‘’ 

Description :
Features are computed from a digitized image of a fine needle aspirate (FNA) of a breast mass. They describe characteristics of the cell nuclei present in the image. n the 3-dimensional space is that described in: [K. P. Bennett and O. L. Mangasarian: "Robust Linear Programming Discrimination of Two Linearly Inseparable Sets", Optimization Methods and Software 1, 1992, 23-34].
The dataset contains null values and has been cleaned ( the cleaning algorithm is presented with the files ) : 	 	 	
Size: 49KB
Access: Open
Quality: Clean with some null values
Type: 	Organization
Columns: 32
 
Fields include:
Diagnosis (M = malignant, B = benign)
Ten real-valued features are computed for each cell nucleus:
 radius (mean of distances from center to points on the perimeter)
 texture (standard deviation of gray-scale values)
 perimeter
 area
 smoothness (local variation in radius lengths)
 compactness (perimeter^2 / area - 1.0)
 concavity (severity of concave portions of the contour)
 concave points (number of concave portions of the contour)
 symmetry
 fractal dimension ("coastline approximation" - 1)
 
 
Dataset ‘’ Novel Coronavirus 2019 ‘’ 
Description :
This dataset has daily level information on the number of affected cases, deaths and recovery from 2019 novel coronavirus. Please note that this is a time series data and so the number of cases on any given day is the cumulative number.
The data is available from 22 Jan, 2020.
The dataset contained null values and has been cleaned (the cleaning algorithm is presented with the files): 	
Size: 12.256 MB
Access: Open
Quality: Clean with some missing values - String values changed to Unknown
Type: 	Organization
Columns: 9
Fields include:
Sno - Serial number.
ObservationDate - Date of the observation in MM/DD/YYYY.
Province/State - Province or state of the observation (Could be empty when missing).
Country/Region - Country of observation.
Last Update - Time in UTC at which the row is updated for the given province or country. (Not standardised and so please clean before using it).
Confirmed - Cumulative number of confirmed cases till that date.
Deaths - Cumulative number of of deaths till that date.
Recovered - Cumulative number of recovered cases till that date.
Active_Cases - Cumulative number of active cases till that date.



III.  Tools

Kafka setup :
“The default port for Kafka is 9091 and for Zookeeper 2181”

The supported OS in the following steps is Arch Linux :
Download the package kafka from AUR ( Arch User Repository )
To run kafka server use : sudo systemctl start kafka

For Windows:
Download and extract ZooKeeper using 7-zip from Apache ZooKeeper™ Releases
Download and extract Kafka using 7-zip from http://kafka.apache.org/downloads.html
Go to C:\zookeeper-3.6.2\conf
Rename file “zoo_sample.cfg” to “zoo.cfg”
Open zoo.cfg then find and edit dataDir=/tmp/zookeeper to :\zookeeper-3.6.2\data
Add ZOOKEEPER_HOME = C:\zookeeper-3.6.2 to the System Variables
Edit the System Variable named “Path” and add ;%ZOOKEEPER_HOME%\bin;
Go to C:\kafka\config
Edit the file “server.properties.”
Find and edit the line log.dirs=/tmp/kafka-logs” to “log.dir= C:\kafka\kafka-logs.


Now PySpark needs the Kafka libraries to run :

spark-streaming-kafka-0-10-assembly_2.12-3.0.0.jar
https://repo1.maven.org/maven2/org/apache/spark/spark-streaming-kafka-0-10-assembly_2.12/3.0.0/spark-streaming-kafka-0-10-assembly_2.12-3.0.0.jar
spark-sql-kafka-0-10_2.12-3.0.0.jar
https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.0.0/spark-sql-kafka-0-10_2.12-3.0.0.jar
commons-pool2-2.8.0.jar
https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.8.0/commons-pool2-2.8.0.jar
kafka-clients-0.10.2.2.jar
https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/0.10.2.2/kafka-clients-0.10.2.2.jar

Run PySpark using this shell command :

PYSPARK_PYTHON=python3 $SPARK_HOME/bin/pyspark --jars spark-sql-kafka-0-10_2.12-3.0.0.jar,spark-streaming-kafka-0-10-assembly_2.12-3.0.0.jar,kafka-clients-0.10.2.2.jar,commons-pool2-2.8.0.jar



IV.  Results
Africa’s Political Conflicts
the Kafa topic was created using this command
kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic conflicts

to test a producer on this topic
kafka-console-producer.sh --broker-list localhost:9092 --topic conflicts
you can visualize the data being streamed by creating a consumer
kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic conflicts --from-beginning


The python program of the producer is provided in the file “conflicts_kafka_publisher.ipynb”
We can visualize the data being streamed in the kafka consumer


**Kafka has its own data parameters where the actual consumed data is in the “value” parameter along with other parameters. We have transformed all this Kafka specific data type into a new type corresponding to our dataset value and keeping the “timestamp” column from Kafka which represents the time where the data was pushed to the stream.

Count events by Region :



Sum fatalities by Interaction :

Count violence against Civilians ( Interaction containing “2” ) by month

Count political militia events ( Interaction “3” ) by year :

Windowing : ( with Watermarking )









Breast Cancer Wisconsin
Dividing the datasets into parts in order to stream the data :

Reading one part of the data and grouping it by diagnosis :

Displaying the data schema :

Selecting the diagnosis count :
	batch 1 & 2 of the dataset :

		batch 49 & 50 of the dataset :

Checking if the stream is still active :

Selecting the radius mean :








Selecting the radius worst :







Novel Coronavirus 2019
Dividing the datasets into parts in order to stream the data :

Reading one part of the data and grouping it by country :

 Displaying the data schema :





Selecting The confirmed cases : 



Selecting the deaths in each country: 
 

Selecting the recovered cases : 

Checking if the stream is still active :


V.  References 
Breast Cancer Wisconson 
https://www.kaggle.com/uciml/breast-cancer-wisconsin-data
Novel Coronavirus 2019
https://www.kaggle.com/sudalairajkumar/novel-corona-virus-2019-dataset
Africa’s Political Conflicts
https://acleddata.com
In order to obtain the dataset you need to create an account and verify your keys.
To have more information about the columns visit this link :
https://acleddata.com/resources/general-guides/#1603120929112-8ecf0356-6cf0


