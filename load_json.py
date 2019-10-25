from __future__ import print_function
from pyspark.sql import SparkSession
from pyspark.sql import Row

def json_dataset_example(spark, number_of_file):
    sc = spark.sparkContext
    # put json data to hdfs first 
    # hdfs dfs -put data_st.json /input

    #put multiple files into hdfs
    # hdfs dfs -put subject* /input
    # update to your path
    # path = "/input/data_st.json"

    # 
    for i in range(number_of_file):
        path = "/input/subject_%s.json"%i
        #read in data
        sleepDF = spark.read.option("inferSchema", "true").json(path).repartition(10)
        # print schema
        sleepDF.printSchema()
        sleepDF.createOrReplaceTempView("sleep_subject_%s"%i)
        test = spark.sql("SELECT * FROM sleep_subject_%s"%i)
        sleep_stage = test.select('annot_stage').collect()[0][0]
        eeg_Fpz_Cz = test.select('EEG Fpz-Cz').collect()[0][0]
        eeg_Pz_Oz = test.select('EEG Pz-Oz').collect()[0][0]
        EMG_submental = test.select('EMG submental').collect()[0][0]
        EOG_horizontal = test.select("EOG horizontal").collect()[0][0]
        Event_marker = test.select("Event marker").collect()[0][0] 
        Resp_oro_nasal =  test.select("Resp oro-nasal").collect()[0][0]
        Temp_rectal = test.select("Temp rectal").collect()[0][0]
        annot_stage = test.select("annot_stage").collect()[0][0]
        annot_time = test.select("annot_time").collect()[0][0]
        Night = test.select("Night").collect()[0][0]
        Subject  = test.select("Subject").collect()[0][0]

    # for i in range(number_of_file):
    #     # get the ith file's sleep stage/channel etc
    #     sleep_stage = test.select('%s.annot_stage'%i).collect()[0][0]
    #     eeg_Fpz_Cz = test.select('%s.EEG Fpz-Cz'%i).collect()[0][0]
    #     eeg_Pz_Oz = test.select('%s.EEG Pz-Oz'%i).collect()[0][0]
    #     EMG_submental = test.select('%s.EMG submental'%i).collect()[0][0]
    #     EOG_horizontal = test.select("%s.EOG horizontal"%i).collect()[0][0]
    #     Event_marker = test.select("%s.Event marker"%i).collect()[0][0] 
    #     Resp_oro_nasal =  test.select("%s.Resp oro-nasal"%i).collect()[0][0]
    #     Temp_rectal = test.select("%s.Temp rectal"%i).collect()[0][0]
    #     annot_stage = test.select("%s.annot_stage"%i).collect()[0][0]
    #     annot_time = test.select("%s.annot_time"%i).collect()[0][0]
    #     Night = test.select("%s.Night"%i).collect()[0][0]
    #     Subject  = test.select("%s.Subject"%i).collect()[0][0]
        
if __name__ == "__main__":
    # spark = SparkSession \
    #     .builder \
    #     .appName("Python Spark SQL data source example") \
    #     .getOrCreate()

    spark = pyspark.sql.SparkSession.builder.config("spark.driver.memory", "10g")
        .config("spark.memory.fraction", 0.8).config("spark.executor.memory", "2g")
        .config("spark.sql.shuffle.partitions" , "100").appName('test').getOrCreate()

    json_dataset_example(spark)



