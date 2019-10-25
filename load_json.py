import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import Row

def json_dataset_example(spark, number_of_file):
    sc = spark.sparkContext
    #put multiple files into hdfs
    # hdfs dfs -put subject* /input
    for i in range(number_of_file):
            # update to your path
        path = "/input/file_%s.json"%(i+1)
        #read in data
        sleepDF = spark.read.option("inferSchema", "true").json(path).repartition(10)
        print("succcessfully read the file%s"%(i+1))
        # print schema
        sleepDF.printSchema()
        sleepDF.createOrReplaceTempView("sleep_file_%s"%(i+1))
        test = spark.sql("SELECT * FROM sleep_file_%s"%(i+1))
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

        
if __name__ == "__main__":
    spark = pyspark.sql.SparkSession.builder.config("spark.driver.memory", "10g")\
        .config("spark.memory.fraction", 0.8).config("spark.executor.memory", "2g")\
        .config("spark.sql.shuffle.partitions" , "100").appName('test').getOrCreate()

    json_dataset_example(spark,38)



