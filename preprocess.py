import pyspark
import pandas as pd
import numpy as np
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

spark = pyspark.sql.SparkSession.builder.config("spark.driver.memory", "10g")\
    .config("spark.memory.fraction", 0.8).config("spark.executor.memory", "2g")\
    .config("spark.sql.shuffle.partitions" , "100").appName('test').getOrCreate()


def preprocess_data(spark, number_of_file):
    #sc = spark.sparkContext()
    #put multiple files into hdfs
    # hdfs dfs -put subject* /input
    
    final_df = pd.DataFrame(columns = ['interval', 'stage', 'eeg_Fpz_mean', 'eeg_Pz_Ox_mean',
       'EMG_submental_mean', 'EOG_horizontal_mean', 'Event_marker_mean',
       'Resp_oro_nasal_mean', 'Temp_rectal_mean', 'Night', 'Subject',
       'eeg_Fpz_std', 'eeg_Pz_Ox_std', 'EMG_submental_std',
       'EOG_horizontal_std', 'Event_marker_std', 'Resp_oro_nasal_std',
       'Temp_rectal_std', 'eeg_Fpz_max', 'eeg_Pz_Ox_max',
       'EMG_submental_max', 'EOG_horizontal_max', 'Event_marker_max',
       'Resp_oro_nasal_max', 'Temp_rectal_max', 'eeg_Fpz_min',
       'eeg_Pz_Ox_min', 'EMG_submental_min', 'EOG_horizontal_min',
       'Event_marker_min', 'Resp_oro_nasal_min', 'Temp_rectal_min'])

    for i in range(number_of_file):

        path = "input/file_%s.json"%(i+1)
        #read in data
        sleepDF = spark.read.option("inferSchema", "true").json(path).repartition(10)
        print("succcessfully read the file%s"%(i+1))
        # print schema
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



        # zip stage and timepoints together into a list of tuples: 
        time_stage = list(zip(annot_stage, annot_time))

        # expand stages to account for each timestep
        stage = []
        for i in range(len(time_stage) - 1):
            stage += (time_stage[i + 1][1]-time_stage[i][1])*[time_stage[i][0]]

        # create final dataframe
        data = pd.DataFrame(list(zip(stage, eeg_Fpz_Cz, eeg_Pz_Oz, EMG_submental, EOG_horizontal, Event_marker, Resp_oro_nasal, Temp_rectal)), columns =['stage', 'eeg_Fpz_Cz', 'eeg_Pz_Oz', 'EMG_submental', 'EOG_horizontal', 'Event_marker', 'Resp_oro_nasal', 'Temp_rectal']) 

        # add in night and subject ID
        data['Night'] = int(Night)
        data['Subject'] = int(Subject)

        # remove first row
        data = data[1:]

        # remove last row
        data = data[:-1]

        # create index row and row of every 30 seconds that we'll use to group by
        data['row_cnt'] = (data.index)
        data['interval'] = np.ceil((data.index) / 30)


        ####################################################
        ### remove all intervals with multiple sleep stages
        # group data by timestamp
        unique_stages = data.groupby(['interval']).stage.nunique()
        unique_stages = pd.DataFrame(unique_stages.reset_index(), columns = ['interval', 'stage'])

        # get list of all indices with multiple sleep stages
        multiple_stages = unique_stages[unique_stages.stage > 1]
        multiple_stages = list(multiple_stages['interval'])

        # remove all intervals with multiple sleep stages
        data = data[~data['interval'].isin(multiple_stages)]


        ###############################################################
        ### remove all intervals that don't have a max row count of 30:
        max_row_cnt = data.groupby(['interval']).row_cnt.count()
        max_row_cnt = pd.DataFrame(max_row_cnt.reset_index(), columns = ['interval', 'row_cnt'])

        less_30 = max_row_cnt[max_row_cnt.row_cnt < 30]
        less_30 = list(less_30['interval'])

        # remove all intervals with multiple sleep stages
        data = data[~data['interval'].isin(less_30)]

        data = data.drop('row_cnt', axis=1)


        #########################################
        # group data and get mean of each column:
        data_mean = data.groupby(['interval']).mean()
        columns_mean = ['interval', 'stage', 'eeg_Fpz_mean', 'eeg_Pz_Ox_mean', 'EMG_submental_mean',
                        'EOG_horizontal_mean', 'Event_marker_mean', 'Resp_oro_nasal_mean', 
                        'Temp_rectal_mean', 'Night', 'Subject']
        data_mean = pd.DataFrame(data_mean.reset_index())
        data_mean.columns = columns_mean

        data = data.drop('Night', axis=1)
        data = data.drop('Subject', axis=1)
        data = data.drop('stage', axis=1)


        # group data and get standard deviation of each column:
        data_std = data.groupby(['interval']).std()
        columns_std = ['interval', 'eeg_Fpz_std', 'eeg_Pz_Ox_std', 'EMG_submental_std',
                        'EOG_horizontal_std', 'Event_marker_std', 'Resp_oro_nasal_std', 
                        'Temp_rectal_std']
        data_std = pd.DataFrame(data_std.reset_index())
        data_std.columns = columns_std

        # group data and get max of each column:
        data_max = data.groupby(['interval']).max()
        columns_max = ['interval', 'eeg_Fpz_max', 'eeg_Pz_Ox_max', 'EMG_submental_max',
                        'EOG_horizontal_max', 'Event_marker_max', 'Resp_oro_nasal_max', 
                        'Temp_rectal_max']
        data_max = pd.DataFrame(data_max.reset_index())
        data_max.columns = columns_max

        # group data and get min of each column:
        data_min = data.groupby(['interval']).min()
        columns_min = ['interval', 'eeg_Fpz_min', 'eeg_Pz_Ox_min', 'EMG_submental_min',
                        'EOG_horizontal_min', 'Event_marker_min', 'Resp_oro_nasal_min', 
                        'Temp_rectal_min']
        data_min = pd.DataFrame(data_min.reset_index())
        data_min.columns = columns_min


        mean_std = pd.merge(data_mean, data_std, on='interval', how='inner')
        min_max = pd.merge(data_max, data_min, on='interval', how='inner')

        merged_df = pd.merge(mean_std, min_max, on='interval', how='inner')
        


        final_df = final_df.append(merged_df)

        print("succcessfully processed the file%s"%(i+1))

    return final_df

final_df = preprocess_data(spark, 3)

final_df.to_csv("final_df2.csv")
