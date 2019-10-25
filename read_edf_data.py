import glob
import numpy as np
from collections import defaultdict
import re
import mne
import json
import os
import pandas as pd

edf_path = "../data/sleep-edf-database-expanded-1.0.0/sleep-cassette"
# edf_path = "../data/sleep-edf-database-expanded-1.0.0/sleep-telemetry"

data = defaultdict()
# pnamestart="ST7"
pnamestart="SC4"
PSGpath = glob.glob(edf_path+"/*PSG.edf")
Hyppath = glob.glob(edf_path+"/*Hypnogram.edf")
print(len(PSGpath))
print(len(Hyppath))

#write file for each subject to a single json file
ind = 0 
my_dict = {}
for ipsgpath in PSGpath:
   [sub,night] = re.match(r".*"+pnamestart+"(\d\d)(\d)", ipsgpath).groups()
   ihyppath = glob.glob(edf_path+"/"+pnamestart+sub+night+"*-Hypnogram.edf")
   raw = mne.io.read_raw_edf(os.path.join(ipsgpath), preload=True, stim_channel='auto', verbose=False)
   annot = mne.read_annotations(ihyppath[0])
   raw.set_annotations(annot)
   events, event_id = mne.events_from_annotations(raw)
   my_dict['Subject'] = sub
   my_dict['Night'] = night
   num_chn = len(raw.ch_names)
   for j in range(num_chn):
       # pick one data point every 100
       my_dict[raw.ch_names[j]] = raw.get_data(j)[0][::100].tolist()
    # add annot, divide 100 to keep consistent
   my_dict['annot_time']= np.int32(np.round(events[:,0]/100)).tolist()
   my_dict['annot_stage']= events[:,2].tolist()
   ind = ind +1
   # with open('../data/ProcessedData/sleep-telemetry/file_%s.json'%ind, 'w') as fp:
   with open('../data/ProcessedData/sleep-cassette/file_%s.json'%ind, 'w') as fp:
    json.dump(my_dict, fp)

