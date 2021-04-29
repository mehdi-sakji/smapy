import pandas as pd                                                                                                                                           
import numpy as np                                                                                                                                            
import uuid                                                                                                                                                   
import os                                                                                                                                                     
import jsonlines                                                                                                                                              
import datetime

def read_raw(crawlies_path):
    raw_df = pd.read_json(crawlies_path)
    return raw_df

def compute_session_df(raw_df):
    session_df = raw_df[raw_df["class"]=="session"].drop(
        columns=[                                                                                                 
            "content_id", "url", "session_order", "session_range", "session_id", "role"])
    return session_df
