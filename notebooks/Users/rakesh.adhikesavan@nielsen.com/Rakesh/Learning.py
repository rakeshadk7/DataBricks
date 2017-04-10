# Databricks notebook source
import this

# COMMAND ----------

from difflib import SequenceMatcher
m = SequenceMatcher(None, "YANKEES", "NEW YORK YANKEES")
print "Ratio 1: " , int(round(m.ratio(),2) * 100)
m = SequenceMatcher(None, "NEW YORK METS", "NEW YORK YANKEES")
print "Ratio 2: " , int(round(m.ratio(),2) * 100)

# COMMAND ----------

from fuzzywuzzy import fuzz

print "Ratio 1: " , fuzz.ratio("YANKEES", "NEW YORK YANKEES")
print "Ratio 2: " , fuzz.ratio("NEW YORK METS", "NEW YORK YANKEES")


# COMMAND ----------

print fuzz.ratio("Bachelor", "The Bachelor")
print fuzz.ratio("Bachelor", "The Bachelorette")

# COMMAND ----------

prog_selection_csv = "/mnt/dsci_s3/users/grifme01/POC_Test_Program_List.csv"

#Location of the program data
program_source_path = "dbfs:/mnt/dsci_s3/projects/MediaAnalytics/TelecastDim_v3"

#Location of the promo data
promo_source_path = "dbfs:/mnt/dsci_s3/projects/MediaAnalytics/AdScheduleParquet5nets.gz.parquet/"

selection_input = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("delimiter", ',').load(prog_selection_csv)
display(selection_input)

# COMMAND ----------

from fuzzywuzzy import process
programs = [ item.Program for item in selection_input.select('Program').distinct().collect()]
programs.append("The Blacklist")

print process.extractOne("Blacklist", programs, scorer = fuzz.partial_ratio)



# COMMAND ----------

import numpy as np
import pandas as pd

dict_of_dataframes = {} #Empty Dictionary

months = ['Jan', 'Feb', 'Mar'] #list of months, sample

for month in months: #Loop over all months
  dict_of_dataframes[month] = pd.DataFrame(np.random.randn(6,4),columns=list('ABCD'))   #Assign Dict[key] = value where key is the month name and value is a random dataframe 
  
print dict_of_dataframes['Jan'] #refer to df of particular month


'''
To iterate over all the DataFrames stored in the dictionary, you can use this code:

for key, value in dict_of_dataframes.iteritems(): 
  month = key
  df = value
  #Write df to location
'''

# COMMAND ----------

lcl = locals() 
lcl['january'] = pd.DataFrame(np.random.randn(6,4),columns=list('ABCD'))

print january

# COMMAND ----------

dbutils.widgets.help()

# COMMAND ----------

import platform
print platform.system()

# COMMAND ----------

dbutils.widgets.combobox("X", "1", [str(x) for x in range(1, 10)])