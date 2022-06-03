# Copyright 2022 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""
This Python Script Load the datasets, cleans it
and generates cleaned CSV, MCF, TMCF file
"""
import os
import sys
import pandas as pd
import numpy as np
sys.path.insert(0, 'util')
# from alpha2_to_dcid import COUNTRY_MAP
from absl import app
from absl import flags
 
# pd.set_option("display.max_columns", None)
# pd.set_option("display.max_rows", None)
 
FLAGS = flags.FLAGS
default_input_path = os.path.dirname(
   os.path.abspath(__file__)) + os.sep + "input_file"
flags.DEFINE_string("input_path", default_input_path, "Import Data File's List")
 
def hlth_ehis_ss1e(df: pd.DataFrame) -> pd.DataFrame:
   """
   Cleans the file hlth_ehis_pe9e for concatenation in Final CSV
   Input Taken: DF
   Output Provided: DF
   """
   cols = ['unit,lev_perc,isced11,sex,age,geo', '2019', '2014']
   df.columns=cols
   col1 = "unit,lev_perc,isced11,sex,age,geo"
   df = _split_column(df,col1)
   # Filtering out the wanted rows and columns   
   df = df[df['age'] == 'TOTAL']
   df = df[(df['geo'] != 'EU27_2020') & (df['geo'] != 'EU28')]
   df = _replace_lev_perc(df)
   df = _replace_sex(df)
   df = _replace_isced11(df)
   df['SV'] = 'Count_Person_'+df['isced11']+'_'+ df['lev_perc'] +'_'+df['sex']+\
       '_'+'HealthEnhancingSocialEnvironment_AsAFractionOf_Count_Person_'+\
       df['isced11']+'_'+df['sex']
   df.drop(columns=['unit','age','isced11','lev_perc','sex'],inplace=True)
   df = df.melt(id_vars=['SV','geo'], var_name='time'\
           ,value_name='observation')
   return df
 
def _replace_sex(df:pd.DataFrame) -> pd.DataFrame:
   """
   Replaces values of a single column into true values
   from metadata returns the DF
   """
   _dict = {
       'F': 'Female',
       'M': 'Male',
       'T': 'Total'
       }
   df = df.replace({'sex': _dict})
   return df
 
def _replace_lev_perc(df:pd.DataFrame) -> pd.DataFrame:
   """
   Replaces values of a single column into true values
   from metadata returns the DF
   """
   _dict = {
       'STR' : 'Strong',
       'INT' : 'Intermediate',
       'POOR' : 'Poor'
       }
   df = df.replace({'lev_perc': _dict})
   return df
 
def _replace_isced11(df:pd.DataFrame) -> pd.DataFrame:
   """
   Replaces values of a single column into true values
   from metadata returns the DF
   """
 
   _dict = {
       'ED0-2': 'EducationalAttainment'+\
       'LessThanPrimaryEducationOrPrimaryEducationOrLowerSecondaryEducation',
       'ED3_4': 'EducationalAttainment'+\
           'UpperSecondaryEducationOrPostSecondaryNonTertiaryEducation',
       'ED5-8': 'EducationalAttainmentTertiaryEducation',
       'TOTAL': 'Total'
       }
   df = df.replace({'isced11': _dict})
   return df
 
def _split_column(df: pd.DataFrame,col: str) -> pd.DataFrame:
   """
   Divides a single column into multiple columns and returns the DF
   """
   info = col.split(",")
   df[info] = df[col].str.split(',', expand=True)
   df.drop(columns=[col],inplace=True)
   return df
 
class EuroStatSocialEnvironment:
   """
   This Class has requried methods to generate Cleaned CSV,
   MCF and TMCF Files
   """
   def __init__(self, input_files: list, csv_file_path: str) -> None:
       self.input_files = input_files
       self.cleaned_csv_file_path = csv_file_path
       self.df = None
       self.file_name = None
       self.scaling_factor = 1
 
   def process(self):
       """
       This Method calls the required methods to generate
       cleaned CSV, MCF, and TMCF file
       """
 
       final_df = pd.DataFrame(columns=['time','geo','SV','observation',\
           'Measurement_Method'])
       # Creating Output Directory
       output_path = os.path.dirname(self.cleaned_csv_file_path)
       if not os.path.exists(output_path):
           os.mkdir(output_path)
       sv_list = []
       for file_path in self.input_files:
           print(file_path)
           df = pd.read_csv(file_path, sep='\t',skiprows=1)
           if 'hlth_ehis_ss1e' in file_path:
               df = hlth_ehis_ss1e(df)
           df['SV'] = df['SV'].str.replace('_Total','')
           df['Measurement_Method'] = np.where(df['observation']\
               .str.contains('u'),'LowReliability/EurostatRegionalStatistics',\
               'EurostatRegionalStatistics')
           df['observation'] = df['observation'].str.replace(':','')\
               .str.replace(' ','').str.replace('u','')
           df['observation']= pd.to_numeric(df['observation'], errors='coerce')
           final_df = pd.concat([final_df, df])
           sv_list += df["SV"].to_list()
       final_df = final_df.sort_values(by=['time', 'geo','SV'])
       # final_df = final_df.replace({'geo': COUNTRY_MAP})
       final_df.to_csv(self.cleaned_csv_file_path, index=False)
       sv_list = list(set(sv_list))
       sv_list.sort()
 
def main(_):
   input_path = FLAGS.input_path
   if not os.path.exists(input_path):
       os.mkdir(input_path)
   ip_files = os.listdir(input_path)
   ip_files = [input_path + os.sep + file for file in ip_files]
   data_file_path = os.path.dirname(
       os.path.abspath(__file__)) + os.sep + "output"
   # Defining Output Files
   cleaned_csv_path = data_file_path + os.sep + \
       "EuroStat_Population_SocialEnvironment.csv"
   loader = EuroStatSocialEnvironment(ip_files, cleaned_csv_path)
   loader.process()
if __name__ == "__main__":
   app.run(main)
 

