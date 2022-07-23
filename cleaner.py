import pandas as pd
import numpy as np
from collections import Counter

roster = pd.read_excel("/Users/thierrybienvenu/Desktop/projects/active_pat_ls_details.xls")
roster = pd.DataFrame(roster)
headers = ['Acct #', 'Last Name', 'First Name', 'DOB', 'Gender', 'Address', 'City', 'State', 'Zip', 'Phone No.', 'Insurance_name', 'Provider_name']
roster.columns = headers
roster = roster.iloc[3:,:]
roster['DOB'] = pd.to_datetime(roster['DOB'], format= '%Y/%m/%d')
roster['Birth_year'] = pd.DatetimeIndex(roster['DOB']).year.astype(str)
roster_index = roster['Last Name'].str.lower() + "_" + roster['First Name'].str.lower()+ "_" + roster['Birth_year']
roster['index'] = roster_index

attribution = pd.read_excel("/Users/thierrybienvenu/Desktop/projects/att_list.xlsx")
att_headers = ["unnamed", 'Active_or_removed', 'last_name', 'first_name', 'middle_name', 'Medicare_Beneficiary_identifier', 'Gender', 'Birth_date', 'Attributed_prior_quarter', 'Risk_score', 'HCC_risk_score', 'Reason_for_removal']
attribution.columns= att_headers
attribution = attribution.drop('unnamed', axis = 1).iloc[1:,:]
attribution['Birth_date'] = pd.to_datetime(attribution['Birth_date'], format= '%m/%d/%Y')
attribution['Birth_year'] = pd.DatetimeIndex(attribution['Birth_date']).year.astype(str)
attr_index = attribution['last_name'].str.lower() + "_" + attribution['first_name'].str.lower() + "_"+ attribution['Birth_year']
attribution['index'] = attr_index

def duplicates_checker(df,column):
    """This function checks duplicate rows based on the value of the column provided"""
    frequency = Counter(list(df[column])).most_common()
    two_plus_appearance = [name for name,freq in frequency if int(freq)>1]
    duplicates = df.loc[df[column].isin(list(two_plus_appearance))]
    return duplicates.sort_values(by = column)

common = [i for i in list(attr_index) if i in list(roster_index)]
print(str(len(common)) + " Patients appear in both files")

in_attr_not_in_roster = [i for i in list(attr_index) if i not in list(roster_index)]
print(str(len(in_attr_not_in_roster)) + " appear in the attribution file but not in the roster")

in_roster_not_in_attr = [i for i in list(roster) if i not in list(attr_index)]
print(str(len(in_roster_not_in_attr)) + " appear in the roster file but in the attribution")

appears_in_both = attribution[attribution['index'].isin(common)]
appears_in_both_excel = appears_in_both.to_excel(r'/Users/thierrybienvenu/Desktop/medicalincs/appears_in_both.xlsx', index = True, header= True)

only_in_roster = roster[roster['index'].isin(in_roster_not_in_attr)]
only_in_roster = only_in_roster.to_excel(r'/Users/thierrybienvenu/Desktop/medicalincs/only_in_roster.xlsx', index = True, header = True)

only_in_attribution = attribution[attribution['index'].isin(in_attr_not_in_roster)]
only_in_attribution = only_in_attribution.to_excel(r'/Users/thierrybienvenu/Desktop/medicalincs/only_in_attribution.xlsx', index = True, header = True)

