
import pandas as pd
import re
import os
import numpy as np
from sqlalchemy import create_engine
from settings import USER, PASSWORD, HOST, PORT, DATABASE

CONNECTION_STRING = f'postgresql://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}'
ENGINE = create_engine(CONNECTION_STRING)


pd.set_option('display.max_rows', 1000)

os.getcwd()
os.chdir('C:\\Users\\Edgar\\Desktop\\Stori\\WBR Data\\SPOC')


#### 1st. Clean date variables and add date
#### (year, month, week, day of week/day #, hour for visualization purposes)

SPOC_INCOMING_TICKETS = open('SPOC_cases_up_2.sql', 'r').read().strip('\n')

spoc_raw = pd.read_sql(SPOC_INCOMING_TICKETS,ENGINE)

###Read incoming contacts raw, 
### spoc_raw = pd.read_csv('SPOC_cases_7.csv')

# Filter contacts that have own user_id (already clean, add phone number later)

spoc_1 = spoc_raw[(spoc_raw['account_id'] != '8c33aec4-6929-0a0d-46dc-5df11bdc8cd5') & spoc_raw['user_id'].notnull()]


# Filter contacts that went to the gral. public account_id bucket, (approx. 18k, or  ~25% of all incoming contacts) and...
# have information on resolution to verify if contact is an actual customer.


spoc_0 = spoc_raw[(spoc_raw['account_id'] == '8c33aec4-6929-0a0d-46dc-5df11bdc8cd5') & spoc_raw['resolution'].notnull()]


# Drop user_id non matched, to be added later on

spoc_0.drop('user_id', axis=1, inplace=True)

### Functions to extract email and phone contacts from resolutions column using regular expressions


# Get mail data


def mail_it(res):
    match = re.search(r'[\w\.]+@[\w\.]+', res)
    if match is not None:
        return match.group(0)
    else:
        return np.NaN


# Get phone data


def phone_it(res):
    match = re.search(r'\d{10}', res)
    if match is not None:
        return match.group(0)
    else:
        return np.NaN


### Apply functions in new column
spoc_0['email'] = spoc_0['resolution'].apply(mail_it)

spoc_0['phone'] = spoc_0['resolution'].apply(phone_it)


spoc_0 = spoc_0[~(spoc_0['email'].isnull() & spoc_0['phone'].isnull())]


### Which cases don't have contact information on resolution column

missing = spoc_0[spoc_0['email'].isnull() & spoc_0['phone'].isnull()]


### Save to csv

missing.to_csv('missing_num.csv')



### Read clients user_id, phone number from whatsapp and email from cca.application_contact_cur / cca.application_master_cur


clients = pd.read_csv('clients_emails.csv')

clients = clients[clients['email'].notnull()]



# In[33]: Make functions for number formatting (classify if # has cc, 
                                        #  remove country code & get local code area )


### Function for flagging Whatsapp numbers registered with country code


def num_fil(row):

    if int(str(row)[:2]) == 52:
        return int(str(row)[:2])
    else:
        return None


### Function that removes Mexico's country code (52) from phone number


def remove_cc(row):
    return str(row)[2:]



### Function that retrieves the state level phone code (first three digits for most cities (and first two for Mexico City (55), Monterrey (81) and Guadalajara (33)))



def get_code(row):
    if ((int(str(row)[:2])== 55) | (int(str(row)== 81)) | ((int(row)== 33))):
        return int(str(row)[:2])
    else:
        return int(str(row)[:3])



### Apply functions to whatsapp number column 

### Check for country code (52)
clients['country_code'] = clients['whatsapp_num'].apply(num_fil)

### Remove country code 
clients['phone'] = np.where(clients['country_code'] == 52.0, clients['whatsapp_num'].apply(remove_cc), clients['whatsapp_num'])

### Get local area code (55/81/33 filtered to select only first two digits)
clients['code'] = clients['phone'].apply(get_code)



# In[42]: Next, compare phone capture quality
# (does it have the format? (xx)xxxx-xxxx / (xxx)xxx-xxxx)

### ladas data from google search phone code table in tpd website,
### look for a better source


#ladas = pd.read_csv('lada_codes.csv')

#clients_ladas = clients.merge(ladas, how='left', left_on='code', right_on='Clave LADA')

#clients_ladas.info()





# In[45]: Clean mail table to merge with scraped emails of account_id's in general public 


clients_mail = clients[['user_id','email']]


# In[46]: Merge email scraped contacts with clients


merge_email = spoc_0.merge(clients_mail,how='left', on='email', indicator=True)


# In[50]: Keep the ones who matched
merge_1 = merge_email[merge_email['_merge'] == 'both']


# In[51]: Get cases that didn't match and has alt contact (phone_contact)


### Get case data that has a phone number but didn't match the customer table because of the email joined table.


missphone = merge_email[(merge_email['_merge']=='left_only') & (merge_email['phone'].notnull())]


missphone = missphone[['case_id','name','agent_name','account_id','date_entered', 'day' , 'date_modified', 'resolution','state','email','phone']]


# Get only clients phones data (already cleansed)

clients_phones = clients[['user_id', 'phone']]


### Merge with contacts' phone number
merge_2 = missphone.merge(clients_phones, how='left', on='phone', indicator=True)

### Keep all the phones that matched

merge_2 = merge_2[merge_2['_merge'] == 'both']



### Concat email and phone matches of general public tables into main user_ids - incoming tickets table


#Merge 1: All incoming spoc contacts kept in general public that         
##        matched a scraped email with  cca tables
#

#Merge: 2 All incoming spoc contacts kept in general public that         
##        matched a scraped phone with clean phone number (whatsapp no w/o country code)
###       @ cca tables



gps =  pd.concat([merge_1,merge_2], ignore_index=True)
gps.drop('_merge', axis = 1, inplace=True)

# Add phone number to mail matches w/o phone contact information

## Filter contacts without phone
gps_phone = gps[gps['phone'].isnull()].drop('phone', axis = 1)

### Merge cca clients phones-user_ids to contacts without phone number
gps_phone = gps_phone.merge(clients_phones, how='left', on='user_id', indicator=True)
gps_phone.drop('_merge', axis = 1, inplace=True)

gps_phone = gps_phone[['case_id', 'name', 'agent_name', 'account_id', 'date_entered',
       'date_modified', 'resolution', 'state', 'email', 'phone','user_id']]

### Filter contact with phone number to gps
gps = gps[gps['phone'].notnull()]



###gps = contacts with phone info
###gps_phone = contacts without phone info matched with cca tables


gps_0 = pd.concat([gps, gps_phone])


### Get final clean clients data

clients_clean  = clients[['user_id', 'phone', 'email']]


# In[137]: gps_1 = Contacts with user_id matched from spoc (to disrupt) cstm table(account_id/id_c)


gps_1 = spoc_1.merge(clients, how = 'left',  on='user_id', indicator=True)

gps_1 = gps_1[['case_id', 'name', 'agent_name','account_id','date_entered', 'day' ,'date_modified','resolution','state','email','phone','user_id']]





# In[144]: FINAL CSV


spoc_final = pd.concat([gps_0,gps_1], ignore_index=True, axis = 0)

spoc_final_cases = spoc_final['case_id'].tolist()


#### First: get previous full table
spoc_final_csv = pd.read_csv('spoc_final_up_2.csv')

### Get all tickets greater than 7 days
spoc_final_csv_1 = spoc_final_csv[~spoc_final_csv['case_id'].isin(spoc_final_cases)]

### Append updated tickets
spoc_final_csv = pd.concat([spoc_final_csv_1, spoc_final], axis = 0, ignore_index=True)

spoc_final_csv  = spoc_final_csv[
    
    ['case_id'
,'name'
,'assigned_user_id'
,'account_id'
,'date_entered'
,'day'
,'date_modified'
,'resolution'
,'state'
,'email'
,'phone'
,'user_id']
    
    ]


##with open('foo.csv', 'a') as f:
##           (spoc_final).to_csv(f, header=False)

### Save to csv

spoc_final_csv.to_csv('spoc_final_up_3.csv')
print('spoc_final_up_3.csv table with incoming contacts  cleansed generated')






