#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os
import re
import pandas as pd
from datetime import date

os.chdir("D:/current_docs/Dixi/nkrekp/soft_register_website")
if not os.path.exists('output'):
    os.mkdir('output')

today1 = date.today().strftime("%d.%m.%Y")
today2 = date.today().strftime("%Y%m%d")

# define and read in the latest file
files_data = os.listdir('data')
unique_dates_output = list(set([int(re.findall('\d+', s)[0]) for s in files_data]))
last_date_output = max(unique_dates_output)
raw_file = pd.ExcelFile('data/'+str(last_date_output)+'_register.xlsx')
sheets = raw_file.sheet_names
contacts = pd.read_excel('data/'+str(last_date_output)+'_register.xlsx', sheet_name = sheets[0], dtype = str)
lic = pd.read_excel('data/'+str(last_date_output)+'_register.xlsx', sheet_name = sheets[1], dtype = str)

#change column names to facilitate the work
contacts.columns = ["id", "title_short", "as_of", "title_full", "manager",
                     "activity_sector", "activity_type",
                     "oblast_code", "zip_code", "address", "mail", "website",
                     "phone", "fax", "working_area", "working_area_code"]
lic.columns = ["authority", "id", "title_short", "license_id", "activity_sector", "activity_type",
               "license_valid",  "archive_n", "license_n", "reg_type", "comment", 
               "start_date", "end_date", "stop_date", "reg_n", "reg_date", "reg_content", "letters",
               "bank", "payment", "payment_deadline", "payment_info"]

# remove regulations with changes
lic = lic.loc[lic['reg_type']!='зміна']

# use mail where there is no website
contacts.loc[contacts['website'].isnull(), 'website'] = contacts['mail']

# choose the columns we need
contacts_short = contacts[['id', 'title_full', 'as_of', 'activity_sector', 'activity_type',
                           'zip_code', 'address', 'website']]
lic_short = lic[['id','activity_sector', 'activity_type', 'license_valid',
           'reg_type', 'reg_n', 'reg_date', 'start_date', 'stop_date', 'end_date']]

# replace contacts with the last ones
contacts['as_of'] =  pd.to_datetime(contacts['as_of'], format='%Y-%m-%d')
last_contacts = contacts.groupby(['id', 'activity_sector', 'activity_type'])['as_of'].max().reset_index()
last_contacts['as_of'] = last_contacts['as_of'].dt.strftime('%Y-%m-%d').astype(str)
last_contacts = pd.merge(last_contacts, contacts_short, on = ['id', 'activity_sector', 'activity_type', 'as_of'], how = 'left')
last_contacts = last_contacts.groupby(['id', 'activity_sector', 'activity_type', 'as_of']).last().reset_index()
last_contacts = last_contacts.drop('as_of', axis = 1)

# join the contacts
register1 = pd.merge(lic_short, last_contacts, on = ['id', 'activity_sector', 'activity_type'], 
                    how = 'left')

# reorder and rename columns
register1 = register1[['id', 'title_full', 'activity_sector', 'activity_type',
                       'license_valid', 'reg_type', 'reg_n', 'reg_date',
                       'start_date', 'stop_date', 'end_date', 'zip_code',
                       'address', 'website']]

register1.columns = ["Код ЄДРПОУ", "Повна назва компанії", "Сфера діяльності",
                     "Вид діяльності", "Статус ліцензії", "Тип постанови",
                     "Номер постанови", "Дата постанови", "Дата початку дії ліцензії",
                     "Дата призупинення або відновлення дії ліцензії", "Дата закінчення дії ліцензії",
                     "Поштовий індекс", "Юридична адреса", "Вебсайт"]

# write with additional text
register1_text = "Ліцензійний реєстр Національної комісії, що здійснює державне регулювання у сферах енергетики та комунальних послуг, станом на " + today1
register1_title = 'output/'+today2+'_register_licenses.xlsx'
writer = pd.ExcelWriter(register1_title)
register1.to_excel(writer, startrow = 2, index = False)
worksheet = writer.sheets['Sheet1']
worksheet.set_column('A:A', 12)
worksheet.set_column('B:B', 50)
worksheet.set_column('C:C', 20)
worksheet.set_column('D:D', 30)
worksheet.set_column('E:E', 12)
worksheet.set_column('F:L', 15)
worksheet.set_column('M:M', 40)
worksheet.set_column('N:N', 15)
worksheet.merge_range('A1:N1', register1_text)
writer.save()

# last contacts by company (ignoring activity types)
last_contacts2 = contacts.groupby('id')['as_of'].max().reset_index()
last_contacts2['as_of'] = last_contacts2['as_of'].dt.strftime('%Y-%m-%d').astype(str)
last_contacts2 = pd.merge(last_contacts2, contacts_short, on = ['id', 'as_of'], how = 'left')
last_contacts2 = last_contacts2.groupby(['id', 'as_of']).last().reset_index()
last_contacts2 = last_contacts2.drop('as_of', axis = 1)

# count valid licenses by company
valid_only = lic[lic['license_valid']=='чинна']
license_count = valid_only.groupby(['id', 'license_id']).size().reset_index(name = 'n')
comp_count = license_count.groupby('id').size().reset_index(name = 'n')
register2 = pd.merge(comp_count, last_contacts2[['id', 'title_full', 'zip_code', 'address']], 
                     on = 'id', how = 'left')
register2['nr'] = register2.index+1
register2['nr'] = register2['nr'].astype(str)

# reorder and rename columns
register2 = register2[['nr', 'id', 'title_full', 'n', 'zip_code', 'address']]
register2.columns = ["№", "Код ЄДРПОУ", "Повна назва компанії", "Кількість діючих ліцензій",
                     "Поштовий індекс", "Юридична адреса"]

# write with additional text
register2_text = "Реєстр суб'єктів господарювання, які провадять діяльність у сферах енергетики та комунальних послуг, діяльність яких регулюється НКРЕКП, станом на " + today1
register2_title = 'output/'+today2+'_register_companies.xlsx'
writer = pd.ExcelWriter(register2_title)
register2.to_excel(writer, startrow = 2, index = False)
worksheet = writer.sheets['Sheet1']
worksheet.set_column('A:A', 5)
worksheet.set_column('B:B', 12)
worksheet.set_column('C:C', 50)
worksheet.set_column('D:D', 22)
worksheet.set_column('E:E', 15)
worksheet.set_column('F:F', 40)
worksheet.merge_range('A1:F1', register2_text)
writer.save()