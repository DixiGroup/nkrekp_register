#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os
import pandas as pd
import numpy as np
from datetime import date

if not os.path.exists('output'):
    os.mkdir('output')

today1 = date.today().strftime("%d.%m.%Y")
today2 = date.today().strftime("%Y%m%d")

# define and read in partial register files
files_data = os.listdir('data')
files_data

def read_contacts_lic(file):
        raw_file = pd.ExcelFile('data/'+file)
        sheets = raw_file.sheet_names
        contacts = pd.read_excel('data/'+file, sheet_name = sheets[0], dtype = str)
        lic = pd.read_excel('data/'+file, sheet_name = sheets[1], dtype = str)
        return(contacts, lic)

list_contacts = []
list_lic = []

for f in files_data:
    c_lic  = read_contacts_lic(f)
    list_contacts.append(c_lic[0])
    list_lic.append(c_lic[1])
    
contacts = pd.concat(list_contacts, sort = False).reset_index(drop = True)
lic = pd.concat(list_lic, sort = False).reset_index(drop = True)

# change column names and select the columns needed
contacts.columns = ["id", "title_short", "as_of", "title_full", "manager",
                     "activity_sector", "activity_type",
                     "oblast_code", "zip_code", "address", "mail", "website",
                     "phone", "fax", "working_area", "working_area_code"]
lic.columns = ["authority", "id", "title_short", "license_id", "activity_sector", "activity_type",
               "license_valid",  "archive_n", "license_n", "reg_type", "comment", 
               "start_date", "end_date", "stop_date", "reg_n", "reg_date", "reg_content", "letters",
               "bank", "payment", "payment_deadline", "payment_info"]

contacts_short = contacts[['id', 'title_short', 'title_full', 'as_of', 'activity_sector', 'activity_type',
                           'zip_code', 'address', 'mail', 'website']]
lic_short = lic[['id', 'license_id', 'activity_sector', 'activity_type', 'license_valid',
           'reg_type', 'reg_n', 'reg_date', 'start_date', 'stop_date', 'end_date']]

# use mail where there is no website
contacts_short.loc[contacts_short['website'].isnull(), 'website'] = contacts_short['mail']
contacts_short = contacts_short.drop('mail', axis = 1)

# remove regulations with changes and rename regulation types
lic_short = lic_short.loc[lic_short['reg_type']!='зміна']
lic_short.loc[lic_short['reg_type'] == 'первинна', 'reg_type'] = 'видача'
lic_short.loc[lic_short['reg_type'].str.contains('анулювання'), 'reg_type'] = 'анулювання'

# remove old regulations (reset end dates by annul)
annul = lic_short[lic_short['reg_type'] == 'анулювання']
annul = annul.groupby('license_id').last().reset_index()
annul.loc[annul['end_date'].isnull(), 'end_date'] = annul['reg_date']
annul_date = annul[['license_id', 'end_date']]
annul_date.columns = ['license_id', 'end_date_new']
lic_short = pd.merge(lic_short, annul_date, on = 'license_id', how = 'left')
lic_short.loc[lic_short['end_date'].isnull(), 'end_date'] = lic_short['end_date_new']

lic_short['reg_date'] =  pd.to_datetime(lic_short['reg_date'], format='%Y-%m-%d')
lic_short['end_date'] =  pd.to_datetime(lic_short['end_date'], format='%Y-%m-%d')
lic_short = lic_short[(lic_short['license_valid']=='чинна') | (lic_short['reg_date']>="2017-11-09") | (lic_short['end_date']>='2017-11-09')]
lic_short = lic_short[(lic_short['end_date_new']>='2017-11-09')|lic_short['end_date_new'].isnull()]
lic_short['reg_date'] = lic_short['reg_date'].dt.strftime('%Y-%m-%d').astype(str)
lic_short['end_date'] = lic_short['end_date'].dt.strftime('%Y-%m-%d').astype(str)
lic_short = lic_short.drop(['end_date_new', 'license_id'], axis = 1)

# replace contacts with the last ones
contacts['as_of'] =  pd.to_datetime(contacts['as_of'], format='%Y-%m-%d')
last_contacts = contacts.groupby(['id', 'activity_sector', 'activity_type'])['as_of'].max().reset_index()
last_contacts['as_of'] = last_contacts['as_of'].dt.strftime('%Y-%m-%d').astype(str)
last_contacts = pd.merge(last_contacts, contacts_short, on = ['id', 'activity_sector', 'activity_type', 'as_of'], how = 'left')
last_contacts = last_contacts.groupby(['id', 'activity_sector', 'activity_type', 'as_of']).last().reset_index()
last_contacts = last_contacts.drop('as_of', axis = 1)

# last contacts by company (ignoring activity types)
contacts_short = contacts_short.drop(['activity_sector', 'activity_type'], axis = 1)
contacts_short = contacts_short.sort_values(['id', 'as_of'])
last_contacts2 = contacts_short.groupby('id').last().reset_index()
last_contacts2 = last_contacts2.drop(['as_of', 'title_short'], axis = 1)

# join the contacts
register1 = pd.merge(lic_short, last_contacts, on = ['id', 'activity_sector', 'activity_type'], 
                    how = 'left')

register1 = register1.merge(last_contacts2, on='id', how='left')
register1['title_full'] = register1['title_full_x'].fillna(register1['title_full_y'])
register1['zip_code'] = register1['zip_code_x'].fillna(register1['zip_code_y'])
register1['address'] = register1['address_x'].fillna(register1['address_y'])
register1['website'] = register1['website_x'].fillna(register1['website_y'])

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

# count valid licenses by company
valid_only = lic[lic['license_valid']=='чинна']
license_count = valid_only.groupby(['id', 'license_id']).size().reset_index(name = 'n')
comp_count = license_count.groupby('id').size().reset_index(name = 'n')
register2 = pd.merge(comp_count, last_contacts2[['id', 'title_full', 'address']], 
                     on = 'id', how = 'left')
register2['nr'] = register2.index+1
register2['nr'] = register2['nr'].astype(str)

# reorder and rename columns
register2 = register2[['nr', 'id', 'title_full', 'n', 'address']]
register2.columns = ["№", "Код ЄДРПОУ", "Повна назва компанії", "Кількість діючих ліцензій",
                     "Юридична адреса"]

# change column types for all dataframes
contacts = contacts.astype(str).replace('nan', np.nan)
lic = lic.astype(str).replace('nan', np.nan)

register1 = register1.astype(str).replace('nan', np.nan)

register2 = register2.astype(str).replace('nan', np.nan)
register2['№'] = register2['№'].astype(int)

# column names for full register
contacts.columns = ["Код ЄДРПОУ", "Скорочена назва", "Станом на", "Повна назва",  
                             "Посада, ПІБ керівника", "Сектор", "Вид діяльності",
                             "Код області", "Поштовий індекс (юр. адреса)",
                             "Юридична адреса", "Електронна адреса", "Веб-сайт",
                             "Телефон", "Факс", "Місце здійснення діяльності",
                             "Коди областей діяльності"]

lic.columns = ["Орган, що видав ліцензію", "Код ЄДРПОУ", "Скорочена назва", "ID ліцензії",
                        "Сфера діяльності", "Вид діяльності", "Чинність ліцензії",
                        "Архівний номер", "№ бланку ліцензії", "Тип постанови", "Примітка",
                        "Дата початку дії ліцензії", "Дата кінця дії ліцензії", "Дата призупинення/відновлення дії ліцензії",
                        "№ постанови", "Дата постанови", "Зміст постанови",
                        "Листи ліцензіата про зміни даних в документах, що додавалися до заяви на видачу ліцензії",
                        "Банківські реквізити ліцензіата", "Сума, сплачена за ліцензування",
                        "Граничний строк сплати за видачу ліцензії", "Інформація про сплату"]

# write
# register 1
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

# register 2
register2_text = "Реєстр суб'єктів господарювання, які провадять діяльність у сферах енергетики та комунальних послуг, діяльність яких регулюється НКРЕКП, станом на " + today1
register2_title = 'output/'+today2+'_register_companies.xlsx'
writer = pd.ExcelWriter(register2_title)
register2.to_excel(writer, startrow = 2, index = False)
worksheet = writer.sheets['Sheet1']
worksheet.set_column('A:A', 5)
worksheet.set_column('B:B', 12)
worksheet.set_column('C:C', 50)
worksheet.set_column('D:D', 15)
worksheet.set_column('E:E', 55)
worksheet.merge_range('A1:E1', register2_text)
writer.save()

# full register
contacts_title = 'output/'+today2+'_full_register.xlsx'
writer = pd.ExcelWriter(contacts_title)
contacts.to_excel(writer, index = False, sheet_name = "1. Контактна інформація")
worksheet1 = writer.sheets["1. Контактна інформація"]
worksheet1.set_column('A:A', 10)
worksheet1.set_column('B:B', 20)
worksheet1.set_column('C:C', 12)
worksheet1.set_column('D:D', 30)
worksheet1.set_column('E:E', 30)
worksheet1.set_column('F:F', 20)
worksheet1.set_column('G:G', 20)
worksheet1.set_column('H:I', 12)
worksheet1.set_column('J:J', 30)
worksheet1.set_column('K:L', 18)
worksheet1.set_column('M:N', 10)
worksheet1.set_column('O:O', 25)
worksheet1.set_column('P:P', 18)

lic.to_excel(writer, index = False, sheet_name = '2. Ліцензування')
worksheet2 = writer.sheets['2. Ліцензування']
worksheet2.set_column('A:A', 30)
worksheet2.set_column('B:B', 8)
worksheet2.set_column('C:C', 20)
worksheet2.set_column('D:D', 15)
worksheet2.set_column('E:F', 30)
worksheet2.set_column('G:G', 15)
worksheet2.set_column('H:I', 10)
worksheet2.set_column('J:J', 13)
worksheet2.set_column('K:K', 20)
worksheet2.set_column('L:N', 10)
worksheet2.set_column('O:O', 8)
worksheet2.set_column('P:P', 10)
worksheet2.set_column('Q:Q', 40)
worksheet2.set_column('R:R', 20)
worksheet2.set_column('S:S', 30)
worksheet2.set_column('T:V', 10)
writer.save()