#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os
import pandas as pd
import numpy as np
import datetime as dt

if not os.path.exists('output'):
    os.mkdir('output')

today1 = dt.date.today().strftime("%d.%m.%Y")
today2 = dt.date.today().strftime("%Y%m%d")

# define and read in partial register files
files_data = os.listdir('data')
files_data

def read_contacts_lic(file):
        raw_file = pd.ExcelFile('data/'+file)
        sheets = raw_file.sheet_names
        contacts = pd.read_excel('data/'+file, sheet_name = sheets[0], dtype = str)
        contacts['file'] = file
        contacts['n'] = contacts.index
        lic = pd.read_excel('data/'+file, sheet_name = sheets[1], dtype = str)
        lic['file'] = file
        lic['n'] = lic.index
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
colnames_contacts = contacts.columns.tolist()
colnames_lic = lic.columns.tolist()
contacts.columns = ["id", "title_short", "as_of", "title_full", "manager",
                     "activity_sector", "activity_type",
                     "oblast_code", "zip_code", "address", "mail", "website",
                     "phone", "fax", "working_area", "working_area_code", "file", "n"]
lic.columns = ["authority", "id", "title_short", "license_id", "activity_sector", "activity_type",
               "license_valid",  "archive_n", "license_n", "reg_type", "comment", 
               "start_date", "stop_date", "end_date", "reg_n", "reg_date", "reg_content", "letters",
               "bank", "payment", "payment_deadline", "payment_info", "file", "n"]

# checks & messages

# id length in [8, 10]

def check_id_length(df):
    ids = df.loc[~df['id'].str.len().isin([8, 10])]['id'].tolist()
    files = df.loc[~df['id'].str.len().isin([8, 10])]['file'].tolist()
    index_ids = df.loc[~df['id'].str.len().isin([8, 10])]['n'] + 2
    problems_log = open("output/problems_log.txt", "a", encoding = 'utf-8')
    if len(ids)>0:
        problems_log.write("Кількість цифр у ЄДРПОУ не дорівнює 8 або 10 у компаній: \n\n")
        for i, j, k in zip(ids, index_ids, files):
            problems_log.writelines(str(i) + " (файл " + k + ", рядок " + str(j) + ")\n")
    else:
        problems_log.write("Кількість цифр у ЄДРПОУ - без помилок\n")
    problems_log.write("\n")
    problems_log.close()

# date format

def check_date_validity(df, var, lic = True):
    na_index = df[var][df[var].isnull()].index.tolist()
    var_str = pd.to_datetime(df[var], format = '%Y-%m-%d', errors = 'coerce').astype(str)
    all_index = var_str[var_str == "NaT"].index.tolist()
    bad_dates = [i for i in all_index if i not in na_index]
    files = df.iloc[bad_dates,:]['file']
    bad_dates_index = df.iloc[bad_dates,:]['n'] + 2
    problems_log = open("output/problems_log.txt", "a", encoding = 'utf-8')
    col_loc = df.columns.get_loc(var)
    if lic == False:
        problems_log.write('Колонка "' + colnames_contacts[col_loc] + '":\n')
    else:
        problems_log.write('Колонка "' + colnames_lic[col_loc] + '":\n')
    if len(bad_dates)>0:
        problems_log.write("Формат дати неправильний у таких файлах і рядках: \n")
        for i, j in zip(files, bad_dates_index):
            problems_log.writelines(i + " - рядок " + str(j) + "\n")
        problems_log.write("\n\n")
    else:
        problems_log.write("Формат дати - без помилок\n\n")
    problems_log.close()

# Execute checks and write them to a log file
problems_log = open("output/problems_log.txt", "w", encoding = 'utf-8')
problems_log.write(dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
problems_log.write("\n\nКОНТАКТИ: \n\n")
problems_log.close()
check_id_length(contacts)
check_date_validity(contacts, 'as_of', lic = False)

problems_log = open("output/problems_log.txt", "a", encoding = 'utf-8')
problems_log.write("\n\nПОСТАНОВИ: \n\n")
problems_log.close()
check_id_length(lic)
check_date_validity(lic, 'start_date', lic = True)
check_date_validity(lic, 'stop_date')
check_date_validity(lic, 'end_date')
check_date_validity(lic, 'reg_date')
problems_log.close()

# check license validity and write the results

lic['reg_date'] = pd.to_datetime(lic['reg_date'], errors = 'coerce').dt.strftime('%Y-%m-%d')
last_regs = lic.groupby(['license_id'])['reg_date'].max().reset_index()
last_regs = pd.merge(last_regs, lic[['license_id', 'id', 'reg_date', 'activity_sector', 'activity_type', 'reg_type', 'end_date', 'license_valid', 'comment']], 
                     on = ['license_id', 'reg_date'], how = 'left')
last_regs.loc[last_regs['reg_type'].str.contains('анулювання'), 'reg_type'] = 'анулювання'
last_regs['end_date'] = pd.to_datetime(last_regs['end_date'], errors = 'coerce')
valid_as_not_valid = last_regs[(last_regs['license_valid']=='не чинна') & (last_regs['reg_type']!='анулювання') & (last_regs['comment']!='зміна законодавства') & ((last_regs['end_date'].isnull())|(last_regs['end_date']>dt.date.today()))]
not_valid_as_valid = last_regs[(last_regs['license_valid']=='чинна') & ((last_regs['reg_type']=='анулювання') | (last_regs['comment']=='зміна законодавства') | (last_regs['end_date']<=dt.date.today()))]
valid_as_not_valid_list = ', '.join(valid_as_not_valid['license_id'])
not_valid_as_valid_list = ', '.join(not_valid_as_valid['license_id'])

lic['reg_date'] = lic['reg_date'].astype(str)
lic['reg_date'][lic['reg_date']=='NaT'] = lic['reg_date'].replace('NaT', np.nan)

problems_log = open("output/problems_log.txt", "a", encoding = 'utf-8')
problems_log.write("\nЛіцензії, які позначені як не чинні, але можуть бути чинними (немає постанови про анулювання, запису про закінчення дії відповідно до закону або відповідної дати закінчення дії):\n")
if(len(valid_as_not_valid_list) > 0):
    problems_log.write(valid_as_not_valid_list)
else:
    problems_log.write('\n - немає')
problems_log.write("\n\nЛіцензії, які позначені як чинні, але можуть бути не чинними (є постанова про анулювання, запис про закінчення дії відповідно до закону або відповідна дата закінчення дії):\n")
if(len(not_valid_as_valid_list) > 0):
    problems_log.write(not_valid_as_valid_list)
else:
    problems_log.write('\n - немає')
problems_log.write('\n\n\n')
problems_log.close()

# select relevant variables for subsequent work

contacts_short = contacts[['id', 'title_short', 'title_full', 'as_of', 'activity_sector', 'activity_type',
                           'zip_code', 'address', 'mail', 'website']]
lic_short = lic[['id', 'license_id', 'activity_sector', 'activity_type', 'license_valid',
           'reg_type', 'reg_n', 'reg_date', 'start_date', 'stop_date', 'end_date']]

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
contacts['as_of'] = contacts['as_of'].astype(str)
contacts['as_of'][contacts['as_of']=='NaT'] = contacts['as_of'].replace('NaT', np.nan)

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
register1['mail'] = register1['mail_x'].fillna(register1['mail_y'])

# reorder and rename columns

register1 = register1[['id', 'title_full', 'activity_sector', 'activity_type',
                       'license_valid', 'reg_type', 'reg_n', 'reg_date',
                       'start_date', 'stop_date', 'end_date', 'zip_code',
                       'address', 'website', 'mail']]

register1['reg_date'][register1['reg_date']=='NaT'] = register1['reg_date'].replace('NaT', np.nan)
register1['end_date'][register1['end_date']=='NaT'] = register1['end_date'].replace('NaT', np.nan)

register1 = register1.drop_duplicates()

register1.columns = ["Код згідно з ЄДРПОУ", "Повне найменування суб’єкта господарювання", 
                     "Сфера діяльності", "Вид діяльності", "Статус ліцензії", "Тип рішення",
                     "Номер рішення", "Дата рішення", "Дата початку дії ліцензії",
                     "Дата зупинення або відновлення дії ліцензії", "Дата закінчення дії ліцензії",
                     "Поштовий індекс", "Юридична адреса суб’єкта господарювання", 
                     "Веб-сайт суб'єкта господарювання", "Електронна адреса суб'єкта господарювання"]

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
register2.columns = ["№ з/п", "Код згідно з ЄДРПОУ", "Повне найменування суб’єкта господарювання", 
                     "Кількість діючих ліцензій*", "Юридична адреса суб’єкта господарювання"]

# change column types for all dataframes
contacts = contacts.astype(str).replace('nan', np.nan)
lic = lic.astype(str).replace('nan', np.nan)

register1 = register1.astype(str).replace('nan', np.nan)

register2 = register2.astype(str).replace('nan', np.nan)
register2['№ з/п'] = register2['№ з/п'].astype(int)

# aggregation

lic_for_aggr = lic[['reg_date', 'activity_sector', 'activity_type', 'reg_type']]
lic_for_aggr['sector_type'] = lic_for_aggr['activity_sector'] + ' / ' + lic_for_aggr['activity_type']
lic_for_aggr.drop(['activity_sector', 'activity_type'], axis = 1, inplace = True)
lic_for_aggr.loc[lic_for_aggr['reg_type'].str.contains('анулювання'), 'reg_type'] = 'анулювання'
lic_for_aggr['reg_type'] = lic_for_aggr['reg_type'].astype('category')
cat_list = lic_for_aggr['reg_type'].cat.categories.to_list()
new_categories = ["зупинення", "відновлення", "звуження", "розширення"]
for cat in new_categories:
    if cat not in cat_list:
        lic_for_aggr['reg_type'] = lic_for_aggr['reg_type'].cat.add_categories(cat)
lic_for_aggr['reg_count'] = 1
aggr = lic_for_aggr.pivot_table(index = ['reg_date', 'sector_type'], columns = 'reg_type', values = 'reg_count', aggfunc = 'sum', dropna = False, fill_value = 0)
aggr = pd.concat([aggr.index.to_frame(), aggr], axis = 1)
aggr.reset_index(drop = True, inplace = True)
aggr['reg_date'] = pd.to_datetime(aggr['reg_date'])
aggr.set_index('reg_date', inplace = True)

monthly = aggr.groupby(['sector_type']).resample('M').sum().reset_index()
monthly['year'] = pd.DatetimeIndex(monthly['reg_date']).year
monthly['month'] = pd.DatetimeIndex(monthly['reg_date']).month
monthly['year'] = monthly['year'].astype(str)
monthly['month'] = monthly['month'].astype(str).str.rjust(2, '0')
monthly[['activity_sector', 'activity_type']] = monthly['sector_type'].str.split(' / ', expand = True)
monthly = monthly[['year', 'month', 'activity_sector', 'activity_type', 'первинна',
                   'анулювання', 'зміна', 'переоформлення', 'відмова',
                   'зупинення', 'відновлення', 'звуження', 'розширення']]
monthly.columns = ["Рік", "Місяць", "Сфера діяльності", "Вид діяльності",
                   "Видано", "Анульовано", "Внесено змін", "Переоформлено", "Відмовлено",
                   "Зупинено", "Відновлено", "Звужено", "Розширено"]
monthly = monthly.sort_values(["Рік", "Місяць", "Сфера діяльності", "Вид діяльності"])

quarterly = aggr.groupby(['sector_type']).resample('Q').sum().reset_index()
quarterly['year'] = pd.DatetimeIndex(quarterly['reg_date']).year
quarterly['quarter'] = pd.DatetimeIndex(quarterly['reg_date']).quarter
quarterly['year'] = quarterly['year'].astype(str)
quarterly['quarter'] = quarterly['quarter'].astype(str)
quarterly[['activity_sector', 'activity_type']] = quarterly['sector_type'].str.split(' / ', expand = True)
quarterly = quarterly[['year', 'quarter', 'activity_sector', 'activity_type', 'первинна',
                   'анулювання', 'зміна', 'переоформлення', 'відмова',
                   'зупинення', 'відновлення', 'звуження', 'розширення']]
quarterly.columns = ["Рік", "Квартал", "Сфера діяльності", "Вид діяльності",
                   "Видано", "Анульовано", "Внесено змін", "Переоформлено", "Відмовлено",
                   "Зупинено", "Відновлено", "Звужено", "Розширено"]
quarterly = quarterly.sort_values(["Рік", "Квартал", "Сфера діяльності", "Вид діяльності"])

# remove working columns + column names for full register
contacts.drop(['file', 'n'], axis = 1, inplace = True)
lic.drop(['file', 'n'], axis = 1, inplace = True)
contacts.columns = ["Код ЄДРПОУ", "Скорочена назва", "Станом на", "Повна назва",  
                             "Посада, ПІБ керівника", "Сектор", "Вид діяльності",
                             "Код області", "Поштовий індекс (юр. адреса)",
                             "Юридична адреса", "Електронна адреса", "Веб-сайт",
                             "Телефон", "Факс", "Місце здійснення діяльності",
                             "Коди областей діяльності"]

lic.columns = ["Орган, що видав ліцензію", "Код ЄДРПОУ", "Скорочена назва", "ID ліцензії",
                        "Сфера діяльності", "Вид діяльності", "Чинність ліцензії",
                        "Архівний номер", "№ бланку ліцензії", "Тип постанови", "Примітка",
                        "Дата початку дії ліцензії", "Дата зупинення/відновлення дії ліцензії", "Дата кінця дії ліцензії",
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
register2_comment = "* Більш детальну інформацію про діючі ліцензії необхідно дивитись в 'Ліцензійному реєстрі НКРЕКП'"
register2_title = 'output/'+today2+'_register_companies.xlsx'
writer = pd.ExcelWriter(register2_title)
register2.to_excel(writer, startrow = 3, index = False)
worksheet = writer.sheets['Sheet1']
worksheet.set_column('A:A', 5)
worksheet.set_column('B:B', 12)
worksheet.set_column('C:C', 50)
worksheet.set_column('D:D', 15)
worksheet.set_column('E:E', 55)
worksheet.merge_range('A1:E1', register2_text)
worksheet.merge_range('A2:E2', register2_comment)
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

# monthly aggregation
monthly_title = 'output/'+today2+'_monthly.xlsx'
writer = pd.ExcelWriter(monthly_title)
monthly.to_excel(writer, index = False)
worksheet = writer.sheets['Sheet1']
worksheet.set_column('A:B', 7)
worksheet.set_column('C:C', 20)
worksheet.set_column('D:D', 25)
worksheet.set_column('E:M', 13)
writer.save()

# quarterly aggregation
quarterly_title = 'output/'+today2+'_quarterly.xlsx'
writer = pd.ExcelWriter(quarterly_title)
quarterly.to_excel(writer, index = False)
worksheet = writer.sheets['Sheet1']
worksheet.set_column('A:B', 7)
worksheet.set_column('C:C', 20)
worksheet.set_column('D:D', 25)
worksheet.set_column('E:M', 13)
writer.save()