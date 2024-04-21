import asyncio
import logging
import sys
import os
import pandas as pd
from aiogram import Bot, Dispatcher, Router, types
from dotenv import load_dotenv
from aiogram.enums import ParseMode
from aiogram.filters import CommandStart
from aiogram.types import Message
from aiogram.utils.markdown import hbold
from aiogram import F
from google.oauth2 import service_account
from googleapiclient.discovery import build
import time
from datetime import datetime,timedelta
from collections import defaultdict

load_dotenv()

SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
SERVICE_ACCOUNT_FILE = 'creds.json'
creds = service_account.Credentials.from_service_account_file(
    SERVICE_ACCOUNT_FILE, scopes=SCOPES)
service = build('sheets', 'v4', credentials=creds)
spreadsheet_id = os.getenv("SPREADSHEET_ID")
TOKEN = os.getenv("TOKEN_TG_BOT")


dp = Dispatcher()

def conver_to_cell(column_num, row_num):
    print("пришло", column_num, row_num)
    column_num += 1
    power = 26
    if column_num > 26:
        dro = (column_num -1) // power

        bro = (column_num -1) % power

        column_name = (f"{chr(dro+64)}{chr(bro+65)}{row_num}:{chr(dro+64)}{chr(bro+65)}")
        print("ушло", column_name)
        return column_name
    else:
        column_name = (f"{chr(column_num+64)}{row_num}:{chr(column_num+64)}")
        print("ушло", column_name)
        return column_name


def current_date_to_table_adress(values):
    now = datetime.now() - timedelta(days=1)
    current_datetime = now.strftime('%d.%m.%Y')

    i = 0
    for object in values:
        i+= 1
        if object[0] == current_datetime:
            return i
def add_freon_to_sheet(freon_data):
    sheet = service.spreadsheets()
    time.sleep(1)
    try:
        result = sheet.values().get(
            spreadsheetId=spreadsheet_id, range='A:ZZ').execute()
        values = result.get('values', [])
        data_row = current_date_to_table_adress(values)

        for index,value in enumerate(values[0]):
            if index+1>= len(values[0]) and value != f"{freon_data['Объект']} / {freon_data['Устройство']}":
                range_new_column = conver_to_cell(len(values[0]),1)
                object = {
                    'values': [[
                        f"{freon_data['Объект']} / {freon_data['Устройство']}"
                    ]]
                }
                print(object)
                result = sheet.values().append(
                    spreadsheetId=spreadsheet_id, range=range_new_column,
                    valueInputOption='USER_ENTERED', body=object).execute()


                range_current_column = conver_to_cell(len(values[0]),data_row)



                body = {
                    'values': [[
                        f"{freon_data['total']}({freon_data['average_count']})",
                    ]]
                }
                result = sheet.values().append(
                    spreadsheetId=spreadsheet_id, range=range_current_column,
                    valueInputOption='USER_ENTERED', body=body).execute()
                return 0
            elif value == f"{freon_data['Объект']} / {freon_data['Устройство']}":
                range_current_column = conver_to_cell(index,data_row)

                body = {
                    'values': [[
                        f"{freon_data['total']}({freon_data['average_count']})",
                    ]]
                }

                result = sheet.values().append(
                    spreadsheetId=spreadsheet_id, range=range_current_column,
                    valueInputOption='USER_ENTERED', body=body).execute()
                return 0
    except Exception as e:
        print('An error occurred:', str(e))
        time.sleep(60)
        # Попытка повторного выполнения функции
        add_freon_to_sheet(freon_data)

def csv_reader(file_path):
    hd = pd.read_csv(file_path, engine="python", encoding="cp1251", delimiter=';')

    hd["total"] = pd.to_datetime(hd["Время устранения на устройстве"], format='%d.%m.%Y %H:%M:%S',
                                 dayfirst=True) - pd.to_datetime(hd["Время регистрации на устройстве"],
                                                                 format='%d.%m.%Y %H:%M:%S', dayfirst=True)
    subset = hd[hd['Описание'] == 'GA1 - Liquid Level Alarm'][['Объект', 'Устройство', 'Описание', 'total']]

    result = []
    for index, row in subset.iterrows():
        data = {
            "Объект": row["Объект"],
            "Устройство": row["Устройство"],
            "Описание": row["Описание"],
            "total": row["total"].total_seconds()
        }
        # add_freon_to_sheet(data)
        result.append(data)

    data_statistics = defaultdict(lambda: {'totals': [], 'count': 0})

    # Заполняем словарь списками total и считаем количество аварий для каждой комбинации
    for item in result:
        key = (item['Объект'], item['Устройство'], item['Описание'])
        data_statistics[key]['totals'].append(item['total'])
        data_statistics[key]['count'] += 1  # Увеличиваем счетчик аварий

    # Считаем среднее значение total для каждой комбинации и создаем новый элемент для хранения этой информации
    result = [{'Объект': key[0], 'Устройство': key[1], 'Описание': key[2],
               'total': '{:02d}:{:02d}:{:02d}'.format(
                   int(sum(data_statistics[key]['totals']) / len(data_statistics[key]['totals']) // 3600),
                   int((sum(data_statistics[key]['totals']) / len(data_statistics[key]['totals']) % 3600) // 60),
                   int((sum(data_statistics[key]['totals']) / len(data_statistics[key]['totals']) % 3600) % 60)),
               'average_count': data_statistics[key]['count']} for key in data_statistics]

    for i in result:
        add_freon_to_sheet(i)

def xls_reader(file_path):

    hd = pd.read_excel(file_path)

    hd["total"] = pd.to_datetime(hd["время окончания"]) - pd.to_datetime(hd["время старта"])

    subset = hd[(hd['описание'] == 'L1 - Liquid level alarm') | (hd['описание'] == 'Низк.ур.жидк.в ресивере')][['объект', 'устройство', 'описание', 'total']]

    result = []
    for index, row in subset.iterrows():
        data = {
            "Объект": row["объект"],
            "Устройство": row["устройство"],
            "Описание": row["описание"],
            "total": row["total"].total_seconds()
        }
        # add_freon_to_sheet(data)
        result.append(data)

    data_statistics = defaultdict(lambda: {'totals': [], 'count': 0})

    # Заполняем словарь списками total и считаем количество аварий для каждой комбинации
    for item in result:
        key = (item['Объект'], item['Устройство'], item['Описание'])
        data_statistics[key]['totals'].append(item['total'])
        data_statistics[key]['count'] += 1  # Увеличиваем счетчик аварий

    # Считаем среднее значение total для каждой комбинации и создаем новый элемент для хранения этой информации
    result = [{'Объект': key[0], 'Устройство': key[1], 'Описание': key[2],
               'total': '{:02d}:{:02d}:{:02d}'.format(
                   int(sum(data_statistics[key]['totals']) / len(data_statistics[key]['totals']) // 3600),
                   int((sum(data_statistics[key]['totals']) / len(data_statistics[key]['totals']) % 3600) // 60),
                   int((sum(data_statistics[key]['totals']) / len(data_statistics[key]['totals']) % 3600) % 60)),
               'average_count': data_statistics[key]['count']} for key in data_statistics]

    for i in result:
        add_freon_to_sheet(i)
@dp.message(CommandStart())
async def command_start_handler(message: Message) -> None:
    """
    This handler receives messages with `/start` command
    """
    asart = r"""
              .---. .---. 
             :     : o   :    Отдавай отчетики по фриону!
         _..-:   o :     :-.._    /
     .-''  '  `---' `---' "   ``-.    
   .'   "   '  "  .    "  . '  "  `.  
  :   '.---.,,.,...,.,.,.,..---.  ' ;
  `. " `.                     .' " .'
   `.  '`.                   .' ' .'
    `.    `-._           _.-' "  .'  .----.
      `. "    '"--...--"'  . ' .'  .'  o   `.
      .'`-._'    " .     " _.-'`. :       o  :
bot .'      ```--.....--'''    ' `:_ o       :
  .'    "     '         "     "   ; `.;";";";'
 ;         '       "       '     . ; .' ; ; ;
;     '         '       '   "    .'      .-'
'  "     "   '      "           "    _.-'
    
    """

    await message.answer(f'''Привет, {hbold(message.from_user.full_name)}!
<code>
{asart}
</code>

''')




@dp.message(F.document)
async def download_doc(message: Message, bot: Bot):
    document = message.document
    file_path = await bot.download(document)
    file_extension = document.mime_type
    print(file_extension)

    if file_extension == 'text/csv':
        csv_reader(file_path)
    elif file_extension in ['xls', 'xlsx','application/vnd.openxmlformats-officedocument.spreadsheetml.sheet']:
        xls_reader(file_path)
    else:
       await message.answer("Неверный формат файла, если это перекресток раскодируйте его через Google таблицы")

    asart = r"""
             _/o\/ \_
    .-.   .-` \_/\o/ '-.
   /:::\ / ,_________,  \
  /\:::/ \  '. (:::/  `'-;
  \ `-'`\ '._ `"'"'\__    \
   `'-.  \   `)-=-=(  `,   |
Еще!   \  `-"`      `"-`   /
    """


    await message.answer(f'''
    Готово, жду следующий файл"
   <code>
            {asart}   
  </code>''')


async def main() -> None:
    # Initialize Bot instance with a default parse mode which will be passed to all API calls
    bot = Bot(TOKEN, parse_mode=ParseMode.HTML)

    # And the run events dispatching
    await dp.start_polling(bot)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, stream=sys.stdout)
    asyncio.run(main())