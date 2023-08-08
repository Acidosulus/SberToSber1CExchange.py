import time
import datetime
import os
import pprint
import logging
from  stek import *
from click import echo, style  
import pandas
import os

logging.basicConfig(level=logging.INFO, filename="convert.log",filemode="a", format="%(asctime)s %(levelname)s %(message)s")

printer = pprint.PrettyPrinter(indent=12, width=160)
prnt = printer.pprint


def str_to_file(file_path:str, st:str):
	file = open(file_path, mode='w', encoding='cp1251')
	file.write(st)
	file.close()


def file_to_str(file_path:str):
	with open(file_path, "r", encoding="cp1251") as myfile:
		data = ' '.join(myfile.readlines())
	myfile.close()
	return data   



def sx(source_string='', left_split='', right_split='', index=1):
	if source_string.count(
			left_split) < index:
		return ""
	lc_str = source_string
	for i in range(0, index):
		lc_str = lc_str[lc_str.find(left_split) + len(left_split):len(lc_str)]
	return lc_str[0:lc_str.find(right_split)]



class SberFile():
	def __init__(self, path:str, taget_path:str):
		logging.info(f"Loading agreements data from STEK")
		self.agreements_data = Get_list_of_agreements_details()
		logging.info(f"Loading data from file:{path}")
		self.path = path
		self.taget_path = taget_path
		self.acc = ('40702810338000152290' if 'SB812' in path else ('40702810738000152366' if 'SB811' in path else ''))
		self.header_pattern = ''
		self.date_from_file = sx(path[::-1],'txt.','_')[::-1]
		self.data = self.LoadSberFile()
		self.SaveSber1CExchange()
		self.pay_order_number = ''
		self.lastrow = ''

	def LoadSberFile(self) -> list:
		result = []
		file_raw_data = file_to_str(self.path)
		lines = file_raw_data.splitlines()
		for counter, line in enumerate(lines):
			if line[1] == '=' or line[0] == '+':
					self.pay_order_number = sx(line,';',';',4)
					self.lastrow = line
					logging.info(f'Pay order number for all is:{self.pay_order_number}')
					break
			llines = line.split(';')
			result.append({	'date'									:llines[0].strip(),
		  					'time'									:llines[1].strip(),
							'department'							:llines[2].strip(),
							'cacher'								:llines[3].strip(),
							'suip'									:llines[4].strip(),
							'nc'									:llines[5].strip(),
							'fio'									:llines[6].strip(),
							'address'								:llines[7].strip(),
							'period'								:llines[8].strip(),
							'transaction'							:llines[9].strip().replace(',','.'),
							'total'									:llines[10].strip().replace(',','.'),
							'comission'								:llines[11].strip().replace(',','.'),
							})
		
		try:
			self.header_pattern = f"""1CClientBankExchange
ВерсияФормата=1.03
Кодировка=Windows
Отправитель=Сбербанк Бизнес Онлайн
Получатель=
ДатаСоздания={self.date_from_file[0:2]}.{self.date_from_file[2:4]}.{self.date_from_file[4:8]}
ВремяСоздания=00:00:10
ДатаНачала={result[0]['date'].replace('-','.')}
ДатаКонца={result[0]['date'].replace('-','.')}
РасчСчет={self.acc}
СекцияРасчСчет
ДатаНачала={result[0]['date'].replace('-','.')}
ДатаКонца={result[0]['date'].replace('-','.')}
НачальныйОстаток=0.00
РасчСчет={self.acc}
ВсегоСписано=0.00
ВсегоПоступило=0.00
КонечныйОстаток=0.00
КонецРасчСчет"""
		except:logging.error("Header create error",exc_info=True)
		logging.info(f"Loaded rows:{len(result)}")
		return result

	def SaveSber1CExchange(self):
		if len(self.data)<0:
			logging.info(f"No data to save")
			return
		path = self.taget_path + os.path.basename(self.path) + '_1CExchange.txt'
		logging.info(f"Saving data to file:{path}")
		result = self.header_pattern+'\n'
		ll_array = []
		for counter, row in enumerate(self.data):
			agrement =get_details_from_STEK_by_agreement_number(self.agreements_data, row['nc'])
			ll_array.append([	row['date'],
		    					row['time'],
								row['department'],
								row['cacher'],
								row['suip'],
								row['nc'],
								row['fio'],
								row['address'],
								row['period'],
								float(row['transaction']),
								float(row['total']),
								float(row['comission'])])
			result += f"""СекцияДокумент=Платежное поручение
Номер={self.pay_order_number}_{counter}
Дата={row['date'].replace('-','.')}
Сумма={row['total']}
ПлательщикСчет=
ДатаСписано=
Плательщик={agrement['name']}
ПлательщикИНН={agrement['inn']}
ПлательщикКПП={(agrement['kpp'] if agrement['kpp']=='0' else '')}
ПлательщикРасчСчет=
ПлательщикБанк1=
ПлательщикБИК=
ПлательщикКорсчет=
ПолучательСчет={self.acc}
ДатаПоступило={row['date'].replace('-','.')}
Получатель=ООО "АтомЭнергоСбыт Бизнес", филиал "АтомЭнергоСбыт" Хакасия
ПолучательИНН=4633017746
ПолучательКПП=190043001
ПолучательРасчСчет={self.acc}
ПолучательБанк1=Московский банк ПАО Сбербанк
ПолучательБИК=044525225
ПолучательКорсчет=30101810400000000225
ВидПлатежа=электронно
ВидОплаты=01
Код=
СтатусСоставителя=
ПоказательКБК=
ОКАТО=
ПоказательОснования=
ПоказательПериода=
ПоказательНомера=
ПоказательДаты=
ПоказательТипа=
Очередность=5
НазначениеПлатежа={row['nc']} / {row['fio']} / {row['address']} / {row['period']}
КонецДокумента\n"""

		result += 'КонецФайла'
		print(path)
		str_to_file(path, result)
		logging.info(f'Rows was saved into file:{path}')
		ll_array.append([self.lastrow,'','','','','','','','','','',''])
		ll_columns = [	'Дата платежа',
						'Время платежа',
						'Номер отделения',
						'Номер кассира/УС/СБОЛ',
						'СУИП',
						'Номер договора',
						'ФИО',
						'Адрес',
						'Период',
						'Сумма операции',
						'Сумма перевода',
						'Сумма комисии банку']
		df = pandas.DataFrame(ll_array, columns=ll_columns )
		writer = pandas.ExcelWriter(f'{path}.xlsx', engine='xlsxwriter')
		df.to_excel(writer, sheet_name='Sheet1', startrow=0)
		sheet = writer.sheets['Sheet1']
		sheet.autofilter(0,0,0,25)
		sheet.set_column('A:Z', 15)
		writer.save()
		logging.info(f'Saved Excel file to {path}.xls')


def IsPartOfFileNameInList(partoffilename:str, lst:list) -> bool:
	for name in lst:
		if partoffilename in name:
			return True
	return False

def addSecs(tm, secs):
	fulldate = datetime.datetime(100, 1, 1, tm.hour, tm.minute, tm.second)
	fulldate = fulldate + datetime.timedelta(seconds=secs)
	return fulldate.time()


def Performance():
	sourcefilenames = config['paths']['soursefolder']
	included_extensions = ['txt']
	source_file_names = [fn for fn in os.listdir(sourcefilenames)  if any(fn.endswith(ext) for ext in included_extensions) and ('SB811' in fn or 'SB812' in fn)]
	#print(source_file_names)

	targetfilenames = config['paths']['targetfolder']
	included_extensions = ['txt']
	target_file_names = [fn for fn in os.listdir(targetfilenames)  if any(fn.endswith(ext) for ext in included_extensions) and ('.txt_1CExchange.txt' in fn)]
	#print(target_file_names)

	for sourcefile in source_file_names:
		if not IsPartOfFileNameInList(sourcefile, target_file_names):
			echo(style(text = 'source:', bg='bright_black', fg='bright_yellow')+' '+style(text = sourcefilenames + sourcefile, fg='bright_green'))
			echo(style(text = 'target:', bg='bright_black', fg='bright_yellow')+' '+style(text = targetfilenames, fg='bright_blue'))
			sf = SberFile(sourcefilenames + sourcefile, targetfilenames)
			print()
		else:
			echo(style(text = 'skipped ', bg='bright_black', fg='bright_red')+' '+style(text = 'source:', bg='bright_black', fg='bright_yellow')+' '+style(text = sourcefilenames + sourcefile, fg='bright_green'))



config = configparser.ConfigParser()
config.read("settings.ini", encoding='UTF-8')
delay = int(config['run']['waitforseconds'])
if delay == 0:
	Performance()
else:
	while True:
		Performance()
		echo(style(text=f'sleep for {addSecs(datetime.datetime.now().time(), delay)}', bg='bright_black', fg='bright_cyan'))
		time.sleep(delay)


		







