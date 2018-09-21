# to generate SQL insert strings, RUN ON CMD LINE:
# python generate_sql.py --s tst1 --n test_table --d table_def.txt --i data.txt
#
# to add COMMIT; every 10,000 records, RUN ON CMD LINE:
# sed '0~10000 s/$/\nCOMMIT;/g' < insert_records.sql > insert_records_final.sql
#
# performance:
# script generates 500K SQL insert strings in about 10 seconds.

import re, time, logging, argparse
import memory_profiler as mem_profile

def timer(original_function):
	def timer_func(*args):
		logging.basicConfig(filename='sql_insert.log', level=logging.INFO)
		t1 = time.time()
		result = original_function(*args)
		t2 = time.time() -t1
		logging.info('{} function ran in: {:.4f} seconds'.format(original_function.__name__,t2))
		return result
	return timer_func

def read_data(data):
	with open(data,'r') as f:
		records = str(f.readlines())
		clean_records = re.sub(r'\[|\]|\'| ','', records).replace('\\n,','\\n').strip()
		return clean_records

def return_formatted_text(field):
	# apply formatting for string, date, numeric, etc. datatypes
	if "CHAR" in field.get('data_type'):
		return("'" + field.get('value') + "'")
	if "NUMBER" in field.get('data_type'):
		return(field.get('value'))
	if "DATE" in field.get('data_type'):
		return('TO_DATE(' + "'" + field.get('value') + "'," + "'YYYY-MM-DD')")

def write_file(sql, out_file):
	with open(output_file, 'w') as f:
		for line in sql:
			f.write("%s" % line)

@timer
def createSQL(schema, tbl_nm, tbl_def, dat, out_file):
	result = []
	with open(tbl_def, 'r') as f:
		data = [re.sub(r'NOT|NULL|','',line).strip().split() for line in f.readlines()]
		header = data.pop(0) # remove header
		line2 = data.pop(0) # remove divider
		col_list = ', '.join([field[0] for field in data])
		dtype_list = [field[1] for field in data]

		final_list = []
		for line in dat.split('\\n'):
			initial_list = line.split(',')

			intermediate_list = []
			for i in range(0,len(initial_list)):
				intermediate_list.append({'value':initial_list[i], 'data_type':dtype_list[i]})
			final_list.append(intermediate_list)

		print("Generating SQL strings...")

		# create SQL string
		for line in final_list:
			cleaned_data = []
			for field in line:
				formatted_text = return_formatted_text(field)
				cleaned_data.append(formatted_text)
			sql = "insert into %s.%s (%s) values (%s); \n" % (schema, tbl_nm, col_list, ', '.join(cleaned_data))
			result.append(sql)

	write_file(result, out_file)

def get_parser():
	parser = argparse.ArgumentParser(description='This script generates SQL insert statements')
	parser.add_argument('--s', help='schema name', required=True, choices=['tst1','tst2','tst3'])
	parser.add_argument('--n', help='table name', required=True)
	parser.add_argument('--d', help='name of file containing table definition; copy/paste DDL into text file')
	parser.add_argument('--i', help='name of file containing data to be inserted; file should be comma-seperated', required=True)
	parser.add_argument('--o', help='name of final output file contasining SQL insert strings', default='insert_records.sql')

	args = parser.parse_args()
	return args

if __name__ == '__main__':

	print('Memory (before): {} mb'.format(mem_profile.memory_usage()))

	args = get_parser()

	schema_nm = args.s
	tbl_nm = args.n
	tbl_def = args.d
	data = args.i
	output_file = args.o

	values = read_data(data)
	createSQL(schema_nm, tbl_nm, tbl_def, values, output_file)

	logging.info("%s records will be inserted into %s.%s table" % (str(values.count('\\n')+1), schema_nm, tbl_nm))

	print('Memory (after): {} mb'.format(mem_profile.memory_usage()))