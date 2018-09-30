
import xlrd
import requests
import json
import boto3
import logging

logging.basicConfig(level=logging.INFO)
JSON_FILE = 'data.json'
URL = 'https://www.iso20022.org/sites/default/files/ISO10383_MIC/ISO10383_MIC.xls'
SHEET_NAME = 'MICs List by CC'

# S3 CONFIGURATION
AWS_ACCESS_KEY_ID = '<aws access key id>'
AWS_SECRET_ACCESS_KEY = '<aws secret access key>'
S3_BUCKET_NAME = '<bucket name>'


def download_file(url):
	"""
	Download file from url
	Input Param: url
	"""
	try:
		local_filename = url.split('/')[-1]
		r = requests.get(url, stream=True)
		with open(local_filename, 'wb') as f:
			for chunk in r.iter_content(chunk_size=1024):
				if chunk:
					f.write(chunk)
		return local_filename
	except Exception as e:
		logging.error("Parser_Upload : Error in download file - {}".format(e))
		return None


def upload_to_s3(filename):
	"""
	Upload file in s3 bucket
	Input Param:
	filename : name of the file in s3 bucket
	"""
	try:
		s3 = boto3.resource('s3')
		data = open(filename, 'rb')

		key = str(filename)
		s3.Bucket(S3_BUCKET_NAME).put_object(Key=key, Body=data, ACL='public-read')
	except Exception as e:
		logging.error("Parser_Upload : Error in upload_to_s3 - {}".format(e))


if __name__ == "__main__":
	logging.info("Parser_Upload : URL - {}".format(URL))
	file_name = download_file(URL)

	if file_name:
		workbook = xlrd.open_workbook(file_name)
		logging.info("Parser_Upload : File Created - {}".format(file_name))
		logging.info("Parser_Upload : Sheet name - {}".format(SHEET_NAME))

		worksheet = workbook.sheet_by_name(SHEET_NAME)

		first_row = [keyword.value for keyword in worksheet.row(0)]

		data = []
		for index in range(1, worksheet.nrows):
			counter = 0
			row_dict = dict()
			rx = worksheet.row(index)
			for rx_values in rx:
				row_dict[first_row[counter]] = rx_values.value
				counter += 1
			data.append(row_dict)

		with open(JSON_FILE, 'w') as outfile:
			json.dump(data, outfile)

		logging.info("Parser_Upload : {} Created ".format(JSON_FILE))
		logging.info("Parser_Upload : Uploading to s3")
		upload_to_s3(JSON_FILE)
		logging.info("Parser_Upload : Successfully Uploaded to s3")
