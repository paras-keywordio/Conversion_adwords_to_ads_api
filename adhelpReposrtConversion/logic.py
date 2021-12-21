# Standard library imports
import os
import sys
import threading
import traceback
from datetime import datetime
import copy
from report import GoogleAdsReports
#Third party imports
import pandas as pd

# local application imports
from genericModules.api_connectors import GoogleAdsAPIConnector
from genericModules.spreadsheet import Spreadsheet
from genericModules.google_ads_operations import GoogleAdsReport
from genericModules.gmail import Gmail

# Global variables
GOOGLE_ADS_API_VERSION = 'v8'


def main(
	PRODUCTION_MODE, 
	script_utility, 
	api_connector_factory, 
	customers_df, 
	result_count, 
	errors
):


	google_ads_yaml_path = script_utility.get_google_ads_yaml_path()

	connector = GoogleAdsAPIConnector(
		google_ads_yaml_path, 
		GOOGLE_ADS_API_VERSION
	)

	# print("Connector--> ",connector)

	google_ads_client = connector.connect()

	# print("Client--> ",google_ads_client)

	log_dict = {}

	call_threads(
		PRODUCTION_MODE,
		script_utility, 
		customers_df, 
		google_ads_client, 
		result_count, 
		errors, 
		log_dict
	)

	return result_count, errors

#---Function to create threads
def call_threads(
	PRODUCTION_MODE, 
	script_utility, 
	customers_df, 
	google_ads_client, 
	result_count, 
	errors, 
	log_dict
):

	threads = []

	for index,row in customers_df.iterrows():
		customer_id = row['Customer Id']
		account_name = row['Account Name']

		print('\nSCRIPT RUNNING FOR - ',str(customer_id))

		thread = myThread(
			PRODUCTION_MODE, 
			script_utility, 
			customer_id, 
			account_name, 
			google_ads_client, 
			result_count, 
			errors, 
			log_dict
		)

		thread.start()
		threads.append(
			thread
		)

	for thread in threads:
		thread.join()

class myThread(threading.Thread):
	_attributes = (
		'PRODUCTION_MODE', 
		'script_utility', 
		'customer_id', 
		'account_name', 
		'google_ads_client', 
		'result_count', 
		'errors', 
		'log_dict'
	)

	def __init__ (self, *args):
		super(myThread, self).__init__()
		for attribute, value in zip(self._attributes, args):
			setattr(self, attribute, value)

	def run(self):
		production_mode = self.PRODUCTION_MODE
		script_utility = self.script_utility
		google_ads_client = self.google_ads_client
		customer_id = self.customer_id
		account_name = self.account_name
		result_count = self.result_count
		errors = self.errors
		log_dict = self.log_dict

		google_ads_client_copy = copy.copy(google_ads_client)

		mutate_count = 0
		try:
			mutate_count = script_logic(
				production_mode, 
				google_ads_client_copy, 
				customer_id, 
				log_dict
			)
			result_count[customer_id] = True
			status = 'SUCCESS'

		except Exception as e:
			print(traceback.format_exc())
			status = 'FAILED | {}'.format(traceback.format_exc())
			errors[customer_id] = status

		script_utility.write_script_running_status(
			account_name, 
			customer_id, 
			status, 
			changes=mutate_count
		)

def script_logic(
	production_mode,
	google_ads_client, 
	customer_id, 
	log_dict
):

	if production_mode:
		google_ads_report = GoogleAdsReport(
			google_ads_client, 
			customer_id
		)

		google_reports = GoogleAdsReports()
		start_date = '2018-10-03'
		end_date = '2018-12-03'
		# df = google_reports.get_ad_performance_report(google_ads_report)
		df = google_reports.get_account_data_segmented(google_ads_report,start_date,end_date)
		# df = google_reports.get_all_time_campaigns_data(google_ads_report)
		# df = google_reports.get_account_ad_spend_budget(google_ads_report,start_date,end_date)
		# df = google_reports.get_url_reports(google_ads_report)
		# df = google_reports.get_business_info_from_report(google_ads_report)
		# df = google_reports.get_image_url(google_ads_report)
		# df = google_reports.get_account_marketing_cost(google_ads_report,start_date,end_date)
		# df = google_reports.get_account_marketing_cost_and_revenue(google_ads_report,start_date,end_date)
		print(df)


	mutate_count = 0
	log_dict[customer_id] = ['log data']

	return mutate_count