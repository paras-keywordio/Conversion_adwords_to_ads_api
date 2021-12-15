# Standard library imports
import os
import sys
import threading
import traceback
from datetime import datetime
import time
import copy
import logging
from numpy import empty

#Third party imports
import pandas as pd

# local application imports
from genericModules.api_connectors import GoogleAdsAPIConnector
from genericModules.spreadsheet import Spreadsheet
from genericModules.google_ads_operations import GoogleAdsReport
from genericModules.gmail import Gmail

logging.basicConfig(level=logging.INFO, format='%(name)s - %(levelname)s - %(message)s')

# Global variables
GOOGLE_ADS_API_VERSION = 'v8'

class GoogleAdsReports:
    def get_ad_performance_report(
        self,
        google_ads_report
    ):

        """
        Description : This function fetches Ad performance report
        provided account id (customer id). 
        """

        fields = """customer.descriptive_name, campaign.id, campaign.name, ad_group.id, ad_group.name, ad_group_ad.ad.id, ad_group_ad.ad.expanded_text_ad.headline_part1, ad_group_ad.status, ad_group_ad.labels, metrics.clicks, metrics.impressions"""

        resource_name = """ad_group_ad"""

        conditions = """campaign.status = 'ENABLED' AND ad_group.status = 'ENABLED' AND ad_group_ad.status IN ('ENABLED', 'PAUSED') AND segments.date DURING LAST_30_DAYS"""

        query = google_ads_report.build_query(
            fields, 
            resource_name, 
            conditions=conditions
        )

        money_to_micros = ['metrics.cost']

        df = google_ads_report.download(
            query, 
            money_to_micros 
        )
        return df

    def get_account_data_segmented(
        self, 
        google_ads_report, 
        start_date, 
        end_date, 
        segment = None
    ):
        """
        Descriptation : This function fetches Clicks, Cost, Conversion Rate, Conversion for 
        provided account id (customer id). 

        Args:
            start_date ([str]): [start_date for which data should be fetched]
            end_date ([str]): [start_date for which data should be fetched]
        """

        fields = """customer.id,metrics.clicks,metrics.cost_micros,metrics.conversions_value,metrics.conversions_from_interactions_rate,metrics.conversions"""

        resource_name = """customer"""

        conditions = f"""segments.date BETWEEN '{start_date}' AND '{end_date}'"""

        try:
            query = google_ads_report.build_query(
                fields, 
                resource_name, 
                conditions=conditions
            )

            money_to_micros = ['metrics.cost']
            df = google_ads_report.download(
                query, 
                money_to_micros 
            )
        except Exception as e:
            print('Exception = '.upper(),e)
            df = pd.DataFrame()

        return df


    def get_all_time_campaigns_data(
        self, 
        google_ads_report,
        start_date,
        end_date
    ):
        """
        Descriptation : This function fetches campaigns_data for 
        provided account id (customer id). 

        Args:
            start_date ([str]): [start_date for which data should be fetched]
            end_date ([str]): [start_date for which data should be fetched]
        """
        fields = """campaign.name"""
        resource_name = """campaign"""
        conditions = f"""campaign.status IN ('ENABLED', 'PAUSED') AND campaign.serving_status = 'SERVING'"""

        try:
            query = google_ads_report.build_query(
            fields, 
            resource_name,
            conditions
            )
            df = google_ads_report.download(
                query
            )
            df = df[df['campaign.name'].str.startswith('AdHelp') | df['campaign.name'].str.startswith('Adhelp')]

        except Exception as e:
            logging.critical(f"Error occured while fetching campaign report {e}")
            df = pd.DataFrame()
        return df
    
    @staticmethod
    def get_account_ad_spend_budget(self, 
        google_ads_report,
        start_date,
        end_date,
    ):
        account_daily_budget = 0
        account_budget = 0
        cost = 0

        reportType = 'campaign'
        fields = """campaign.id, campaign_budget.amount_micros, metrics.cost_micros"""
        predicates = [
			# {'field':'CampaignStatus','operator': 'IN','values': ['ENABLED']},
            # {'field':'ServingStatus','operator':'EQUALS','values':'SERVING'},
		]

        campaign_report.date_range = date_range
        campaign_report.fields = fields
		# campaign_report.predicates = predicates

        try:
            df = campaign_report.download()
			# df = df[df['campaign'].str.startswith('AdHelp') | df['campaign'].str.startswith('Adhelp')]
        except Exception as e:
            logging.error(f"Error occured while fetching campaign report {e}")
            df = pd.DataFrame()

        if not df.empty:
			# find the number of days between start & end_date
            date_format = '%Y-%m-%d'
            days_diff = DateTimeUtils.get_diff_days(start_date, end_date, date_format)

            account_daily_budget = df['budget'].sum()

			# multiply daily budget to number of days
            account_budget = account_daily_budget * days_diff
            account_budget = round(account_budget, 2)
            cost = round(df['cost'].sum(), 2)

        print('df\n', df)

        result = {
			'spend': cost,
			'budget': account_budget,
			'id': id_,
		}

        if result:
            platform_name = PlatformTypes.GOOGLE_ADS.get_name()
            results[platform_name] = result

        return result
