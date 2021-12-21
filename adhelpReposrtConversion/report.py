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
from genericModules.google_ads_operations import _MILLION, GoogleAdsReport
from genericModules.gmail import Gmail
# This import of line 20 is present at data_utils.py file in common_utils folder
# from common_utils.date_utils import DateTimeUtils

logging.basicConfig(level=logging.INFO, format='%(name)s - %(levelname)s - %(message)s')

# Global variables
GOOGLE_ADS_API_VERSION = 'v8'

class GoogleAdsReports:
    # def get_ad_performance_report(
    #     self,
    #     google_ads_report
    # )-> pd.DataFrame:

    #     """
    #     Description : This function fetches Ad performance report which has campaign details, ad details, along with clicks and impressions for the
    #     provided account id (customer id). 
    #     """

    #     fields = """customer.descriptive_name, campaign.id, campaign.name, metrics.clicks, metrics.impressions"""

    #     resource_name = """ad_group_ad"""

    #     conditions = """campaign.status = 'ENABLED' AND ad_group.status = 'ENABLED' AND ad_group_ad.status IN ('ENABLED', 'PAUSED') AND segments.date DURING LAST_30_DAYS"""

    #     query = google_ads_report.build_query(
    #         fields, 
    #         resource_name, 
    #         conditions=conditions
    #     )

    #     money_to_micros = ['metrics.cost_micros']

    #     df = google_ads_report.download(
    #         query, 
    #         money_to_micros 
    #     )
    #     # df = df[df['campaign.name'].str.startswith('- Jollyroom') | df['campaign.name'].str.startswith('- Jollyroom')]
    #     print(df)
    #     # return df


    def get_account_data_segmented(
        self, 
        google_ads_report : object, 
        start_date : str, 
        end_date : str
    )-> pd.DataFrame:
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

            money_to_micros = ['metrics.cost_micros']
            df = google_ads_report.download(
                query, 
                money_to_micros
            )
            Cost = df['metrics.cost_micros']
            # print(df)
        except Exception as e:
            print('Exception = '.upper(),e)
            df = pd.DataFrame()

        return df.head(10)


    def get_all_time_campaigns_data(
        self, 
        google_ads_report : object, 
    )-> pd.DataFrame:
        """
        Descriptation : This function fetches campaigns_data for 
        provided account id (customer id) for all time. 
        """
        fields = """campaign.name, campaign.id, metrics.clicks"""
        resource_name = """campaign"""
        conditions = f"""campaign.status IN ('ENABLED', 'PAUSED') AND campaign.serving_status = 'SERVING'"""

        try:
            query = google_ads_report.build_query(
            fields, 
            resource_name,
            conditions= conditions
            )
            df = google_ads_report.download(
                query
            )
            print(df)

        except Exception as e:
            logging.critical(f"Error occured while fetching campaign report {e}")
            df = pd.DataFrame()
        return df.head(10)
 
   
    @staticmethod
    def get_account_ad_spend_budget(
        google_ads_report : object, 
        start_date : str,
        end_date : str
    )-> pd.DataFrame:
        """
        Descriptation : This function fetches campaign serving_status, Amount, Cost for 
        provided account id (customer id) in the provided date range. 

        Args:
            start_date ([str]): [start_date for which data should be fetched]
            end_date ([str]): [start_date for which data should be fetched]
        """

        account_daily_budget = 0

        account_budget = 0

        cost = 0

        fields = """campaign.id, campaign.status, campaign.serving_status, campaign_budget.amount_micros, metrics.cost_micros"""  # campaign_budget.amount_micros is the budget cost of the campaign per day.

        resource_name = 'campaign'

        conditions = f"""campaign.status IN ('ENABLED') AND campaign.serving_status = 'SERVING'"""

        try:
            query = google_ads_report.build_query(
            fields,
            resource_name,
            conditions= conditions
            )
            df = google_ads_report.download(
                query
            )
            money_to_micros = ['metrics.cost_micros']
            df = google_ads_report.download(
                query, 
                money_to_micros
            )
        except Exception as e:
            logging.error(f"Error occured while fetching campaign report {e}")
            df = pd.DataFrame()

        # if not df.empty:
		# 	# find the number of days between start & end_date
        #     date_format = '%Y-%m-%d'
        #     # days_diff = DateTimeUtils.get_diff_days(start_date, end_date, date_format)

        #     account_daily_budget = df['budget'].sum()

		# 	# multiply daily budget to number of days
        #     # account_budget = account_daily_budget * days_diff
        #     account_budget = round(account_budget, 2)
        #     cost = round(df['cost'].sum(), 2)

        # print('df\n', df)
        return df.head(10)


    @staticmethod
    def get_url_reports(
        google_ads_report : object, 
    )-> pd.DataFrame:
            """
            Descriptation : This function fetches url for the landing webpage image for 
            provided account id (customer id). 
            """

            website_url = ''

            fields = """customer.descriptive_name, customer.id, landing_page_view.unexpanded_final_url"""

            conditions = f"""segments.date DURING YESTERDAY"""

            resource_name = 'landing_page_view'

            try:
                query = google_ads_report.build_query(
                    fields, 
                    resource_name,
                    conditions=conditions
                )
                df = google_ads_report.download(
                    query
                )
            except Exception as e:
                logging.error(f"Error occured while fetching campaign report {e}")
                df = pd.DataFrame()
            if not df.empty:
                customer_id = df['customer.id'].tolist()[0]
                customer_descriptive_name = df['customer.descriptive_name'].tolist()[0]
                website_url = df['landing_page_view.unexpanded_final_url'].tolist()[0]
            return df.head(10)


    def get_business_info_from_report(
            self,
            google_ads_report : object
    )-> pd.DataFrame:
            """
            Descriptation : This function fetches business_name and logo_images of a particular ad_group_ad for 
            provided account id (customer id). 
            """
            
            result = {
                'business_name':'',
                'logo_url':''
            }

            fields = """ad_group_ad.ad.name, ad_group_ad.ad.type, campaign.status, ad_group_ad.policy_summary.approval_status, ad_group_ad.ad.responsive_display_ad.business_name, ad_group_ad.ad.responsive_display_ad.logo_images"""
            
            resource_name = 'ad_group_ad'
            
            conditions = f"""ad_group_ad.ad.type IN ('RESPONSIVE_DISPLAY_AD') AND campaign.status IN ('ENABLED') AND ad_group_ad.policy_summary.approval_status IN ('APPROVED')"""

            try:
                query = google_ads_report.build_query(
                    fields, 
                    resource_name,
                    conditions=conditions
                )
                df = google_ads_report.download(
                    query
                )
                # print(query)
                # print(df)
            except Exception as e:
                logging.error(f"Error occured while fetching campaign report {e}")
                df = pd.DataFrame()
            if not df.empty:
                business_name = df.loc[0,'ad_group_ad.ad.responsive_display_ad.business_name']	
                image_assets = df.loc[0,'ad_group_ad.ad.responsive_display_ad.logo_images']
                # result['business_name'] = business_name
                # asset_id = image_assets['assetId']
                # result['asset_id'] = asset_id
                # print(result)

            return df.head(10)

    # "This function was totally written in Ads words API (-_-)"
    def get_image_url(
        self,
        google_ads_report : object
    )-> pd.DataFrame:
            """
            Descriptation : This function fetches Image information of the assets for the
            provided account id (customer id). 
            """
            fields = """asset.image_asset.file_size, asset.image_asset.full_size.height_pixels, asset.image_asset.full_size.width_pixels, asset.image_asset.full_size.url, asset.id, asset.name, asset.policy_summary.approval_status"""

            resource_name = 'asset'

            # conditions= f"""asset.image_asset.file_size < 500"""

            try:
                query = google_ads_report.build_query(
                    fields, 
                    resource_name
                    # conditions=conditions
                )
                df = google_ads_report.download(
                    query
                )
                if not df.empty:
                    asset_id = df['asset.id']
                    asset_name = df['asset.name']
                    approval_status = df['asset.policy_summary.approval_status']
                    image_asset_file_size = df['asset.image_asset.file_size']
                    image_asset_width = df['asset.image_asset.full_size.width_pixels']
                    image_asset_height = df['asset.image_asset.full_size.height_pixels']
            except Exception as e:
                print('Exception = '.upper(),e)
                df = pd.DataFrame()
            
            return df

        
    def get_account_marketing_cost(
            self, 
            google_ads_report : object, 
            start_date : str, 
            end_date : str
    )-> pd.DataFrame:
            """
            Descriptation : This function fetches Cost and Currency customer code for 
            provided account id (customer id) in the provided date range. 

            Args:
                start_date ([str]): [start_date for which data should be fetched]
                end_date ([str]): [start_date for which data should be fetched]
            """
            fields = """metrics.cost_micros, customer.currency_code"""

            resource_name = 'customer'

            conditions = f"""segments.date BETWEEN '{start_date}' AND '{end_date}'"""

            try:
                query = google_ads_report.build_query(
                    fields, 
                    resource_name,
                    conditions=conditions
                )
                df = google_ads_report.download(
                    query
                )
                # print(query)
                # print(df)
                if not df.empty:
                    total_cost = round(df['metrics.cost_micros'].sum(), 3) 
                    currency = df['customer.currency_code'].tolist()[0]
            except Exception as e:
                print('Exception = '.upper(),e)
                df = pd.DataFrame()
            
            return df


    def get_account_marketing_cost_and_revenue(
		self,
        google_ads_report : object, 
		start_date : str,
		end_date : str
	)->pd.DataFrame:
        """
		Descriptation : This function fetches Marketing cost and Revenue cost for 
		provided account id (customer id). 

        Args:
            start_date ([str]): [start_date for which data should be fetched]
            end_date ([str]): [start_date for which data should be fetched]
        """

        fields = """metrics.cost_micros, metrics.clicks, metrics.conversions_value,customer.currency_code"""

        resource_name = 'customer' 

        conditions = f"""segments.date BETWEEN '{start_date}' AND '{end_date}'"""

        total_cost = 0.00

        conv_value= 0.00

        currency = ''

        website_visitors=0.00

        roas=0.00

        try:
            query = google_ads_report.build_query(
                fields, 
                resource_name,
                conditions=conditions
            )
            df = google_ads_report.download(
                query
            )
            # print(query)
            # print(df)
            if not df.empty:
                metrics_cost_micros = round(df['metrics.cost_micros'].sum())
                metrics_conversions_value =round(df['metrics.conversions_value'].sum())
                customer_currency_code = df['customer.currency_code'].tolist()[0]
                metrics_clicks = df['metrics.clicks'].sum()
        except Exception as e:
            print('Exception = '.upper(),e)
            df = pd.DataFrame()
        
        # performance_dict['GOOGLE_ADS'] = {
		# 	"revenue": conv_value,
		# 	"spends": total_cost,
		# 	"website_visitors": website_visitors,
		# 	"roas": roas,
		# 	"currency": currency
		# }
        
        return df