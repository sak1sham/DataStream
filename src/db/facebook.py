from config.facebook_ads import ad_insights_fields, ad_campaign_fields, ads_fields
from dst.main import DMS_exporter
import pytz
import pandas as pd
from typing import Dict, Any, List
from helper.logger import logger
import traceback
from datetime import datetime
from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.adobjects.campaign import Campaign
from facebook_business.api import FacebookAdsApi
from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.api import FacebookAdsApi
from helper.util import typecast_df_to_schema


class FacebookManager:
    def __init__(self, project_name: str, source_mapping: Dict[str, Any], tz_str: str = 'Asia/Kolkata') -> None:
        self.access_token = source_mapping["access_token"]
        self.app_secret = source_mapping["app_secret"]
        self.ad_account_id = source_mapping["ad_account_id"]
        self.app_id = source_mapping["app_id"]
        self.facebook_init = FacebookAdsApi.init(
            self.app_id, self.app_secret, self.access_token)
        self.project_name = project_name
        self.event_names = []
        self.tz_info = pytz.timezone(tz_str)
        self.fb_account = AdAccount(self.ad_account_id)
        # super().__init__(self.project_name)

    def parse_json_to_dict(self, insights_json: list):
        list_of_insights = []
        if len(insights_json) == 0:
            return
        for insight in insights_json:
            insight_dict = dict(insight)
            insight_keys = list(insight_dict.keys())
            for each_key in insight_keys:
                if type(insight_dict[each_key]) == list:
                    for ele in insight_dict[each_key]:
                        key = each_key + "_" + \
                            "_".join(ele["action_type"].split("."))
                        insight_dict[key] = ele["value"]
                        if "7d_click" in ele:
                            insight_dict[key + "_7d_click"] = ele["7d_click"]
                        if "1d_view" in ele:
                            insight_dict[key + "_1d_view"] = ele["1d_view"]
                    insight_dict[each_key] = None
            insight_dict["__hevo_id"] = "_".join([insight_dict["campaign_id"], insight_dict["adset_id"],
                                                 insight_dict["ad_id"], insight_dict["date_start"], insight_dict["date_stop"]])
            list_of_insights.append(insight_dict)
        return list_of_insights

    def get_ad_insights_from_api(self, insights_date: datetime, curr_mapping: Dict[str, Any]):
        all_campaign_ids = self.fb_account.get_campaigns()
        params = {
            'time_range': {
                'since': str(insights_date),
                'until': str(insights_date),
            },
            'level': 'ad',
            'action_attribution_windows': ["7d_click", "1d_view"]
        }
        fields = ad_insights_fields
        all_campaigns_insights = []
        final_insights_df = []
        for each_campaign in all_campaign_ids:
            try:
                each_campaign_insights = Campaign(each_campaign["id"])
                each_campaign_insights = each_campaign_insights.get_insights(
                    params=params, fields=fields)
                modified_df = pd.DataFrame(
                    self.parse_json_to_dict(each_campaign_insights))
                if modified_df.shape[0]:
                    final_insights_df.append(modified_df)
                all_campaigns_insights.append(each_campaign_insights)
            except Exception as e:
                logger.err(curr_mapping['unique_id'], traceback.format_exc())
                logger.err(curr_mapping['unique_id'], curr_mapping['unique_id'] + ": Error in retrieving data through API - {0} for date {1} having campaign_id: {2}. Exception: {3}".format(
                    curr_mapping["api_name"], insights_date, each_campaign["id"], str(e)))
        final_insights_df = pd.concat(final_insights_df, ignore_index=True)
        final_insights_df = final_insights_df.reindex(list(curr_mapping["fields"].keys()), axis="columns", fill_value=None)
        final_insights_df = typecast_df_to_schema(final_insights_df, curr_mapping['fields'])
        return final_insights_df

    def get_ad_campaigns_from_api(self, insights_date: datetime, curr_mapping: Dict[str, Any]):
        fields = ad_campaign_fields
        try:
            all_campaigns = self.fb_account.get_campaigns(fields=fields)
            final_campaigns_df = pd.DataFrame(all_campaigns)
            final_campaigns_df = final_campaigns_df.reindex(list(curr_mapping["fields"].keys()), axis="columns", fill_value=None)
            final_campaigns_df = typecast_df_to_schema(final_campaigns_df, curr_mapping['fields'])
        except Exception as e:
            logger.err(curr_mapping['unique_id'], traceback.format_exc())
            logger.err(curr_mapping['unique_id'], curr_mapping['unique_id'] +
                       ": Error in retrieving data through API - {0} for date {1}. Exception: {2}".format(curr_mapping["api_name"], insights_date, str(e)))
        return final_campaigns_df

    def cleaned_processed_data(self, curr_mapping: Dict[str, Any], dst_saver: List[DMS_exporter], start_date: datetime):

        year = start_date.strftime('%Y')
        month = start_date.strftime('%m')
        day = start_date.strftime('%d')
        
        for saver_i in dst_saver:
            if saver_i.type == 'redshift':
                try:
                    cur = saver_i.saver.conn.cursor()
                    cur.execute("select 1 from pg_tables where schemaname = %s and tablename = %s",
                                (saver_i.saver.schema, curr_mapping['api_name']))
                    result = cur.fetchone()
                    if result and curr_mapping["api_name"]:
                        delete_query = "delete from {0}.{1} where {2}='{3}-{4}-{5}'".format(
                            saver_i.saver.schema,
                            curr_mapping['api_name'],
                            curr_mapping["date_column"],
                            year,
                            month,
                            day
                        )
                        cur.execute(delete_query)
                    saver_i.saver.conn.commit()
                    cur.close()
                except Exception as e:
                    logger.err(curr_mapping['unique_id'], traceback.format_exc())
                    logger.err(curr_mapping['unique_id'], curr_mapping['unique_id'] +
                            ": Error in deleting existing table - {0} for date {1} in redshift. Exception: {2}".format(curr_mapping["api_name"], start_date, str(e)))