from oauth2client.service_account import ServiceAccountCredentials
import pandas as pd
import openpyxl
import os
from pandas.io.json import json_normalize
from datetime import date
from datetime import timedelta
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
#from analytics_data_api import data_v1beta
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import DateRange
from google.analytics.data_v1beta.types import Dimension
from google.analytics.data_v1beta.types import Metric
from google.analytics.data_v1beta.types import BatchRunReportsRequest
from google.analytics.data_v1beta.types import OrderBy
from google.analytics.data_v1beta.types import RunReportRequest
from google.analytics.data_v1beta.types import FilterExpression
from google.analytics.data_v1beta.types import FilterExpressionList
from google.analytics.data_v1beta.types import Filter




import numpy as np


#============================================================

SCOPES = ['https://www.googleapis.com/auth/analytics.readonly']
KEY_FILE_LOCATION = 'C:\\Users\\ga4-data-analytics-**********.json'

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = KEY_FILE_LOCATION
property_id = 'xxxxxxxx'


client = BetaAnalyticsDataClient()
def get_report(start_date, end_date_delta):
    return client.run_report(RunReportRequest(
                    property = 'properties/xxxxxxxxxx',
                    dimensions=[Dimension(name="date"),
                                Dimension(name='pagePath'),
                                Dimension(name="sessionSourceMedium"),
                                Dimension(name="sessionCampaignName"),
                                Dimension(name="eventName"),                       
                                Dimension(name='customEvent:asset'),
                                Dimension(name='customEvent:position')],
                    metrics=    [Metric(name="activeUsers"),
                                 Metric(name="totalUsers"),
                                 Metric(name="screenPageViews"),
                                 Metric(name="eventCount"),
                                 Metric(name="averageSessionDuration"),
                                 Metric(name="sessions")],

                    date_ranges = [DateRange(start_date= start_date.strftime("%Y-%m-%d"),
                                    end_date= (start_date+end_date_delta).strftime("%Y-%m-%d"))],
                    dimension_filter=FilterExpression(
                          not_expression = FilterExpression(                 #use this only if there are multiple filter conditions
                              or_group = FilterExpressionList(              #use this only if there are multiple filters with 'AND' logic
                                expressions=[FilterExpression(
                                            filter=Filter(field_name="cityId",
                                            string_filter=Filter.StringFilter(value="1027028"))),
                                             FilterExpression(
                                             filter=Filter(field_name="cityId",
                                             string_filter=Filter.StringFilter(#match_type=Filter.StringFilter.MatchType.PARTIAL_REGEXP,
                                                                              value="1027048")))
                                            ])
                                            )
                        ),
                    limit = 100000))

final_data = pd.DataFrame()
start_date = date(2023, 1, 25)
end_date = date(2023, 1, 31)
delta = timedelta(days=10)
end_date_delta = timedelta(days=9)

while start_date < end_date:
    response = get_report(start_date, end_date_delta)
    columns = []
    for s in range(len(response.dimension_headers)):
        columns.append(response.dimension_headers[s].name)
    for s in range(len(response.metric_headers)):
        columns.append(response.metric_headers[s].name)
    while response.rows is not None:
        data = []
        for row in response.rows:
            dimensions = []
            metrics = []
            for dim in range(len(row.dimension_values)):
                dimensions.append(row.dimension_values[dim].value)
            for met in range(len(row.metric_values)):
                metrics.append(row.metric_values[met].value)
            data.append(dimensions + metrics)
        response_final = pd.DataFrame(data, columns=columns)
        final_data=final_data.append(response_final,ignore_index=True)
        break
    start_date+=delta

print(final_data)
final_data.to_csv('GA4_python_export.csv')
