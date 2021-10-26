"""Hello Analytics Reporting API V4."""
""" Create and save client_credentials.json in the same folder as this Python file or provide the file path to KEY_FILE_LOCATION"""

!pip3 install apiclient
#If apiclient gives you errors, try the alternative 'google-api-python-client' and use googleapiclient instead of apiclient
!pip3 install oauth2client
!pip3 install openpyxl

from apiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials
import pandas as pd
import openpyxl
import csv
import ast
from datetime import date
from datetime import timedelta

SCOPES = ['https://www.googleapis.com/auth/analytics.readonly']
KEY_FILE_LOCATION = 'client_secrets_services.json'
VIEW_ID = 'XXXXXXXX'

def initialize_analyticsreporting():
  """Initializes an Analytics Reporting API V4 service object.

  Returns:
    An authorized Analytics Reporting API V4 service object.
  """
  credentials = ServiceAccountCredentials.from_json_keyfile_name(KEY_FILE_LOCATION, SCOPES)

  # Build the service object.
  analytics = build('analyticsreporting', 'v4', credentials=credentials)

  return analytics


def get_report(analytics, start_date, end_date_delta, pageToken = None):
      """Queries the Analytics Reporting API V4.

      Args:
        analytics: An authorized Analytics Reporting API V4 service object.
      Returns:
        The Analytics Reporting API V4 response.
      """
      return analytics.reports().batchGet(
         body={
                'reportRequests': [
            {
              'viewId': VIEW_ID,
              'pageSize': 10000,
              'pageToken': pageToken,
              'dateRanges': 
                [{'startDate': start_date.strftime("%Y-%m-%d") , 'endDate': (start_date+end_date_delta).strftime("%Y-%m-%d")}
                ],
               'metrics': [{'expression': 'ga:pageviews'}, {'expression': 'ga:sessions'}, {'expression': 'ga:pageviewspersession'}, {'expression': 'ga:totalEvents'}, {'expression': 'ga:avgtimeonpage'}],
               'dimensions': [{'name':'ga:date'},{'name':'ga:clientid'},{'name':'ga:campaign'}, {'name':'ga:sourceMedium'}, {'name':'ga:pagePath'}, {'name':'ga:userType'},{'name':'ga:eventLabel'}]
            #   'dimensionFilterClauses': [
            #                    {"filters": [{"dimensionName": "ga:eventLabel",
            #                                  "operator": "REGEXP",
            #                                  "expressions": '/blog/'}]
            #                            }],
                #'samplingLevel' : 'LARGE'
            }]
        }
 ).execute()


def print_response(response):
  """Parses and prints the Analytics Reporting API V4 response.

  Args:
    response: An Analytics Reporting API V4 response.
  """
  for report in response.get('reports', []):
    columnHeader = report.get('columnHeader', {})
    dimensionHeaders = columnHeader.get('dimensions', [])
    metricHeaders = columnHeader.get('metricHeader', {}).get('metricHeaderEntries', [])

    for row in report.get('data', {}).get('rows', []):
      dimensions = row.get('dimensions', [])
      dateRangeValues = row.get('metrics', [])

      for header, dimension in zip(dimensionHeaders, dimensions):
        print(header + ': ', dimension)

      for i, values in enumerate(dateRangeValues):
        print('Date range:', str(i))
        for metricHeader, value in zip(metricHeaders, values.get('values')):
          print(metricHeader.get('name') + ':', value)

def main():
  final_data = pd.DataFrame()
  start_date = date(2021, 10, 13)
  end_date = date(2021, 10, 15)
  delta = timedelta(days=3)
  end_date_delta = timedelta(days=3)
  analytics = initialize_analyticsreporting()
  
  while start_date < end_date:
    print("Start: ", start_date, " - End: ", start_date + end_date_delta)
    response = get_report(analytics, start_date, end_date_delta)
    columns = []
    for s in range(len(response['reports'][0]['columnHeader']['dimensions'])):
        columns.append(response['reports'][0]['columnHeader']['dimensions'][s])
    for s in range(len(response['reports'][0]['columnHeader']['metricHeader']['metricHeaderEntries'])):
        columns.append(response['reports'][0]['columnHeader']['metricHeader']['metricHeaderEntries'][s]['name'])
    dimensions = []
    for s in range(len(response['reports'][0]['data']['rows'])):
        dimensions.append(response['reports'][0]['data']['rows'][s]['dimensions'] )
    metrics = []
    for s in range(len(response['reports'][0]['data']['rows'])):
        metrics.append(response['reports'][0]['data']['rows'][s]['metrics'][0]['values'])
    rows=[]
    for i in range(len(dimensions)):
        rows.append(dimensions[i] + metrics[i])
    print("Rows# : ", len(rows))
    print("==============")
    response_final = pd.DataFrame(rows, columns=columns)
    final_data=final_data.append(response_final,ignore_index=True)
    start_date+=delta
  
  print(final_data)
  print(len(final_data))
 
  final_data.to_csv('ga_reporting.csv', sep=',')

 

if __name__ == '__main__':
  main()
