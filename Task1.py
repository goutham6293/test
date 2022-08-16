import apache_beam as beam
import apache_beam.dataframe.convert as convert
import numpy as np
import json
import datetime
from decimal import Decimal
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to
import time

class Convert_to_datetiem(beam.DoFn):
  def process(self, text):
     temp = text[0].split(" UTC")
     tme=time.strptime(temp[0],'%Y-%m-%d %H:%M:%S')
     if  tme.tm_year>2010:
      new= beam.Row(timestamp = tme.tm_year,origin=text[1],destination=text[2],transaction_amount= Decimal(text[3]))
      yield new

with beam.Pipeline() as pipe:
 df= (pipe | "reading from gcs " >> beam.dataframe.io.read_csv("gs://cloud-samples-data/bigquery/sample-transactions/transactions.csv",dtype={"timestamp": np.string_,"origin":np.string_,"destination":np.string_,"transaction_amount":np.double}))
 df1=df[df['transaction_amount']>20]
 
 output=convert.to_pcollection(df1)|"preprocessing ">>beam.ParDo(Convert_to_datetiem())|"aggregate">>beam.GroupBy("timestamp").aggregate_field('transaction_amount', sum, 'total_transaction_amount')|"isdifnsf" >> beam.Map(lambda x:{"year":x[0],"total_transaction_amount":x[1]} )
 output|beam.io.WriteToText("output/results.json")
 assert_that(
 output,
 equal_to([{"year": 2017, "total_transaction_amount" : Decimal('13700002125.29999992370585460')},
 {"year": 2018, "total_transaction_amount":  Decimal('129.1200000000000045474735089')}]))





