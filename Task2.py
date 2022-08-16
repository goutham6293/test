import apache_beam as beam
import apache_beam.dataframe.convert as convert
import re
from decimal import Decimal
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to
import time
from apache_beam.options.pipeline_options import PipelineOptions
beam_options = PipelineOptions()
#class for converting the string to date time and filtering the date which is greater than 2010
class convertingToDateTime(beam.DoFn):
  header = False
  def process(self, text):
     if Decimal(text[3])>20:
        temp = text[0].split(" UTC")
        tme=time.strptime(temp[0],'%Y-%m-%d %H:%M:%S')
        if  tme.tm_year>2010:
            yield beam.Row(timestamp = tme.tm_year,origin=text[1],destination=text[2],transaction_amount= Decimal(text[3]))

#class for filtering the the transactions
class filtering_transactions(beam.DoFn):
  def process(self, text):
    if( bool(re.search(r'[0-9]+\.[0-9]+',text[3]))):
        yield text
#Composite Transform
class composite_ptransform(beam.PTransform):
 def expand(self, pcoll):
    # Transform logic goes here.
   return (
      pcoll
      # Convert lines of text into individual words.
      |"splitting the text" >> beam.Map(lambda x: x.split(","))
      # |"filterdkmslcmk" >> beam.Map(lambda x: print(x[3]))
      # |"filter34" >> beam.FlatMap(lambda x: re.findall(r'[0-9]+\.[0-9]+', x[3])])
      |"filtering_transactions" >> beam.ParDo(filtering_transactions())
      |"filtering of the data frame for transaction greater than 20£ and and year > 2010" >> beam.ParDo(convertingToDateTime())
      # Count the number of times each word occurs.
      | "aggregate">>beam.GroupBy("timestamp").aggregate_field('transaction_amount', sum, 'total_transaction_amount')
      
      |"Mapping output in the form of json" >> beam.Map(lambda x:{"year":x[0],"total_transaction_amount":x[1]})
       
  )

with beam.Pipeline() as pipe:
 df= pipe | "reading from gcs " >> beam.io.ReadFromText("gs://cloud-samples-data/bigquery/sample-transactions/transactions.csv")
 output= df|"ptransform" >> composite_ptransform() 
 output| "writing  to output" >> beam.io.WriteToText("output/results.json")
 # output=convert.to_pcollection(df1)|"preprocessing ">>beam.ParDo(Convert_to_datetiem())|"aggregate">>beam.GroupBy("timestamp").aggregate_field('transaction_amount', sum, 'total_transaction_amount')|"isdifnsf" >> beam.Map(lambda x:{"year":x[0],"total_transaction_amount":x[1]} )
 # output|beam.io.WriteToText("output/results.json")
 assert_that(
 output,
 equal_to([{"year": 2018, "total_transaction_amount":  Decimal('129.12')},
     {"year": 2017, "total_transaction_amount" : Decimal('13700002125.30')}
 ]))



