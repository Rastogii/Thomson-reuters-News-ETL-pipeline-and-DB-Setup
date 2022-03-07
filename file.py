from pyspark.sql import SparkSession
from pyspark.sql import Row
from sparknlp.pretrained import PretrainedPipeline
import pyspark.sql.functions as F
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, StringType
import dateutil.parser
import re
import json
spark = SparkSession.builder.appName('Parser').getOrCreate()
numslist = []
text =""
count = 0
headers = ""
df = spark.createDataFrame([Row(Text='')])
pipeline = PretrainedPipeline("translate_mul_en", lang = "xx")
#language_detector_pipeline = PretrainedPipeline('detect_language_375', lang='xx')
with open("2013-07-01.csv", encoding="utf-8") as f:
    for index,line in enumerate(f):
        if index == 0:
            headers=line
        #else:
        elif index < 500:
            z= re.search('^"\d{4}-\d{2}-\d{2}","\d{2}:\d{2}:\d{2}.\d{3}",".*",".*",".*".*', line)
            if z and text!="":

                newRow = spark.createDataFrame([Row(Text=text)])
                df = df.union(newRow)
                text=""
                del newRow
                count+=1
            text+=line.strip().strip("\n")
            text+=" "
    newRow = spark.createDataFrame([Row(Text=text)])
    df = df.union(newRow)

partitioneddf = df.repartition(numPartitions=100)
#partitioneddf.rdd.getNumPartitions().show()

def parsers(x):
    x = str(x)
    listitems = x.split('","')
    #I want to convert data tyep right here for elasticsearch
    if x!="" and len(listitems) > 3 and listitems[3].strip('"').lower() != "delete":
        #from sparknlp.pretrained import PretrainedPipeline
        dict = {}

        datev = listitems[0].strip('"')
        timev = listitems[1].strip('"')
        
        time = timev.split(" ")[0].split(":")
        timev = time[0] +":"+ time[1] + ":" + time[2].split('.')[0]
        dict["dates"] = timev
        try:
            unique_story_indexv= listitems[2].strip('"')
        except:
            unique_story_indexv=""
        if unique_story_indexv == "":
            unique_story_indexv=None
        dict["unique_story_index"] = unique_story_indexv
        try:
            event_typev = listitems[3].strip('"')
        except:
            event_typev=None
        if event_typev == "":
            event_typev=None
        dict["event_type"]= event_typev
        try:
            pnacv = listitems[4].strip('"')
        except:
            pnacv = ""
        if pnacv == "":
            pnacv = None
        dict["pnac"]= pnacv
        story_date_timev = ""
        take_date_timev = ""
        try:
            story_date_timev = listitems[5].strip('"')
            story_date_timev = dateutil.parser.parse(story_date_timev)
            story_date_timev = story_date_timev.strftime('%Y-%m-%d %H:%M:%S')
        except:
            story_date_timev = None
        dict["story_date_time"]=story_date_timev
        try:
            take_date_timev = listitems[6].strip('"')
            take_date_timev = dateutil.parser.parse(take_date_timev)
            take_date_timev = take_date_timev.strftime('%Y-%m-%d %H:%M:%S')
        except:
            take_date_timev = None
        dict["take_date_time"]= take_date_timev
        try:
            headline_alert_textv = listitems[7].strip('"')
        except:
            headline_alert_textv= "Empty"
        if headline_alert_textv == "":
            dict["headline"]="Empty"
        try:
            accumulated_story_textv = listitems[8].strip('"')
        except:
            accumulated_story_textv= "Empty"
        if accumulated_story_textv == "":
            accumulated_story_textv="Empty"
        dict["accumulated_story_text"]= accumulated_story_textv
        try:
            take_textv = listitems[9].strip('"')
        except:
            take_textv="Empty"
        if take_textv == "":
            take_textv="Empty"
        try:
            productsv = listitems[10].strip('"')
        except:
            productsv = ""
        if productsv == "":
            productsv = None
        dict["products"]= productsv
        try:
            topicsv = listitems[11].strip('"')
        except:
            topicsv="Empty"
        if topicsv=="":
            topicsv="Empty"
        dict["topics"]= topicsv
        if headline_alert_textv=="Empty" and take_textv=="Empty":
            return (None)
        try:
            related_ricsv = listitems[12].strip('"')
        except:
            related_ricsv=""
        if related_ricsv == "":
            related_ricsv = None
        dict["related_rics"]= related_ricsv
        try:
            named_itemsv = listitems[13].strip('"')
        except:
            named_itemsv = ""
        dict["named_items"]= named_itemsv
        try:
            story_typev = listitems[15].strip('"')
        except:
            story_typev=""
        dict["story_type"]= story_typev
        languagev = 'EN'
        #(language_detector_pipeline, pipeline) = createobject()
        #lang = language_detector_pipeline.annotate(take_textv)['language'][0]
        #dict["language"]= lang
        #take_test_eng  = pipeline.annotate(take_textv)['translation'][0]
        #dict["take_text"]= take_text_eng
        dict['language'] = languagev
        dict['take_text'] = take_textv
        #return (dict['unique_story_index'], json.dumps(dict))
        return (json.dumps(dict))
    return (None)

convertUDF = F.udf(lambda z: parsers(z))

es_write_conf = {
    "es.nodes" : "13.211.190.7",
    "es.port" : "9200",
    "es.resource" : 'thomreuters/2013-07-01',
    "es.input.json": "yes",
    "es.mapping.id": "unique_story_index"
}

#rdd = partitioneddf.foreachPartition(lambda x: parsers(x, language_detector_pipeline, pipeline))
#rdd = partitioneddf.map(lambda x: parsers(x))
newPartitioneddf = partitioneddf.withColumn("Text", convertUDF(col("Text")))
#newPartitioneddf1 = newPartitioneddf.filter(newPartitioneddf.Text != (None))
dataCollect = newPartitioneddf.collect()
print(dataCollect[0])
i = json.loads(dataCollect[0]['Text'])
take_test_en = pipeline.annotate(i['take_text'])['translation']
take_text_eng =' '.join(item for item in take_test_en)
headline_eng =' '.join(item for item in pipeline.annotate(i['headline'])['translation'])
schema = StructType([StructField("dates", StringType(), True),StructField("unique_story_index", StringType(), True), StructField("event_type", StringType(), True), StructField("pnac", StringType(), True),StructField("story_date_time", StringType(), True), StructField("take_date_time", StringType(), True), StructField("headline", StringType(), True),StructField("accumulated_story_text", StringType(), True), StructField("take_text", StringType(), True),StructField("products", StringType(), True),StructField("topics", StringType(), True), StructField("related_rics", StringType(), True), StructField("named_items", StringType(), True), StructField("story_type", StringType(), True), StructField("language", StringType(), True)])
finalDf = spark.createDataFrame([Row(dates=i['dates'], unique_story_index=i['unique_story_index'], event_type=i['event_type'], pnac=i['pnac'], story_date_time=i['story_date_time'], take_date_time=i['take_date_time'], headline=headline_eng, accumulated_story_text=i['accumulated_story_text'], take_text=take_text_eng, products=i['products'], topics=i['topics'], related_rics=i['related_rics'], named_items=i['named_items'], story_type=i['story_type'], language=i['language'])], schema=schema)
finalDf.show()
for it in dataCollect[1:]:
    try:
        i = json.loads(it['Text'])
        if i == (None):
            continue
    except:
        print('Got bad Row. Skipping')
        continue
    take_test_en = pipeline.annotate(i['take_text'])['translation']
    take_text_eng =' '.join(item for item in take_test_en)
    headline_eng =' '.join(item for item in pipeline.annotate(i['headline'])['translation'])
    nRow = spark.createDataFrame([Row(dates=i['dates'], unique_story_index=i['unique_story_index'], event_type=i['event_type'], pnac=i['pnac'], story_date_time=i['story_date_time'], take_date_time=i['take_date_time'], headline=headline_eng, accumulated_story_text=i['accumulated_story_text'], take_text=take_text_eng, products=i['products'], topics=i['topics'], related_rics=i['related_rics'], named_items=i['named_items'], story_type=i['story_type'], language=i['language'])], schema=schema)
    finalDf = finalDf.union(nRow)
    print('Preprocessed Row')
    del nRow

#finalDf.rdd.saveAsNewAPIHadoopFile(path='-', outputFormatClass="org.elasticsearch.hadoop.mr.EsOutputFormat", keyClass="org.apache.hadoop.io.NullWritable", valueClass="org.elasticsearch.hadoop.mr.LinkedMapWritable",conf=es_write_conf)
finalDf.write\
    .format("org.elasticsearch.spark.sql")\
    .option("es.nodes",  "x.x.x.x")\
    .option("es.port" , "9200")\
    .option("es.resource","thomreuters/2013-07-01")\
    .option("es.mapping.id", "unique_story_index")\
    .save()

finalDf.head(3)
