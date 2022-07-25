from cvzone.FaceDetectionModule import FaceDetector
import pandas as pd
from utils import encode_obj, decode_obj
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

BOOTSTRAP_SERVERS = 'localhost:9092,localhost:9093,localhost:9094'
VIDEO_IN_TOPIC = 'videostream_in'
VIDEO_OUT_TOPIC = 'videostream_out'


def face_detect(frames: pd.Series) -> pd.Series:
    detector = FaceDetector()
    frames = frames.map(decode_obj)
    res = frames.map(lambda x: encode_obj(detector.findFaces(x)(0)))
    return res


def main():
    spark = SparkSession \
        .builder \
        .appName("VideoStream") \
        .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0') \
        .getOrCreate()

    frames = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092,localhost:9093,localhost:9094") \
        .option("startingOffsets", "latest") \
        .option("subscribe", VIDEO_IN_TOPIC) \
        .load()

    df = frames.selectExpr("value")
    face_detect_udf = F.udf(face_detect)
    df = df.withColumn('frames_out', face_detect_udf(F.col('value'))) \
        .drop('value') \
        .withColumnRenamed('frames_out', 'value')
    query = df.writeStream \
        .format("kafka") \
        .outputMode("append") \
        .option("checkpointLocation", "/tmp/vaquarkhan/checkpoint") \
        .option("kafka.bootstrap.servers", "localhost:9092,localhost:9093,localhost:9094") \
        .option("topic", VIDEO_OUT_TOPIC) \
        .start()

    print("Start")
    query.awaitTermination()
    print("Done")


if __name__ == "__main__":
    main()
