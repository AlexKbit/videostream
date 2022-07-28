import cv2
from kafka import KafkaConsumer
from utils import decode_obj

BOOTSTRAP_SERVERS = 'localhost:9092,localhost:9093,localhost:9094'
VIDEO_TOPIC = 'videostream_out'

consumer = KafkaConsumer(VIDEO_TOPIC,
                         group_id='video_reader',
                         bootstrap_servers=BOOTSTRAP_SERVERS,
                         auto_offset_reset='earliest',
                         enable_auto_commit=True)

for msg in consumer:
    frame = decode_obj(msg.value)
    cv2.imshow("VideoReader", frame)
    cv2.waitKey(1)