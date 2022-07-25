from utils import encode_obj
import uuid
import cv2
from kafka import KafkaProducer

BOOTSTRAP_SERVERS = 'localhost:9092,localhost:9093,localhost:9094'
VIDEO_TOPIC = 'videostream_in'
CAMERA_ID = str(uuid.uuid4())

camera = cv2.VideoCapture(0)
producer = KafkaProducer(bootstrap_servers=BOOTSTRAP_SERVERS, max_request_size=2682303)

while True:
    success, img = camera.read()
    cv2.imshow("VideoRecorder", img)
    producer.send(VIDEO_TOPIC, encode_obj(img))#.add_errback(lambda e: print(f"Error {e}"))
    cv2.waitKey(1)
