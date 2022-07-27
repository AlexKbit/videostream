from utils import encode_obj
import uuid
import cv2
from kafka import KafkaProducer

BOOTSTRAP_SERVERS = 'localhost:9092'
VIDEO_TOPIC = 'videostream_in'
CAMERA_ID = str(uuid.uuid4())

camera = cv2.VideoCapture(0)
camera.set(cv2.CAP_PROP_FRAME_WIDTH, 350)
camera.set(cv2.CAP_PROP_FRAME_HEIGHT, 350)
producer = KafkaProducer(bootstrap_servers=BOOTSTRAP_SERVERS,
                         key_serializer=str.encode,
                         max_request_size=1246000,
                         compression_type='lz4',
                         acks=1)

while True:
    success, img = camera.read()
    cv2.imshow("VideoRecorder", img)
    producer.send(VIDEO_TOPIC, value=encode_obj(img), key=CAMERA_ID).add_errback(lambda e: print(f"Error {e}"))
    cv2.waitKey(1)
