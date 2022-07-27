import codecs
import pickle


def encode_obj(obj):
    return codecs.encode(pickle.dumps(obj, protocol=pickle.HIGHEST_PROTOCOL), "base64")


def decode_obj(obj):
    return pickle.loads(codecs.decode(obj, "base64"))
