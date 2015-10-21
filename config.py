
class Config(object):
    DEBUG = False


class DevelopmentConfig(Config):
    DEBUG = True
    REGISTER_URI = "http://localhost:5004"
    LEGACY_DB_URI = "http://localhost:5007"
    NAMES_SEARCH_URI = "http://localhost:5013"
    MQ_USERNAME = "mquser"
    MQ_PASSWORD = "mqpassword"
    MQ_HOSTNAME = "localhost"
    MQ_PORT = "5672"


class PreviewConfig(Config):
    REGISTER_URI = "http://localhost:5004"
    LEGACY_DB_URI = "http://localhost:5007"
    NAMES_SEARCH_URI = "http://localhost:5013"
    MQ_USERNAME = "mquser"
    MQ_PASSWORD = "mqpassword"
    MQ_HOSTNAME = "localhost"
    MQ_PORT = 5672
