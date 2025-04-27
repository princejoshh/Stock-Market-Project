#superset config

import os
from superset.config import *

# Your custom secret key
SECRET_KEY = '7878'


# The SQLite database URI
SQLALCHEMY_DATABASE_URI = r'sqlite:///C:\Users\ACER\OneDrive\Desktop\NEW\stock_data.db'

# Additional configurations
ENABLE_PROXY_FIX = True
DATABASES_ALLOWED_EXTENSIONS = {'sqlite'}
PREVENT_UNSAFE_DB_CONNECTIONS = False

# Enable file uploads
UPLOAD_FOLDER = os.path.expanduser(r"C:\Users\ACER\OneDrive\Desktop\NEW\uploads")
ALLOWED_EXTENSIONS = {'csv'}

# Feature flags to explicitly enable file uploads
FEATURE_FLAGS = {
    "ENABLE_FILE_UPLOADS":True,
}