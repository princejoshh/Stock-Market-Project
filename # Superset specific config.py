# Superset specific config
ROW_LIMIT = 5000

SECRET_KEY = '7878'
import urllib

#params = urllib.parse.quote_plus("DRIVER={ODBC Driver 17 for SQL Server};SERVER=localhost;DATABASE=master;Trusted_Connection=yes")
SQLALCHEMY_DATABASE_URI = 'sqlite:///C:\\Workspace\\Qriocity\\Other Domain Projects\\Apache Superset_Without_venv\\stock_data.db'

ALLOWED_SQLALCHEMY_SCHEMAS = ["sqlite"]

def is_sqlalchemy_uri_valid(uri: str) -> bool:
    # Allow SQLite for local use only
    return uri.startswith("sqlite:")



# Flask-WTF flag for CSRF
WTF_CSRF_ENABLED = True
# Add endpoints that need to be exempt from CSRF protection
WTF_CSRF_EXEMPT_LIST = []
# A CSRF token that expires in 1 year
WTF_CSRF_TIME_LIMIT = 60 * 60 * 24 * 365

# Set this API key to enable Mapbox visualizations
MAPBOX_API_KEY = ''