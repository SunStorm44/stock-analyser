import os
import keyring

CREDENTIALS = dict()

# Get user data
CREDENTIALS['primary_user'] = os.getenv('PRIMARY_USERNAME')
CREDENTIALS['second_user'] = os.getenv('SECONDARY_USERNAME')

# Get Database URL
CREDENTIALS['stock_analyser_database_url'] = os.getenv('STOCK_ANALYSER_DATABASE_URL')

# Get XTB credentials
CREDENTIALS['xtb'] = keyring.get_password('xtb', CREDENTIALS['primary_user'])
