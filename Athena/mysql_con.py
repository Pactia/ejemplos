from sqlalchemy import create_engine

user = "*"
password = "*"
endpoint = "*"
db = "*"

def create_conn():
    myconn = create_engine(f'mysql+mysqlconnector://{user}:{password}@{endpoint}/{db}')
    
    return myconn.connect()
