
# ORM using sqlalchemy orm. The data is loaded from a csv file
import sqlite3
import pymysql
import pandas as pd
from numpy import genfromtxt
from time import time
from datetime import datetime
from sqlalchemy import Column, Integer, Float, Date, String, BigInteger
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import create_engine, inspect
from sqlalchemy.orm import sessionmaker

def Load_Data(file):
    df = pd.read_csv(file)
    return df.info()

Base = declarative_base()

class Price_History(Base):
    #Tell SQLAlchemy what the table name is and if there's any table-specific arguments it should know about
    __tablename__ = 'Price_History'

    #tell SQLAlchemy the name of column and its attributes:
    ID = Column(Integer, primary_key=True)
    SYMBOL = Column(String(20), nullable=False) 
    DATE = Column(Date)
    OPEN = Column(Float)
    HIGH = Column(Float)
    LOW = Column(Float)
    CLOSE = Column(Float)
    VOLUME = Column(BigInteger)

if __name__ == "__main__":
    t = time()

    # create db
    engine = create_engine('sqlite:///stocks_test.db')
    Base.metadata.create_all(engine)

    #Create the session
    session = sessionmaker()
    session.configure(bind=engine)
    s = session()

    try:
        file_name = "stockdata.csv" #sample CSV file used:  http://www.google.com/finance/historical?q=NYSE%3AT&ei=W4ikVam8LYWjmAGjhoHACw&output=csv
        data = Load_Data(file_name) 

        for i in data: # dict of key:values as column:values
            record = Price_History(**{
                'symbol':i[0],
                'date' : datetime.strptime(i[1], '%d/%b/%Y').date(),
                'open' : i[2],
                'high' : i[3],
                'low' : i[4],
                'close' : i[5],
                'volume' : i[6]
            })
            s.add(record) #Add all the records

        s.commit() #Attempt to commit all the records
    except:
        s.rollback() #Rollback the changes on error
    finally:
        s.close() #Close the connection
    print ("Time elapsed: " + str(time() - t) + " s.") #0.091s
    insp = inspect(Price_History)
    print(insp)
    print(list(insp.columns))
