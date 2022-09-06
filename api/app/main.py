import configparser
from typing import Optional
from models import SearchUser
import uvicorn
from fastapi.responses import JSONResponse
import pandas as pd
import psycopg2
from fastapi import FastAPI
import db_engine as dbe
import os
import configparser
from sql_queries import get_users_by_department_company

app = FastAPI()


@app.get('/')
def root():
    return {'message': 'Hello, This is my database api'}


@app.post('/read')
async def read(item: SearchUser):
    try:
        item = item.dict()
        params = {}

        config = configparser.ConfigParser()
        config.read_file(open(os.getcwd()+'/app/config/config.cfg'))
        pg_config = dict(config.items('POSTGRESQL'))

        for k, v in pg_config.items():
            params[k] = v

        cur, conn = await dbe.create_connection(params)

        query = get_users_by_department_company.format(**item)

        cur.execute(query)
        rows = cur.fetchall()
        dbe.close_connection(cur, conn)
        return rows

    except (Exception, psycopg2.Error) as e:
        msg = f'Error while fetching data from PostgreSQL: {e}'
        dbe.close_connection(cur, conn)
        return {
            'error': True,
            'message': msg
        }

if __name__ == '__main__':
    uvicorn.run(app, host='0.0.0.0', port=8080)
