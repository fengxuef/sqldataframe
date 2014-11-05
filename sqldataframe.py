"""
independant to DB implementations
"""
from sqlalchemy import create_engine, inspect
import pandas as pd

class SqlEngine:
  """
  wraper class of sqlalchemy engine
  """
  @classmethod
  def create(cls, uri):
    con = create_engine(uri)
    return cls(con)
  @classmethod
  def copy(cls, other):
    """
    create and copy from another item in as collection
    """
    return cls(other._connection)
  def __init__(self, con):
    self._db_uri = con.url
    self._connection = con
    self._inspector = inspect(self._connection)

  def query(self, sql):
    return pd.read_sql_query(sql, self._connection)
  def insert(self, name, df):
    df.to_sql(name, self._connection)