#%%
class Base:
    def __init__(self, env) -> None:
        self.env = env


    def connect_db(self):
        import mariadb
        try:
            self.connection = mariadb.connect(user="admin", password="upilot_123",
                                          host="bds-upilot.cmo7yug22aho.ap-south-1.rds.amazonaws.com", port=3306, database="demo1")
            self.cursor = self.connection.cursor()
        except Exception as e:
            return "Error: "+str(e)
    


    def run_query_pandas(self, query):
        import pandas as pd
        try:
            data = pd.read_sql(query, self.connection)
            return data
        except Exception as e:
            return "Error: "+str(e)

    def set_date(self,time):
        try:
            self.time=time
            month,year=time[0],str(time[1])
            d={"January":"01","February":"02","March":"03","April":"04",
            "May":"05","June":"06","July":"07","August":"08",
            "September":"09","October":"10","November":"11","December":"12"}
            date=year+"-"+d[month]+"-01 00:00:00"
            return date
        except Exception as e:
            return "Error: "+str(e)
# %%
