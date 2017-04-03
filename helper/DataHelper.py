import tushare
import numpy as np
import pandas as pd
from datetime import datetime, timedelta


class M5DataHelper (object):
    def __init__(self):
        self.stock_pool = tushare.get_stock_basics()

    def get_batch_list(self, start_date, end_date, interval=7):

        sd = datetime.strptime(start_date, '%Y-%m-%d')
        ed = datetime.strptime(end_date, '%Y-%m-%d')

        while sd <= ed:
            start = sd.strftime("%Y-%m-%d")
            end = (sd + timedelta(days=interval-1)).strftime("%Y-%m-%d")
            for stock in np.unique(self.stock_pool.index):
                name = self.stock_pool.ix[stock]['name']
                print stock, name, start, end
                df = tushare.get_hist_data(stock, ktype='5', start=start, end=end)
                if df is None or df.empty:
                    continue
                df['name'] = name
                yield (name, start, end, df.loc[:,['name','high', 'low', 'volume']].sort_index())
            sd += timedelta(days=interval)


if __name__ == "__main__":
    m5 = M5DataHelper()
    root = "~/source_code/stock/data"
    for (name, start, end, df) in m5.get_batch_list('2016-09-05', '2017-03-31'):
        path = "%s/m5_%s_%s_%s.csv" % (root, name, start, end)
        df.to_csv(path)

