#!python3

from mrjob.job import MRJob
from mrjob.step import MRStep

import csv
import json

cols = 'id_transaction,id_customer,name_customer,gender_customer,country_customer,birthdate_customer,date_transaction,Type,product_transaction,amount_transaction'.split(',')

def csv_readline(line):
    """Given a sting CSV line, return a list of strings."""
    for row in csv.reader([line]):
        return row

class Jumlahbarangterbeli(MRJob):

    def mapper(self, _, line):
        # Convert each line into a dictionary
        row = dict(zip(cols, csv_readline(line)))

        #skip first row as header
        if row['Product'] != 'Product':
            # Yield the order_date
            yield row['Product'],1

    def reducer(self, key, values):
        #for 'order_date' compute
        yield None, (key,sum(values))
   
if __name__ == '__main__':
    Jumlahbarangterbeli.run()

