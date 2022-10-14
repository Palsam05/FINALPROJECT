#!python3

from functools import total_ordering
from optparse import Values
from mrjob.job import MRJob
from mrjob.step import MRStep

import csv
import json

#displit dengan menggunakan ,
cols = 'id_transaction,id_customer,name_customer,gender_customer,country_customer,birthdate_customer,date_transaction,Type,product_transaction,amount_transaction'.split(',')

def csv_readline(line):
    """Given a sting CSV line, return a list of strings."""
    for row in csv.reader([line]):
        return row

class Jumlahpelangganmemebeli(MRJob):

    #step ini optional bisa dihapus,dan step ini untuk sort
    def steps(self):
        return [
            MRStep(mapper=self.mapper, reducer=self.reducer),
            MRStep(reducer=self.sort)


    def mapper(self, _, line):
        # Convert each line into a dictionary
        row = dict(zip(cols, csv_readline(line)))

        #skip first row as header
        if row['id_customer'] != 'id_customer':
            yield date_transaction[0:7], int(id_customer)

    def reducer(self, key, values):
        total = 0
        for x in Values:
            total +=1
            yield key,total
        #for 'order_date' compute
        yield key,sum(values)

if __name__ == '__main__':
    Jumlahpelangganmemebeli.run()

