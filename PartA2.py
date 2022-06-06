from mrjob.job import MRJob

import time

#This is similar to in part a but now we use the transaction file

class Part1b(MRJob):
    def mapper(self,_,line):
        fields = line.split(',') #similarly to that used in lab 3 but now with comma since csv file
        try:
            if len(fields) == 7: #since there are 7 fields in the transactions file
                time_stamp = fields[6]
                time_stamp = float(time_stamp)
                month = time.strftime("%m",time.gmtime(time_stamp))
                year = time.strftime("%Y",time.gmtime(time_stamp))
            #need to consider year as well so two months different years not considered same
                transaction_value = fields[3]
                transaction_value = float(transaction_value)
                if transaction_value != 0: #we do not want to include the 0 transactions since this is when transaction created
                    yield((month,year),transaction_value) #(month,year) the key and transactions value the value
        except:
            pass

    def reducer(self,key,values):
        transaction_values = []
        for value in values:
            transaction_values.append(value)
        average = sum(transaction_values)/len(transaction_values)
        yield(key,average)

if __name__ == "__main__":
    Part1b.run()
