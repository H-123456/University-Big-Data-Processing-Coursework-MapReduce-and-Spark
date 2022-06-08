# The aim of this part is to show the average transaction value each month

# Reference: stackoverflow, https://stackoverflow.com/questions/9039961/finding-the-average-of-a-list (accessed 15th April 2022)

# Code also refers to some information/guidance from Big Data Science module

#Some further references: https://mrjob.readthedocs.io/en/stable/

# https://hadoop.apache.org/docs/r2.5.2/hadoop-mapreduce-client/hadoop-mapreduce-client-core/HadoopStreaming.html#Generic_Command_Options

# https://mrjob.readthedocs.io/en/latest/guides/writing-mrjobs.html

# Python Software Foundation, time – Time access and conversions, Python, 2001-2022, https://docs.python.org/3/library/time.html#time.strftime




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
