# In this part the aim is to show the transactions occurring each month.

# Reference: Python Software Foundation, time – Time access and conversions, Python, 2001-2022, https://docs.python.org/3/library/time.html#time.strftime (accessed 15th April 2022)

# Code also refers to some information/guidance from Big Data Science module

#Some further references: https://mrjob.readthedocs.io/en/stable/ 

# https://hadoop.apache.org/docs/r2.5.2/hadoop-mapreduce-client/hadoop-mapreduce-client-core/HadoopStreaming.html#Generic_Command_Options

# https://mrjob.readthedocs.io/en/latest/guides/writing-mrjobs.html




from mrjob.job import MRJob

#Lab 3 supports with this question, for example on information about extracting time from the timestamp
#we run this on the blocks file


import time

class Part1a(MRJob):
    def mapper(self,_,line):
        fields = line.split(',') #similarly to that used in lab 3 but now with comma since csv file
        try:
            if len(fields) == 9: #since there are 9 fields in the blocks file
                time_stamp = fields[7]
                time_stamp = float(time_stamp)
                month = time.strftime("%m",time.gmtime(time_stamp))
                year = time.strftime("%Y",time.gmtime(time_stamp))
            #need to consider year as well so two months different years not considered same
                transactions = fields[8]
                transactions = float(transactions)
                yield((month,year),transactions) #(month,year) the key and transactions the value
        except:
            pass

    def reducer(self,key,values):
        yield(key,sum(values))


if __name__ == "__main__":
    Part1a.run()
