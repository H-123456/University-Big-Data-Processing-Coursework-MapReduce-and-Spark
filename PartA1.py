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
