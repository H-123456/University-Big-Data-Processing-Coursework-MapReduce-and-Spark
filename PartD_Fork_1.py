# Aim to find a fork and analyse its effect

# References: Fork (blockchain), Wikipedia, https://en.wikipedia.org/wiki/Fork_(blockchain) (accessed 15th April 2022)

# Python Software Foundation, time – Time access and conversions, Python, 2001-2022, https://docs.python.org/3/library/time.html#time.strftime (accessed 15th April 2022)

# Code also refers to some information/guidance from Big Data Science module




#looking for when miners at same block at same day and what day happened

from mrjob.job import MRJob


import time

class PartD_Fork(MRJob):

    def mapper(self,_,line):
        fields = line.split(',')
        try:
            if len(fields) == 9: #since there are 9 fields in the blocks file
                block_number = fields[0]
                miner = fields[2]
                time_stamp = fields[7]
                time_stamp = float(time_stamp)
                year = time.strftime("%Y",time.gmtime(time_stamp))
                month = time.strftime("%m",time.gmtime(time_stamp))
                day = time.strftime("%d",time.gmtime(time_stamp))
                time_summary = (year,month,day)
                yield((time_summary,block_number),miner) #(month,year,day) and block number the key and miner the value
        except:
            pass

    def reducer(self,key,values):
        miners = []
        for value in values:
            miners.append(value) #making a list of miners for each key
        if len(miners) > 1: #looking for when more than 2 miners at same block at same day
            yield(key,miners)


if __name__ == "__main__":
    PartD_Fork.run()
