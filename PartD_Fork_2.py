# Aim to find a fork and analyse its effect

# References: Fork (blockchain), Wikipedia, https://en.wikipedia.org/wiki/Fork_(blockchain) (accessed 15th April 2022)

# Python Software Foundation, time – Time access and conversions, Python, 2001-2022, https://docs.python.org/3/library/time.html#time.strftime (accessed 15th April 2022)

# Code also refers to some information/guidance from Big Data Science module

#Some further references: https://mrjob.readthedocs.io/en/stable/

# https://hadoop.apache.org/docs/r2.5.2/hadoop-mapreduce-client/hadoop-mapreduce-client-core/HadoopStreaming.html#Generic_Command_Options

# https://mrjob.readthedocs.io/en/latest/guides/writing-mrjobs.html




#Appeared to be a fork on 22nd December 2017

#seeing how average gas price varied for days surrounding the fork



from mrjob.job import MRJob


import time

class PartD_Fork(MRJob):

    def mapper(self,_,line):
        fields = line.split(',')
        try:
            if len(fields) == 7: #transactions file
                time_stamp = fields[6]
                time_stamp = float(time_stamp)
                year = time.strftime("%Y",time.gmtime(time_stamp))
                month = time.strftime("%m",time.gmtime(time_stamp))
                day = time.strftime("%d",time.gmtime(time_stamp))
                gas_price = float(fields[5])
                day = int(day) #so can consider if before or after day
                if year == "2017" and month == "12" and (day > 16) and (day<30):
                    yield(day,gas_price) #day the key and gas_price the value
        except:
            pass

    def reducer(self,key,values):
        gas_prices = []
        for value in values:
            gas_prices.append(value)
        average = sum(gas_prices)/len(gas_prices)
        yield(key,average) #for each day considering the average gas price


if __name__ == "__main__":
    PartD_Fork.run()
