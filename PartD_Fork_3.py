#see how the gas used changed for days surrounding the fork



from mrjob.job import MRJob


import time

class PartD_Fork(MRJob):

    def mapper(self,_,line):
        fields = line.split(',')
        try:
            if len(fields) == 9: #blocks file
                time_stamp = fields[7]
                time_stamp = float(time_stamp)
                year = time.strftime("%Y",time.gmtime(time_stamp))
                month = time.strftime("%m",time.gmtime(time_stamp))
                day = time.strftime("%d",time.gmtime(time_stamp))
                gas_used = float(fields[6])
                day = int(day)
                if year == "2017" and month == "12" and (day > 16) and (day<30):
                    yield(day,gas_used) #day the key and gas_used the value
        except:
            pass

    def reducer(self,key,values):
        yield(key,sum(values)) #summing the total gas used for each day




if __name__ == "__main__":
    PartD_Fork.run()
