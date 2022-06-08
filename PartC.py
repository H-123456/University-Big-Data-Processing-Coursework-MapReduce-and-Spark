# ‘Evaluate the top 10 miners by the size of the blocks mined’

# Code also refers to some information/guidance from Big Data Science module

#Some further references: https://mrjob.readthedocs.io/en/stable/

# https://hadoop.apache.org/docs/r2.5.2/hadoop-mapreduce-client/hadoop-mapreduce-client-core/HadoopStreaming.html#Generic_Command_Options

# https://mrjob.readthedocs.io/en/latest/guides/writing-mrjobs.html

# https://stackoverflow.com/questions/8459231/sort-tuples-based-on-second-parameter




#followed a similar method to lab 4 here

from mrjob.job import MRJob
from mrjob.step import MRStep


class PartC(MRJob):

    def mapper1(self,_,line):
        fields = line.split(",") #using , since csv file
        try:
            if len(fields) == 9: #the blocks file has 9 columns
                miner = fields[2]
                size = float(fields[4]) #using float so can sum
                yield(miner,size)
        except:
            pass

    def reducer1(self,key,values):
        yield(key,sum(values))


    def mapper2(self,key,values):
        yield(None,(key,values))

    def reducer2(self,key,values):
        sorted_values = sorted(values,reverse=True, key=lambda x:x[1])
        i = 0
        for value in sorted_values:
            yield(value[0],value[1])
            i+=1
            if i >=10:
                break


    def steps(self): #similar to lab 4
        return [MRStep(mapper=self.mapper1,
                    reducer=self.reducer1),
                MRStep(mapper=self.mapper2,
                    reducer=self.reducer2)]

if __name__ == "__main__":
    PartC.run()
