# ‘Evaluate the top 10 smart contracts by total Ether received’

# References: stackoverflow, https://stackoverflow.com/questions/16310015/what-does-this-mean-key-lambda-x-x1 (accessed 15th April 2022)

# Python while loop, w3resource, https://www.w3resource.com/python/python-while-loop.php (accessed 15th April 2022)

# Code also refers to some information/guidance from Big Data Science module




from mrjob.job import MRJob
from mrjob.step import MRStep #as used in lab 4

class Partb(MRJob):
    def mapper1(self,_,line):
        try:
            if(len(line.split(','))==7): #the transactions file has 7 columns
                fields=line.split(',')
                join_key=fields[2] #to_address for transactions
                join_value=float(fields[3]) #the value in transactions, use float so can sum
                if join_value != 0: #we don't need to include the 0 transactions
                    yield(join_key,(1,join_value)) #label this input as 1

            elif(len(line.split(','))==5): #the contracts file
                fields=line.split(',')
                join_key=fields[0] #the address
                yield(join_key,(2,None)) #label this input as 2
        except:
            pass

    def reducer1(self,key,values):
        address_values = []
        in_contract = False
        for value in values:
            if value[0] == 1:
                address_values.append(value[1]) #the values collected from transactions
            if value[0]==2:
                in_contract = True
        if in_contract and sum(address_values)!=0:
            yield(key,sum(address_values))


            #for the contracts return address and sum of the values appearing for
            #that address in the transactions file

    def mapper2(self,key,values):
        yield(None,(key,values))  #we want everything in the key so can sort


    def reducer2(self,key,values):
        sorted_values = sorted(values,reverse=True, key=lambda x:x[1]) #similar to lab 4
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
    Partb.run()
