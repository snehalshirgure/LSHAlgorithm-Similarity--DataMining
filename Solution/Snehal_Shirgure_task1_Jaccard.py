from pyspark import SparkContext
from random import randint
import itertools
import collections
import sys
import csv
import math
import time

inputfile = sys.argv[1]

sc = SparkContext('local[10]','task1')

t = time.time()

infile = sc.textFile(inputfile)

rdd1 = infile.map( lambda x: x.split(',') )\
            .filter( lambda x: 'userId' not in x )\
            .map( lambda x: ( int(x[0]) , [int(x[1])] ) )\
            .reduceByKey( lambda x,y: x+y)

rdd2 = infile.map( lambda x: x.split(',') )\
            .filter( lambda x: 'userId' not in x )\
            .map( lambda x: ( int(x[1]) , [int(x[0])] ) )\
            .reduceByKey( lambda x,y: x+y)

userlist =  rdd1.sortByKey(ascending=True).collect()
movielist = rdd2.sortByKey(ascending=True).collect()
movielistdict = dict(movielist)
# print movielistdict

users = len(userlist)
movies = len(movielist)

# print ("last userid :---> " + str(userlist[users-1][0]))
# print ("last movieid :---> " + str(movielist[movies-1][0]))

lastuserid = userlist[users-1][0]
lastmovieid = movielist[movies-1][0]

m = lastuserid + 1
#prime number ~
#p= 89

#store all minhash values 
minhash = []
numminhash = 60


#for each min hash function ~
for index in range(0,numminhash):
    
    #formula --> ((ax+b)%p)m
    a= randint(0,1000)
    b= randint(0,1000)
    
    # print("a is: "+str(a) + " b is: "+str(b))

    #initialise dictionary of movieids to infinity(max+1)~
    hashvalues = {}
    for mid in range(movies):
            hashvalues[movielist[mid][0]] = m

    #iterate over all userids ~
    for u in range(users):
        #generate random minhash for current userid ~
        u_mh = (a*u+b)%m   
        #print (str(u) + "-->" + str(u_mh))
        for i in userlist[u][1]:
            if(hashvalues[i] > u_mh):
                hashvalues[i] = u_mh
        #print hashvalues

    minhash.append(hashvalues)

# print minhash

rdd3 = sc.parallelize(minhash)

result = rdd3.map(lambda x: [(k , [x[k]]) for k in x] ).reduce(lambda x,y: x+y)

# print result

rdd4 = sc.parallelize(result)
rdd5 = rdd4.reduceByKey(lambda x,y : x+y)
# for each in rdd5.collect():
#       print each

rdd6 = rdd5.map(lambda x: [ [ ( x[0], x[1][i] ) for i in range(itr,itr+3) ] for itr in range(0,58,3) ] )

bandlist = rdd6.collect()

# for each in bandlist:
#     print each

bucketlist = {}

for index in range(0,20):
    buckets={}
    for bands in bandlist:
        # print bands[index]
        mid = bands[index][0][0]
        # print ("m-id is ------->" + str(mid))
        str1 =''
        for each in bands[index]:
            str1 += ''.join(str(each[1]))
        
        hashbin = hash(str1)

        # print ("hash is : --- "+ str(hashbin))
        if hashbin not in buckets:
            buckets[hashbin] = [mid]
        else:
            buckets[hashbin]+= [mid]
        
    bucketlist[index] = buckets.values()
    del buckets 

# print bucketlist

rdd7 = sc.parallelize(list(bucketlist.values())).reduce(lambda x,y: x+y)
# print rdd7
rdd8 = sc.parallelize(rdd7).filter(lambda x: len(x) > 1 ).map(lambda x: list(set(x))).map(lambda x:  list(itertools.combinations(x,2))).reduce(lambda x,y:x+y)
# print rdd8

finalpairs = rdd8

similarity = {}

def jaccard(x,y):
    s1 = set(x)
    s2 = set(y)
    return ((len(s1&s2)/float(len(s1|s2))))


for pair in finalpairs:
    a = pair[0]
    b = pair[1]
    
    l1 = movielistdict[pair[0]]
    l2 = movielistdict[pair[1]]
    jaccard_value = jaccard(l1,l2)
    if(jaccard_value>=0.5):
        if(a<b):
            if (a,b) not in similarity:
                similarity[(a,b)] = jaccard_value        
        else:
            if (b,a) not in similarity:
                similarity[(b,a)] = jaccard_value 



print "Time taken -----------"
print(time.time()-t)


# print similarity

orderedsimilarity = collections.OrderedDict(sorted(similarity.items()))

# print orderedsimilarity

outputfile = open("Snehal_Shirgure_SimilarMovies_Jaccard.txt","w+")

for key,value in orderedsimilarity.iteritems():
    outputfile.write(str(key[0])+", "+str(key[1])+", "+str(value))
    outputfile.write("\n")

outputfile.close()

# #precision and recall ~

# groundtruthfile = "similarmovies.csv"

# infile2 = sc.textFile(groundtruthfile)

# similarmovies = infile2.map( lambda x: x.split(',') )\
#                 .map( lambda x: ( ( int(x[0]) , int(x[1]) ), 1) )\
#                 .reduceByKey(lambda x,y: x+y)

# similarmoviesdict = dict(similarmovies.collect())

# tp=0
# fp=0
# fn=0

# for key in similarmoviesdict:
#     tp+=1
#     if key not in similarity:
#         fn+=1

# for key in similarity:
#     if key not in similarmoviesdict:
#         fp+=1
    
# print "----------"
# print tp
# print fp
# print fn

# print "\n\nPrecision -----"
# print (tp/float(tp+fp))

# print "\n\nRecall -----"
# print (tp/float(tp+fn))
        


