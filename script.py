import csv

with open('ClimateData.csv') as csvClimateDataFile:
    csvReader = csv.reader(csvClimateDataFile)
    climateList = list(csvReader)
    climateList = climateList[1:]

with open('FireData.csv') as csvFireDataFile:
    csvReader = csv.reader(csvFireDataFile)
    fireList = list(csvReader)
    fireList = fireList[1:]

# Task 1.1 It requires to search records in climate data on an exact date, where the distribution of records dosen't
# skew hard. So we can use hash partitioning to partition the data, where only one processer need to be activated for
#  selecting records on a certain day. Compared with Round Robin partitioning, as here everyday only has 1 record,
# and hashing basing on reminder of date divided by number of processers, hash partitioning dosen't skew hard. Linear
#  search method is applied as dates are not sorted (It might be sorted, but we could not assume that, as there might
#  be a delay for some station to report a record). Coding based on Week2 tutorial code

def s_hash11(x,n):
    # Convert last 2 digits of date(days) into int, hash base on reminder of it divided by number of processers
    x= int(x[-2:])
    result = x%n
    return result

# hash partition climate data into a dictionary with hashed data as keys.
def h_partition11(data, n):
    dic = {}
    for x in data:
        # As date is the second attribute of climate data, pass x[1] into the method
        h = s_hash11(x[1],n)
        if (h in dic.keys()):
            s = dic[h]
            s.append(x)
            dic[h] = s
        else:
            s = list(x)
            dic[h] = []
            dic[h].append(s)
    return dic

def linear_search11(data,key):
    matched_record = []
    for x in data:
        # As date is the second attribute of climate data, check x[1] for the match
        if x[1] == key:
            matched_record.append(x)
            break
    return matched_record


from multiprocessing import Pool  # For multiprocessing


def parallel_search11(data, date, n_processor):
    results = []
    pool = Pool(processes=n_processor)

    # Perform data partitioning first.Each element in DD has a pair (hash key: records)
    DD = h_partition11(data, n_processor)
    # Hash the date as the key to activate only certain part of dictionary that contains the record
    date_hash = s_hash11(date, n_processor)
    d = list(DD[date_hash])
    result = pool.apply(linear_search11, [d, date])
    # Although it is an exact match, we still check whether there are multiple results just in case
    results.extend(result)

    return results

# Task 1.2
# It requires to search Fire data for records that have surface temperature betweer 65 and 100 celcius.
# As we don't know the distribution of data, Round Robin would be the most suitable way to partition the data to achieve work balance.
# Firstly, we were thinking about using range partitioning, but as the original data is not sorted, and there is only one range to look into, and the range is passed as a parameter. So if we search in another range we need to do the partition again. So range partitioning has no advantage.
# Linear search is applied as the data is not sorted.
# Coding based on Week2 tutorial code

def rr_partition12(data, n):
    result = []

    for i in range(n):
        result.append([])

    n_bin = len(data) / n

    for index, element in enumerate(data):
        index_bin = (int)(index % n)
        result[index_bin].append(element)

    return result

def linear_search12(data,r):
    matched_record = []
    for x in data:
        # As surface tempeture is the 8th attribute of climate data, check x[7] for the match
        if ((int(x[7]) >= r[0]) and (int(x[7])<= r[1])):
            matched_record.append(x)
    return matched_record


from multiprocessing import Pool  # For multiprocessing


def parallel_search12(data, temperature, n_processor):
    results = []
    pool = Pool(processes=n_processor)

    # Perform data partitioning first.
    DD = rr_partition12(data, n_processor)
    # Go through all partitioned list for results
    for d in DD:
        result = pool.apply(linear_search12, [d, temperature])
        results.append(result)

    return results

def drawtable12(table):
    # header
    print("latitude | longitude | confidence | surface temperature")
    print( "--------------------------------------------------")
    # content
    for j in table:
        for i in j:
            # format based on the length of each attribute (for instance: '100' is 1 char longer than '80')
            print('\n'+str(i[0])+"    |  "[(len(str(i[0]))-5):]+str(i[1])+"    |     "[(len(str(i[1]))-5):]+str(i[5])+"     |    "[(len(str(i[5]))-2):]+str(i[7]))
    return

# Task 2.1
# We need to join Climate Data and Fire Data based on the Date attribute
# Because for two same dates, they must have the same hash key, so we decide to use hash based join method.
# For partitioning, because the dates that could match must have the same hash value, so we could use hash partitioning.
# Overall, we perform a disjoint partitioning-based parallel join algorithm.
# And then we draw the result in a more readable way.
# Coding based on Week3 tutorial code
# Hash join
def s_hashj21(x):
    # Convert last digit of date into int as key. Because more keys in hash table makes it faster for join. (2600records mapping to 30, average cost would be around (2600/30)+30)
    m = int(x[-2:])
    result = m
    return result


def HB_join21(FD, CD):
    result = []

    dic = {}

    # For each record in a sublist of partitioned Climate Data, we hash them into a table, because it is smaller compared with Fire Data
    for cr in CD:
        # Hash the record based on Date using hash function s_hashj21 into hash table
        cr_key = s_hashj21(cr[1])
        if (cr_key in dic.keys()):
            s = dic[cr_key]
            s.append(cr)
            dic[cr_key] = s
        else:
            s = list(cr)
            dic[cr_key] = []
            dic[cr_key].append(s)

    # For each record in a sublist of partitioned Fire Data
    for fr in FD:
        # Hash the record based on Date using s_hashj21
        fr_key = s_hashj21(fr[6])

        # If an index entry is found Then
        # Compare each record on this index entry with the record of table hashed Clinate Data
        # If the key is the same then put needed attributes value into the result
        if fr_key in dic:
            for s in dic[fr_key]:
                if fr[6] == s[1]:
                    result.append({", ".join([s[1], s[2], s[3], s[5], fr[7]])})

    return result
# Hash partition
def s_hashp21(x,n):
    # Convert last digit of date into int, hash base on reminder of it divided by number of processers
    k = int(x[-2:])
    result = k%n
    return result

def h_partition21(data, n,k):
    dic = {}
    for x in data:
        # As date is the second attribute of climate data, pass x[1] into the method
        h = s_hashp21(x[k],n)
        if (h in dic.keys()):
            s = dic[h]
            s.append(x)
            dic[h] = s
        else:
            s = list(x)
            dic[h] = []
            dic[h].append(s)
    return dic


# Include this package for parallel processing
import multiprocessing as mp


def DPBP_joinhash(FD, CD, n_processor):
    result = []

    # Partition fireList and climateList into sub-tables using h_partition21
    FD_subsets = h_partition21(FD, n_processor, 6)
    CD_subsets = h_partition21(CD, n_processor, 1)
    # Apply local join for each processor
    pool = mp.Pool(processes=n_processor)

    # For each subset that share the same hash key, we do local join
    for i in range(0, n_processor):
        result.append(pool.apply(HB_join21, [FD_subsets[i], CD_subsets[i]]))

    return result
def drawtable21(table):
    # header
    print("    Date   | Air Temperature | Relative Humidity | Max Wind Speed | Surface temperature")
    print( "-----------------------------------------------------------------------------------------")
    # content
    for j in table:
        for k in j:
            i = list()
            for n in k:
                i = n.split(",")
                # format based on the length of each attribute (for instance: '100' is 1 char longer than '80')
                print('\n'+str(i[0])+"    |      "[(len(str(i[0]))-7):]+str(i[1])+"           |       "[(len(str(i[1]))):]+str(i[2])+"          |     "[(len(str(i[2]))-2):]+str(i[3])+"         |    "[(len(str(i[3]))-2):]+str(i[4]))
    return

# Task 2.2
# This one is similar to Task 2.1, only difference is a range of confidence need to be passed as a parameter.
# Hash join
def s_hashj22(x):
    # Convert last digit of date into int as key. Because more attributes in hash table makes it faster for join.
    m = int(x[-2:])
    result = m
    return result


# r1 r2 refers to the range of tempreature
def HB_join22(FD, CD, r1, r2):
    result = []
    dic = {}
    for cr in CD:
        cr_key = s_hashj21(cr[1])
        if (cr_key in dic.keys()):
            s = dic[cr_key]
            s.append(cr)
            dic[cr_key] = s
        else:
            s = list(cr)
            dic[cr_key] = []
            dic[cr_key].append(s)

    for fr in FD:
        fr_key = s_hashj21(fr[6])
        if fr_key in dic:
            for s in dic[fr_key]:
                if fr[6] == s[1]:
                    # Check if confidence is in the given range
                    if int(fr[5]) in range(int(r1), int(r2)):
                        result.append({", ".join([fr[3], s[2], fr[2], fr[5]])})

        ### END CODE HERE ###

    return result


# Include this package for parallel processing
import multiprocessing as mp


def DPBP_joinhash22(FD, CD, n_processor, r1, r2):
    result = []

    # hash based on date
    FD_subsets = h_partition21(FD, n_processor, 6)
    CD_subsets = h_partition21(CD, n_processor, 1)

    pool = mp.Pool(processes=n_processor)

    for i in range(0, n_processor):
        result.append(pool.apply(HB_join22, [FD_subsets[i], CD_subsets[i], r1, r2]))
    return result

def drawtable22(table):
    # header
    print("    Date Time        | Air Temperature | Surface temperature | Confidence ")
    print( "-------------------------------------------------------------------------")
    # content
    for j in table:
        for k in j:
            i = list()
            for n in k:
                i = n.split(",")
            # format based on the length of each attribute (for instance: '100' is 1 char longer than '80')
            print('\n'+str(i[0])+"      |      "[(len(str(i[0]))-15):]+str(i[1])+"           |       "[(len(str(i[1]))):]+str(i[2])+"          |   "[(len(str(i[2]))-4):]+str(i[3]))
    return

# Task 3
# We want to use an algorithm that could make the merge phase for sorted sub_lists not too expensive, so we somehow establish a tricky way to do parallel partition sort (5th method introduced in the lecture)which dosen't need final merge at all.
# To do parallel partition sort, we need to firstly redistribute records into ranges of surface tempReture, we use a hash function to do this, which might incure skew.
# Coding based on Week5 tutorial code
def qsortst(arr):
    if len(arr) <= 1:
        return arr
    else:
        leftarr = qsortst([x for x in arr[1:] if int(x[7]) <= int(arr[0][7])])
        result = leftarr
        result.append(arr[0])
        rightarr = qsortst([x for x in arr[1:] if int(x[7]) > int(arr[0][7])])
        result.extend(rightarr)

        # Didn't know that using + could work the same
        # result = qsortst([x for x in arr[1:] if int(x[7]) <= int(arr[0][7])]) + [arr[0]]+ qsortst([x for x in arr[1:] if int(x[7]) > int(arr[0][7])])
        return result


import sys


# Find the index of smallest record
def find_min(records):
    m = records[0]
    index = 0
    for i in range(len(records)):
        if (int(records[i][7]) < int(m[7])):
            index = i
            m = records[i]
    return index


def k_way_merge(record_sets):
    # indexes will keep the indexes of sorted records in the given buffers
    indexes = []
    for x in record_sets:
        indexes.append(0)  # initialisation with 0

    # final result will be stored in this variable
    result = []

    # the merging unit (i.e. # of the given buffers)
    tuple = []

    # Create a record that is in the same format of records with maxsize as compare value
    mx = list()
    mx = [0 for x in range(8)]
    mx[7] = sys.maxsize

    while (True):
        tuple = []  # initialise tuple

        # This loop gets the current position of every buffer
        for i in range(len(record_sets)):
            if (indexes[i] >= len(record_sets[i])):
                tuple.append(mx)  # append a record that have a same max value
            else:
                tuple.append(record_sets[i][indexes[i]])

                # find the smallest record
        smallest = find_min(tuple)

        # if we only have a record whichs surface tempreture is sys.maxsize on the tuple, we reached the end of every record set
        if (tuple[smallest][7] == sys.maxsize):
            break

        # This record is the next on the merged list
        result.append(record_sets[smallest][indexes[smallest]])
        indexes[smallest] += 1

    return result


# Similar way with toturial example

def serial_sorting3(dataset, buffer_size):
    if (buffer_size <= 2):
        print("Error: buffer size should be greater than 2")
        return

    result = []
    tr = []

    # --- Sort Phase ---
    sorted_set = []

    # Read buffer_size pages at a time into memory and
    # sort them, and write out a sub-record set (i.e. variable: subset)
    start_pos = 0
    N = len(dataset)

    while True:
        if ((N - start_pos) > buffer_size):
            # read B-records from the input, where B = buffer_size
            subset = dataset[start_pos:start_pos + buffer_size]
            # sort the subset (using qucksort defined above)
            sorted_subset = qsortst(subset)
            sorted_set.append(sorted_subset)
            start_pos += buffer_size
        else:
            # read the last B-records from the input, where B is less than buffer_size
            subset = dataset[start_pos:]
            # sort the subset (using qucksort defined above)
            sorted_subset = qsortst(subset)
            sorted_set.append(sorted_subset)
            break

    # --- Merge Phase ---
    merge_buffer_size = buffer_size - 1
    dataset = sorted_set

    while True:
        merged_set = []
        N = len(dataset)
        start_pos = 0
        while True:
            if ((N - start_pos) > merge_buffer_size):
                # read C-record sets from the merged record sets, where C = merge_buffer_size
                subset = dataset[start_pos:start_pos + merge_buffer_size]
                merged_set.append(k_way_merge(subset))  # merge lists in subset
                start_pos += merge_buffer_size
            else:
                # read C-record sets from the merged sets, where C is less than merge_buffer_size
                subset = dataset[start_pos:]
                merged_set.append(k_way_merge(subset))  # merge lists in subset
                break

        dataset = merged_set
        if (len(dataset) <= 1):  # if the size of merged record set is 1, then stop
            tr = merged_set
            break

    result = tr[0]

    return result

def findMax(data):
    k = 0
    m = 0
    for x in data:
        k = int(x[7])
        if m < k:
            m = k
    return m

def findMin(data):
    k = 0
    m = int(data[0][7])
    for x in data:
        k = int(x[7])
        if m > k:
            m = k
    return m

def s_hash3(x,ma,mi,n):
    r = (ma - mi)//n + 1
    k = (x - mi)//r
    return k

# Partation fire data into range of surface tempreture
def h_partition3(data, n):
    dic = {}
    ma = findMax(data)
    mi = findMin(data)
    for x in data:
        h = s_hash3(int(x[7]),ma,mi,n)
        if (h in dic.keys()):
            s = dic[h]
            s.append(x)
            dic[h] = s
        else:
            s = list(x)
            dic[h] = []
            dic[h].append(s)
    return dic


import multiprocessing as mp


def parallel_partition_sorting3(dataset, n_processor, buffer_size):
    if (buffer_size <= 2):
        print("Error: buffer size should be greater than 2")
        return

    result = []

    subsets = h_partition3(dataset, n_processor)
    pool = mp.Pool(processes=n_processor)
    r = []
    for x in range(0, n_processor):
        r = pool.apply(serial_sorting3, [subsets[x], buffer_size])
        # This is the final merge phase
        # Because sorted subets are in range already so we just need to add them in order
        # To aviod reading too much input that is larger than buffer size, we use extend(), which in python is a loop of append(),that only read 1 record into the memory to process each time.
        result.extend(r)

    return result

def drawtable3(table):
    # header
    print("Latitude | Longitude |  ST(k)  |         Datetime        |  Power  | Conf  |     Date     | ST(C)")
    print( "---------------------------------------------------------------------------------------------------")
    # content
    for i in table:
        #print(i)
                # format based on the length of each attribute (for instance: '100' is 1 char longer than '80')
        print('\n'+str(i[0])+"    |   "[(len(str(i[0]))-5):]+str(i[1])+"   |  "[(len(str(i[1]))-5):]+str(i[2])+\
              "    |  "[(len(str(i[2]))-3):]+str(i[3])+"          |  "[(len(str(i[3]))-13):]+str(i[4])+\
              "     |   "[(len(str(i[4]))-2):]+str(i[5])+"  |  "[(len(str(i[5]))-2):]+str(i[6])+\
              "       |  "[(len(str(i[6]))-5):]+str(i[7]))
    return

# Task 4.1
# We need to get fire numbers for each day, we create a dictionary for group by results with date as key and fire number as value.

def local_groupby41(data):
    dic = {}
    for x in data:
        key = x[6]
        # For fire record with date key, we add its value by 1
        if (key in dic.keys()):
            dic[key] += 1
        else:
            dic[key] = 0
            dic[key] += 1
    return dic


import multiprocessing as mp


def parallel_merge_all_groupby41(FD, n_processor):
    result = {}

    # we use round robin partition in task 1.2 to partition fire data
    dataset = rr_partition12(FD, n_processor)

    pool = mp.Pool(processes=n_processor)

    local_result = []
    for s in dataset:
        local_result.append(pool.apply(local_groupby41, [s]))
    pool.close()

    # global aggregation
    for i in local_result:
        for j in i.keys():
            if (j in result.keys()):
                result[j] += i[j]
            else:
                result[j] = 0
                result[j] = i[j]

    return result

def drawtable41(dic):
    # header
    print("      Date       |   Number of Fires")
    print( "----------------------------------------")
    # content
    for i in dic.keys():
        print('\n   '+ str(i) +"       |       "[(len(str(i))-7):]+str(dic[i]))
    return

# Task 4.2
# This task is similar to 4.1, the only diifference is apart from the fire number, we also store the sum of surfance tempreature for each day.
def local_groupby42(data):
    dic = {}
    for x in data:
        key = x[6]
        if (key in dic.keys()):
            dic[key][0] += 1
            dic[key][1] += int(x[7])
        else:
            # for each day key, it maps to a list with two elements, where list[0] is number of fires, list[1] is sum of surface tempreatures.
            dic[key] = [0,0]
            dic[key][0] += 1
            dic[key][1] += int(x[7])
    return dic


def getaverageST42(FD, n_processor):
    result = {}

    dataset = rr_partition12(FD, n_processor)

    pool = mp.Pool(processes=n_processor)

    local_result = []
    for s in dataset:
        local_result.append(pool.apply(local_groupby42, [s]))
    pool.close()

    # global aggreation
    for i in local_result:
        for j in i.keys():
            if (j in result.keys()):
                result[j][0] += i[j][0]
                result[j][1] += i[j][1]
            else:
                result[j] = [0, 0]
                result[j][0] += i[j][0]
                result[j][1] += i[j][1]

    return result

def drawtable42(dic):
    # header
    print("      Date       |   Average Surface Temperature")
    print( "-----------------------------------------------")
    # content
    for i in dic.keys():
        print('\n   '+ str(i) +"       |       "[(len(str(i))-7):]+str((dic[i][1]//dic[i][0])))
    return

# Task 5
# In this task we need to get average surface tempreature for stations, where the join attribute is date, and group by attribute is station.
# So we decide to use group by after join as join and group by attributes are different.
# We use an join attribute schema, where we partition based on join attributes and do local aggregation after local join and then do global aggregation.
# group by after hash join
def group_by_after_HB_join5(FD, CD):
    result = {}

    dic = {}

    # For each record in a sublist of partitioned Climate Data, we hash them into a table, because it is smaller compared with Fire Data
    for cr in CD:
        # Hash the record based on Date using hash function s_hashj21 into hash table
        cr_key = s_hashj21(cr[1])
        if (cr_key in dic.keys()):
            s = dic[cr_key]
            s.append(cr)
            dic[cr_key] = s
        else:
            s = list(cr)
            dic[cr_key] = []
            dic[cr_key].append(s)

    # For each record in a sublist of partitioned Fire Data
    for fr in FD:
        # Hash the record based on Date using s_hashj21
        fr_key = s_hashj21(fr[6])

        if fr_key in dic:
            for s in dic[fr_key]:
                # Check for matched records to join
                if fr[6] == s[1]:
                    # If a match is found, then we extract information we need from this match to groupby.
                    # When this method is carried parallely, it skips the global redistribution of join results.
                    # In another words, the group by use the same partition with join. After partition, a local join is counducted, then a group by is counducted based on it, and then we aggregate the global result.
                    # In this case, we group surface tempreture in fire data by station in climate data.
                    # Then we store the result in a dictionary with station as its keys.
                    k = 0
                    if s[0] in result.keys():
                        result[s[0]][0] += 1
                        result[s[0]][1] += int(fr[7])
                    else:
                        # result[station][number of fires, sum of surface tempreture]
                        result[s[0]] = [0, 0]
                        result[s[0]][0] += 1
                        result[s[0]][1] += int(fr[7])

    return result


# Include this package for parallel processing
import multiprocessing as mp


def groupby_DPBP_joinhash5(FD, CD, n_processor):
    local_result = []
    result = {}

    # Partition fireList and climateList into sub-tables using h_partition21
    FD_subsets = h_partition21(FD, n_processor, 6)
    CD_subsets = h_partition21(CD, n_processor, 1)
    # Apply local join for each processor
    pool = mp.Pool(processes=n_processor)

    # For each subset that share the same hash key, we do local join + local group_by
    for i in range(0, n_processor):
        local_result.append(pool.apply(group_by_after_HB_join5, [FD_subsets[i], CD_subsets[i]]))
    pool.close

    # Global aggregation of local group_by results for each processer
    for i in local_result:
        for j in i.keys():
            if j in result.keys():
                result[j][0] += i[j][0]
                result[j][1] += i[j][1]
            else:
                result[j] = [0, 0]
                result[j][0] += i[j][0]
                result[j][1] += i[j][1]

    return result

def drawtable5(dic):
    # header
    print("  Station    |   Average Surface Temperature")
    print( "-----------------------------------------------")
    # content
    for i in dic.keys():
        print('\n   '+ str(i) +"       |           "[(len(str(i))-3):]+str((dic[i][1]//dic[i][0])))
    return