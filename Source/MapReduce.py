# MapReduce.py

"""
Performs a MapReduce-like task to complete three objectives.
Data necessary for each taskis imported from two csv files,
"AComp_Passenger_data.csv" and "Top30_airports_LatLong.csv" Each
task features a separate mapper and reducer to map and reduce the
 ppropriate keys and values for each task. Mapper functions will
take an input and create a tuple of (key, value), intermediate functions
where necessary will group repeated keys and have their values grouped
into a list, and finally the reducer functions are applied to reduce
key, value pairs to a smaller data set. The output is exported as a
csv file. The tasks are defined as below:
Task 1 - Determines the number of flights from each airport,
including list of airports not used, outputs as t1.csv
Task 2 - Creates a list of flights based on flightID, output includes
IATA/FAA codes, departure time, arrival time and flight duration
Task 3 - Calculates the number of passengers on each flight
"""

import csv
import itertools
import os
import re
import sys
import time

# globals
dataset = []
lookup = []
flight_cache = []
airport_cache = []
# set file path, frozen means the python script has been converted to exe form
if getattr(sys, 'frozen', False):
    file_path = os.path.dirname(sys.executable)
elif __file__:
    file_path = os.path.dirname(__file__)

# file reader, reads both CSV files
def importCSV(data_file, lookup_file):
    global file_path
    print("Reading " + data_file + "...")
    data_path = os.path.join(file_path, data_file)
    with open(data_path, "r", encoding="utf-8-sig") as read_data:
        read = csv.reader(read_data)
        global dataset
        dataset = list(read)
    print(data_file + " imported")
    print("Reading " + lookup_file + "...")
    lookup_path = os.path.join(file_path, lookup_file)
    with open(lookup_path, "r", encoding="utf-8-sig") as read_lookup:
        read2 = csv.reader(read_lookup)
        global lookup
        lookup = list(read2)
    print(lookup_file + " imported")


# file writer, write output of tasks to CSV
def outputCSV(content, outfile):
    global file_path
    with open(file_path + "\\" + outfile, "w", newline="") as write_file:
        print("Writing " + outfile + " to " + file_path + "...")
        write = csv.writer(write_file)
        if outfile == "t1.csv":
            write.writerow(["Airport", "Departures"])
        if outfile == "t2.csv":
            write.writerow(["Flight ID", "Departure", "Departure Time", "Arrival", "Arrival Time", "Flight Duration", "Passengers"])
        if outfile == "t3.csv":
            write.writerow(["Flight ID", "Passengers"])
        for key, value in content.items():
            if isinstance(value, int):
                write.writerow([key, value])
            else:
                write.writerow([key, value[0], value[1], value[2], value[3], value[4], value[5][0]])
                for item in value[5][1:]:
                    write.writerow(["","","","","","",item])
        print(outfile + " writing complete")

# remove malformed data:
#   remove empty lines
#   convert characters to uppercase
#   remove punctuation by deleting whole line
#   validate match to XXXnnnnX for flight id
def remove_errors(data, punc_flag, flightID_flag):

    def convert_uppper(list):
        copy_list = []
        temp_row = []
        for row in list:
            for col in row:
                temp_row.append(col.upper())
            copy_list.append(temp_row)
            temp_row = []
        list = copy_list
        return list

    def remove_punc(list):
        print("Removed for invalid punctuation:")
        for row in list:
            for col in row:
                if not re.match("^[a-zA-Z0-9]+$", col):
                    print(row)
                    list.remove(row)
        return list

    def validateFlightID(list):
        print("Removed for invalid flightID format:")
        for row in list:
            invalid = False
            # make sure fligtid is the right length
            if len(row[1]) == 8:
                # check alphabet chars
                if row[1][0].isdigit() or row[1][1].isdigit() or row[1][2].isdigit() or row[1][7].isdigit():
                    invalid = True
                # check digit chars
                if row[1][3].isalpha() or row[1][4].isalpha() or row[1][5].isalpha() or row[1][6].isalpha():
                    invalid = True
            else:
                invalid = True
            if invalid:
                print(row)
                data.remove(row)
        return list

    for row in data:
        if row:
            if row[0] == "":
                data.remove(row)
        else:
            data.remove(row)

    data = convert_uppper(data)
    if punc_flag:
        data = remove_punc(data)
    if flightID_flag:
        data = validateFlightID(data)

    return data

# convert epoch to dd-mm-yyyy HH/MM/SS
def epoch_to_datetime(epoch):
    return time.strftime("%d %b %Y %H:%M:%S", time.gmtime(epoch))

# convert epoch to HH/MM/SS
def epoch_to_time(epoch):
    return time.strftime("%H:%M:%S", time.gmtime(epoch))

# task 1 - determine number of flights from each airport
def t1():
    global dataset
    global lookup
    global airport_cache
    intermediate = []
    groups = {}
    output = {}

    # start map
    print("Starting mapper...")
    for row in dataset:
        map = t1mapper(row[2], row[1])
        if map is not None:
            intermediate.append(map)
    for row in lookup:
        if not row[1] in airport_cache:
            map = t1mapper(row[1], "-")
            if map is not None:
                intermediate.append(map)
    print("Mapping finished")

    # group values by key
    # [(a, b), (a, c), (d, e)] -> [(a, (b, c)), (d, e)]
    for key, group in itertools.groupby(sorted(intermediate), lambda x: x[0]):
        groups[key] = list(y for (x, y) in group)

    print("Starting reducer...")
    output.update(t1reducer(group_key, groups[group_key]) for group_key in groups)
    print("Reducing finished")

    outputCSV(output, "t1.csv")

    key = input('Press any key to continue')

# input key = airport id, value = flight id
# output key = airport name, value = flight id
# looks up airport name from other csv file "lookup"
# removes duplicate flight id values
def t1mapper(input_key, input_value):
    global lookup
    global flight_cache
    global airport_cache

    # set flight id value for tuple only if it is not in cache
    if not input_value in flight_cache:

        output_value = input_value
        if input_value is not "-":
            flight_cache.append(input_value)

        # lookup airport name and assign it as key
        for row in lookup:
            if input_key == row[1]:
                output_key = row[0]
                airport_cache.append(row[1])

        return((output_key, output_value))

# input = output of mapper
# output key = airport name, output value = number of flights frome ach airport
def t1reducer(input_key, input_value):
    if input_value[0] is not "-":
        return (input_key, len(input_value))
    else:
        return (input_key, 0)


# task 2 - list of flights based on flight id
# output include passenger id, IATA/FAA codes,
# departure time, arrival time
# key = flight id
# value = composite(passid, AATI from, AATI to, depature time, duration)
# output format flightid | deptfrom | depttime | arriveat | arrivetime | duration | list[passid]
def t2():
    global dataset
    global lookup
    intermediate = []
    temp = []
    output = {}

    print("Starting mapper...")
    for row in dataset:
        map = t2mapper(row[1], row)
        if map is not None:
            intermediate.append(map)
    print("Mapping finished")

    # sort by flight id
    intermediate.sort(key = lambda x: (x[0], x[1][0]))

    #combiner
    index = 0
    passengers = []
    for items in intermediate:
        a = intermediate[index]
        current_key = intermediate[index][0]
        if index+1 < len(intermediate):
            next_key = intermediate[index+1][0]
        else:
            next_key = 0

        passengers.append(a[1][0])
        if current_key != next_key:
            temp.append((a[0], [a[1][1], a[1][2], a[1][3], a[1][4], (passengers)]))
            passengers = []
        index += 1
    groups = dict(temp)

    print("Starting reducer...")
    output.update(t2reducer(group_key, groups[group_key]) for group_key in groups)
    print("Reducing finished")

    outputCSV(output, "t2.csv")

    key = input('Press any key to continue')


# output key is equal to flight id
# output value is composite, extracted from the row of flight data
# composite value = [passenger, airport from, airport to, dept time, flight duration]
def t2mapper(input_key, input_value):
    output_key = input_key
    output_value = []
    for col in input_value:
        if col != input_value[1]:
            output_value.append(col)
    return (output_key, output_value)


# input format key - flightid val - deptcode | arrivecode | depttimepoch | duration | list[passid]
# output format flightid | deptfrom | depttime | arriveat | arrivetime | duration | list[passid]
def t2reducer(input_key, input_value):
    output_value = []
    depttime = int(input_value[2])
    duration = int(input_value[3])

    # dept from and time
    for row in lookup:
        if input_value[0] == row[1]:
            output_value.append(row[0])
    output_value.append(epoch_to_datetime(depttime))
    # arrive at and time
    for row in lookup:
        if input_value[1] == row[1]:
            output_value.append(row[0])
    output_value.append(epoch_to_datetime(depttime+duration*60))
    # duration
    output_value.append(epoch_to_time(duration*60))

    # passengers
    output_value.append(input_value[4])

    return input_key, output_value


# task 3 - number of passengers on each flight
# key = flight id col 2, value = no. of passengers (passenger id col 1)
def t3():
    global dataset
    intermediate = []
    groups = {}
    output = {}

    # map data where flight id = key
    # passenger id = value
    print("Starting mapper...")
    for row in dataset:
        map = t3mapper(row[1], row[0])
        if map is not None:
            intermediate.append(map)
    print("Mapping finished")

    # group by keys
    for key, group in itertools.groupby(sorted(intermediate), lambda x: x[0]):
        groups[key] = list(y for (x, y) in group)

    # reduce data by summing up passengers on each flight
    print("Starting reducer...")
    output.update(t3reducer(group_key, groups[group_key]) for group_key in groups)
    print("Reducing finished")

    outputCSV(output, "t3.csv")

    key = input('Press any key to continue')


def t3mapper(input_key, input_value):
    return (input_key, input_value)


def t3reducer(input_key, input_value):
    return (input_key, len(input_value))


def main(argv):
    global dataset
    global lookup
    global file_path
    loop = True

    # import csv files
    importCSV("AComp_Passenger_data.csv", "Top30_airports_LatLong.csv")

    # clean up csv files
    dataset = remove_errors(dataset, True, True)
    lookup = remove_errors(lookup, False, False)

    while loop:
        print("\nTasks:")
        print("[1] Determine number of flights from each airport")
        print("[2] List flights based on flight ID")
        print("[3] Determine number of passengers on each flight")
        print("[exit]")

        text = input("Select task:")
        if text == "1":
            t1()
        elif text == "2":
            t2()
        elif text == "3":
            t3()
        elif text == "exit":
            loop = False
        else:
            print(file_path)
            print("Invlid Input")


if __name__ == "__main__":
    main(sys.argv)
