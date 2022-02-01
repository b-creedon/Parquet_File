## simple Python script to read a Parquet format file

## https://numpy.org/doc/stable/
import numpy as np

## https://pandas.pydata.org/docs/index.html
import pandas as pd

## https://arrow.apache.org/docs/index.html
import pyarrow as pa 

import os, sys, getopt

source_directory = 'NA'
source_filename = 'NA'
output_directory = 'NA'
output_filename_csv = 'NA'
verbose = False

# Remove 1st argument from the
# list of command line arguments
argumentList = sys.argv[1:]
# Options
options = "hi:f:o:v"
# Long options
long_options = ["Help", "Source Directory=", "Input Filename =", "Output Directory =", "Verbose"]

try:
    # Parsing argument
    arguments, values = getopt.getopt(argumentList, options, long_options)
     
    # checking each argument
    for currentArgument, currentValue in arguments:
        if currentArgument in ("-h", "--Help"):
            print ("Usage: read_parquet_file.py [-i|--source] <source directory> [-f|--filename] <source filename> [-o|--output] <output directory> -h [-v]")
            sys.exit()
        elif currentArgument in ("-i", "--source"):
            source_directory = currentValue
        elif currentArgument in ("-f", "--filename"):
            source_filename = currentValue
        elif currentArgument in ("-o", "--output"):
            output_directory = currentValue
        elif currentArgument in ("-v"):
            verbose = True

except getopt.error as err:
    # output error, and return with an error code
    print (str(err))
    sys.exit(2)

if source_directory == "NA":
    print ("Usage: [-i|--source] <source directory> is a mandatory value. ")
    sys.exit(1)

if source_filename == "NA":
    print ("Usage: [-f|--filename] <source filename> is a mandatory value. ")
    sys.exit(1)

if output_directory == "NA":
    print ("Usage: [-o|--output] <output directory> is a mandatory value. ")
    sys.exit(1)

if verbose == True:
    print (("Source Directory: % s") % (source_directory))
    print (("Source Filename: % s") % (source_filename))
    print (("Output Directory: % s") % (output_directory))
    print()
    if not os.path.isdir(source_directory):
        print("Source Directory is a NOT a directory...Exiting.")
        sys.exit(1)
    if not os.path.isfile(source_directory+source_filename):
        print("Source Filename is a NOT a file...Exiting")
        sys.exit(1)
    if not os.path.isdir(output_directory):
        print("Output Directory is a NOT a directory...Exiting")
        sys.exit(1)
    src_dir_list = os.scandir(source_directory)
    for file_name in src_dir_list:
        if file_name.is_dir():
            print("Directory-->", file_name.name)
        elif file_name.is_file() and file_name.name.endswith(".parquet"):
            print("Parquet File-->", file_name.name)
        # else:
            # print("Some weird file...", file_name.name )

print()
# Create CSV Output Filename
output_filename_csv = output_directory + source_filename + ".csv"

## Open and read contents of Parquet Source Filename
result = pd.read_parquet( source_directory+source_filename, engine="pyarrow", columns=["payoutorderid", "programcode"])

## Convert Parquet to CSV and wrote to output file.
result.to_csv(output_filename_csv)

if verbose == True:
    print( result.dtypes )
    print("Source Parquet[", source_directory+source_filename, "] converted to CSV[", output_filename_csv, "]")

# print ( result )