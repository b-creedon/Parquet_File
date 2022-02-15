## simple Python script to read a Parquet format file

## https://numpy.org/doc/stable/
# import numpy as np

## https://pandas.pydata.org/docs/index.html
import pandas as pd

## https://arrow.apache.org/docs/index.html
# import pyarrow as pa 

import fnmatch 
import re

import os, sys, getopt

source_directory = 'NA'
source_filename = 'NA'
source_filename_treatment = 'catchall_rename' # Default Source File Treatment is 'rename'
output_directory = 'NA'
verbose = False

output_filename_csv = 'NA'
files_to_convert = []
rename_suffix = '.DONE'

# Remove 1st argument from the
# list of command line arguments
argumentList = sys.argv[1:]
# Options
short_options = "s:f:o:vhird"
# Long options
long_options = [ "source=", "filename=", "output=", "verbose", "help", "ignore", "rename", "delete" ]

# --------------------
# Get Command Line Arguments and Values
try:
    # Parsing command line arguments
    arguments, values = getopt.getopt( argumentList, short_options, long_options )
     
    # checking each argument
    for currentArgument, currentValue in arguments:
        if currentArgument in ("-h", "--help"):
            print ("Usage: read_parquet_file.py [-s|--source] <source directory> [-f|--filename] <source filename> [-o|--output] <output directory> [-h|help] [-v|--verbose] [-i|--ignore|-r|--rename|-d|--delete]")
            sys.exit()
        elif currentArgument in ("-s", "--source"):
            source_directory = currentValue
        elif currentArgument in ("-f", "--filename"):
            source_filename = currentValue
        elif currentArgument in ("-o", "--output"):
            output_directory = currentValue
        elif currentArgument in ("-d", "--delete"):
            source_filename_treatment = 'delete'
        elif currentArgument in ("-r", "--rename"):
            source_filename_treatment = 'rename'
        elif currentArgument in ("-i", "--ignore"):
            source_filename_treatment = 'ignore'
        elif currentArgument in ("-v", "--verbose"):
            verbose = True

except getopt.error as err:
    # output error, and return with an error code
    print (str(err))
    sys.exit(2)
# --------------------
# --------------------
# Check that the Command Line Arguments have valid values
if source_directory == "NA":
    print ("Usage: [-s|--source] <source directory> is a mandatory value. ")
    sys.exit(1)

## Check if the Source and Output Directory have an input value
if source_filename == "NA":
    print ("Usage: [-f|--filename] <source filename> is a mandatory value. ")
    sys.exit(1)

if output_directory == "NA":
    print ("Usage: [-o|--output] <output directory> is a mandatory value. ")
    sys.exit(1)
# --------------------
# --------------------
# Ensure that the Source Directory and Output Directory values end with the Directory Delimiter
## Check if the Source and Output Directory values end with the Directory Delimiter
if not source_directory.endswith("/"):
    source_directory = source_directory + "/"

if not output_directory.endswith("/"):
    output_directory = output_directory + "/"
# --------------------
# --------------------
#  Ensure that the Source Directory and Output Directory values are valid, existing directories
if not os.path.isdir(source_directory):
    print("Source Directory is a NOT a directory...Exiting.")
    sys.exit(1)

if not os.path.isdir(output_directory):
    print("Output Directory is a NOT a directory...Exiting")
    sys.exit(1)
# --------------------
# --------------------
# Debug input values
if verbose == True:
    print (("Source Directory: % s") % (source_directory))
    print (("Source Filename: % s") % (source_filename))
    print (("Source Filename Treatment: % s") % (source_filename_treatment))
    print (("Output Directory: % s") % (output_directory))
# --------------------
# --------------------
# Get a list of files in the Source Directory
src_dir_list = os.listdir(source_directory)

# Check that the list of filenames match the Source Filename 'pattern'
for file_name in src_dir_list:

    file_name_match = re.match( source_filename, file_name )
    if file_name_match:
        print( 'RE MATCHING Looking for "%s" in "%s" ->' % (source_filename, file_name) )

    if fnmatch.fnmatch( file_name, source_filename ):
        if verbose == True:
            print("--> MATCHING file. Adding", file_name )
        files_to_convert.append(file_name)
    else:
        if verbose == True:
            print("--> NOT MATCHING file. Ignoring", file_name )

# If there are no filenames that match the Source Filename 'pattern', exit indicating "Success" 
if len(files_to_convert) == 0:
    print("No Source Filenames found in Source Directory...Exiting")
    sys.exit(0)
# --------------------
# --------------------
# Convert the Source Filename to Parquet Output Filename
for file_to_convert in files_to_convert:
    # Create CSV Output Filename
    output_filename_csv = output_directory + file_to_convert + ".csv"
    # --------------------
    ## Check that output filename does NOT already exist. If it does, exit with error
    if os.path.exists(output_filename_csv):
        print("Output Filename", output_filename_csv, "already exists...Exiting")
        sys.exit(1)
    # --------------------
    ## Open and read contents of Parquet Source Filename
    result = pd.read_parquet( source_directory+file_to_convert, engine="auto", columns=["payoutorderid", "programcode"])
    # result = pd.read_parquet( source_directory+file_to_convert, engine="pyarrow", columns=["payoutorderid", "programcode"])
    # result = pd.read_parquet( source_directory+file_to_convert, engine="fastparquet", columns=["payoutorderid", "programcode"])
    ## Convert Parquet to CSV and wrote to output file.
    result.to_csv(output_filename_csv)
    if verbose == True:
        # print( result.dtypes )
        print("Source Parquet[", source_directory+file_to_convert, "] converted to CSV[", output_filename_csv, "]")
    # --------------------
    ## Source Filename Treatment
    if source_filename_treatment == 'delete':
        os.remove( file_to_convert )
        if verbose == True:
            print("--> DELETE Source Filename[", source_directory+file_to_convert, "]" )
    elif source_filename_treatment == 'rename':
        os.rename( source_directory+file_to_convert, source_directory+file_to_convert+rename_suffix)
        if verbose == True:
            print("--> RENAME Source Filename[", file_to_convert, "] --> [", file_to_convert+rename_suffix, "]" )
    elif source_filename_treatment == 'ignore':
        if verbose == True:
            print("--> IGNORE Source Filename[", file_to_convert, "]" )
    else:
        os.rename( source_directory+file_to_convert, source_directory+file_to_convert+rename_suffix)
        if verbose == True:
            print("--> CATCHALL RENAME Source Filename[", file_to_convert, "] --> [", file_to_convert+rename_suffix, "]" )
    # --------------------
# --------------------
# Exit indicating "Success"
sys.exit(0)