"""
This is the entrypoint to the program. 'python main.py' will be executed and the 
expected csv file should exist in ../data/destination/ after the execution is complete.
"""
#imports
from dataclasses import dataclass
import os, csv, sys
path = os.path.dirname(__file__)
# Allows to run the script from starting path.
sys.path.append("src/")
from src.some_storage_library import SomeStorageLibrary

# global variable class
@dataclass
class Constants():
    columns_filename:str = 'data/source/SOURCECOLUMNS.txt'
    source_data_filename:str = 'data/source/SOURCEDATA.txt'
    cleaned_data_filename:str = 'data/source/CLEANEDDATA.TXT'

class WritePremissionError(Exception):
    pass

class ReadFile():
    def __init__(self, filename:str):
        if os.path.exists(filename):
            self.filename = filename
        else:
            raise IOError("File does not exist")
            # return None
            
    def read_file(self) -> list:
        with open(self.filename, mode='r') as file_data:
            csv_reader = csv.reader(file_data,delimiter="|")
            return [l for l in csv_reader]

class FilteredSortedFile(ReadFile):
    def read_file(self) -> list:
        with open(self.filename, mode='r') as file_data:
            csv_reader = csv.reader(file_data,delimiter="|")
            '''Open file and read file then split lines and convert the first item to int. 
                Use the sort function on the integers to sort correctly. '''
            raw_data = [[int(l[0]), l[1]] for l in csv_reader]
            filtered_sorted_data = [datum[1] for datum in sorted(raw_data)]
            return filtered_sorted_data
            
class WriteFile():
    #TODO: Check to make sure you can write to file.
    def __init__(self, filename:str) -> None:
        self.filename = filename
        try:
            with open(self.filename, mode='w'):
                pass
        except IOError:
            raise WritePremissionError('You cannot write to this write.')
    #TODO: error handling if cannot write to disk or type error.      
    def write_file(self, header:list, data:list)->bool:
        try:
            with open(self.filename, mode='w', newline='') as col_data:
                csv_writer = csv.writer(col_data, delimiter=",")
                csv_writer.writerow(header)
                csv_writer.writerows(data)
            return True
        except IOError as x:
            print(x)
            return False
        
if __name__ == '__main__':
    """Entrypoint"""
    print('Beginning the ETL process...')
    global_vars = Constants()
    print('Reading files')
    cols = FilteredSortedFile(global_vars.columns_filename)
    data = ReadFile(global_vars.source_data_filename)
    print('Cleaning and formating data')
    writer = WriteFile(global_vars.cleaned_data_filename)
    writer.write_file(cols.read_file(), data.read_file())
    print('Writing data to disk')
    ssl = SomeStorageLibrary()
    ssl.load_csv(global_vars.cleaned_data_filename)
