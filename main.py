"""
This is the entrypoint to the program. 'python main.py' will be executed and the 
expected csv file should exist in ../data/destination/ after the execution is complete.
"""
#imports
import os, csv, sys
path = os.path.dirname(__file__)
sys.path.append("src/")
from some_storage_library import SomeStorageLibrary

# global variable class
#TODO: setup as dataclass 
class Constants():
    def __init__(self):
        self.columns_filename = 'data/source/SOURCECOLUMNS.txt'
        self.source_data_filename = 'data/source/SOURCEDATA.txt'
        self.cleaned_data_filename = 'data/source/CLEANEDDATA.TXT'

class ReadFile():
    def __init__(self,filename:str):
        if os.path.exists(filename):
            self.filename = filename
        else:
            raise IOError("File does not exist")
            # return None
            
    #TODO: error handling if file does not exist
    def read_file(self,sort_list:bool = False)->list:
    #Open file and read file then split lines and convert the first item to int. Use the sort function on the integers to sort correctly.  
        with open(self.filename,'r') as file_data:
            csv_reader = csv.reader(file_data,delimiter="|")
            #Sort columns if need be.
            if sort_list:
                raw_data = [[int(l[0]),l[1]] for l in csv_reader]
                filitered_sorted_data = [datum[1] for datum in sorted(raw_data)]
                return filitered_sorted_data
            else:
                return [l for l in csv_reader]
                
class WriteFile():
    #TODO: Check to make sure you can write to file.
    def __init__(self,filename:str):
        self.filename = filename
        
    #TODO: error handling if cannot write to disk or type error.      
    def write_file(self,header:list,data:list)->bool:
        try:
            with open(self.filename,'w',newline='') as col_data:
                csv_writer = csv.writer(col_data,delimiter=",")
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
    cols = ReadFile(global_vars.columns_filename)
    data = ReadFile(global_vars.source_data_filename)
    print('Cleaning and formating data')
    writer = WriteFile(global_vars.cleaned_data_filename)
    writer.write_file(cols.read_file(True), data.read_file(False))
    print('Writing data to disk')
    ssl = SomeStorageLibrary()
    ssl.load_csv(global_vars.cleaned_data_filename)
