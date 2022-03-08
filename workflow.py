# import modules
from re import A
from tkinter import W
import yaml
from yaml.loader import SafeLoader
import logging
import datetime 
import multiprocessing
from time import sleep

log=""
def generateLogs(operation,ct= datetime.datetime.now()):
    with open("log"+wf.Name+".txt", 'w') as f:
        f.writelines(ct+";"+".".join(log)+operation)



#Flow Type
class Flow:
    def __init__(self,Name,Type,Execution,Activities):
        self.Name=Name
        self.Type=Type 
        self.Execution=Execution
        self.Activities=Activities
        if(self.Execution=="Sequential"):
            self.parseActivitiesSequential()
        else:
            self.parseActivitiesConcurrent()
    def parseActivities(self):
        for activity in self.Activities:
            log.append(activity)
            if(activity['Type']=='Task'):
                tk=Task(activity,activity['Type'],activity['Function'],activity['Inputs'],activity['Outputs'])
            else:
                sf=Flow(activity,activity["Type"],activity["Execution"],activity["Activities"])
                self.parseActivities(sf)
    def parseActivitiesConcurrent(self):
        pass




#Task Type
class Task(Flow):
    def __init__(self,Name,Type,Function,Inputs,Outputs=[]):
        generateLogs("Entry")
        self.Name=Name
        self.Type=Type
        self.Function=Function
        self.Inputs=Inputs 
        self.Outputs=Outputs
        generateLogs("Executing %s()")
        if(Function=="TimeFunction"):
            self.TimeFunction(self,Inputs)
        generateLogs("Exit")

    #TimeFunction
    def TimeFunction(self,Inputs):
        sleep(int(Inputs['ExecutionTime']))

    #DataLoad
    def DataLoad(self):
        pass 

    #Binning
    def Binningself(self):
        pass 

    #MergeResults
    def MergeResults(self):
        pass 

    #ExportResults
    def ExportResults(self):
        pass


# Open the file and load the file
yamlFileName=input("Enter the file path:").strip()
data=dict()
with open(yamlFileName) as f:
    data = yaml.load(f, Loader=SafeLoader)
    for datakey in list(data.keys()):
        workflow=data[datakey]
        log.append(datakey)
        wf=Flow(datakey,workflow["Type"],workflow["Execution"],workflow["Activities"])
        
                
