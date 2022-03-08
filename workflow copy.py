# import modules
import yaml
from yaml.loader import SafeLoader
import logging
import datetime 
import multiprocessing
from time import sleep

log=[]
def generateLogs(operation,ct= datetime.datetime.now()):
    with open("log.txt", 'a') as f:
        f.writelines(str(ct)+";"+".".join(log)+" "+operation+"\n")
#Flow Type
class Flow:
    def __init__(self,Name,Type,Execution,Activities):
        generateLogs("Entry")
        self.Name=Name
        self.Type=Type 
        self.Execution=Execution
        self.Activities=Activities
        if(self.Execution=="Sequential"):
            self.parseActivitiesSequential()
        else:
            self.parseActivitiesConcurrent()
        generateLogs("Exit")
        log.pop()
    def parseActivitiesSequential(self):
        for act in self.Activities:
            activity=self.Activities[act]
            log.append(act)
            if(activity['Type']=='Task'):
                if('Outputs' in activity):
                    tk=Task(activity,activity['Type'],activity['Function'],activity['Inputs'],activity['Outputs'])
                else:
                    tk=Task(activity,activity['Type'],activity['Function'],activity['Inputs'])
            else:
                sf=Flow(activity,activity["Type"],activity["Execution"],activity["Activities"])
    def parseActivitiesConcurrent(self):
        for act in self.Activities:
            activity=self.Activities[act]
            log.append(act)
            if(activity['Type']=='Task'):
                if('Outputs' in activity):
                    tk=Task(activity,activity['Type'],activity['Function'],activity['Inputs'],activity['Outputs'])
                else:
                    tk=Task(activity,activity['Type'],activity['Function'],activity['Inputs'])
            else:
                sf=Flow(activity,activity["Type"],activity["Execution"],activity["Activities"])




#Task Type
class Task(Flow):
    def __init__(self,Name,Type,Function,Inputs,Outputs=[]):
        generateLogs("Entry")
        self.Name=Name
        self.Type=Type
        self.Function=Function
        self.Inputs=Inputs 
        self.Outputs=Outputs
        if(Function=="TimeFunction"):
            self.TimeFunction(Inputs)
        log.pop()

    #TimeFunction
    def TimeFunction(self,Inputs):
        generateLogs("Executing %s(%s)"%(self.Function,list(self.Inputs.values())[-1]))
        sleep(int(Inputs['ExecutionTime']))
        generateLogs("Exit")
        

    #DataLoad
    def DataLoad(self):
        generateLogs("Executing %s(%s)"%(self.Function,list(self.Inputs.values())[-1]))

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
        
                