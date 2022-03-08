# import modules
import yaml
from yaml.loader import SafeLoader
import datetime 
import threading
from time import sleep

log=[]
def generateLogs(operation,entry=""):
    with open("log1B.txt", 'a') as f:
        if(entry==""):   
            f.writelines(str(datetime.datetime.now())+";"+".".join(log)+" "+operation+"\n")
        else:
            f.writelines(str(datetime.datetime.now())+";"+entry+" "+operation+"\n")

Threads=[]

#Flow Type
class Flow:
    def __init__(self,Name,Type,Execution,Activities,flowType="Sequential"):
        self.Name=Name
        self.Type=Type 
        self.Execution=Execution
        self.Activities=Activities
        self.log=log[:]
        if(flowType=="Concurrent"):
            generateLogs("Entry",".".join(self.log)+"."+Name)
        else:
            generateLogs("Entry")
        if(self.Execution=="Sequential"):
            self.parseActivitiesSequential()
        else:
            self.parseActivitiesConcurrent()
        if(flowType=="Concurrent"):
            generateLogs("Exit",".".join(self.log)+"."+Name)
        else:
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
        Threads=[]
        for act in self.Activities:
            activity=self.Activities[act]
            if(activity['Type']=='Task'):
                if('Outputs' in activity):
                    t=threading.Thread(target=Task,args=(act,activity['Type'],activity['Function'],activity['Inputs'],activity['Outputs'],"Concurrent"))
                else:
                    t=threading.Thread(target=Task,args=(act,activity['Type'],activity['Function'],activity['Inputs'],[],"Concurrent"))
                t.start()    
                Threads.append(t)
            else:
                t=threading.Thread(target=Flow,args=(act,activity["Type"],activity["Execution"],activity["Activities"],"Concurrent"))
                Threads.append(t)    
        for t in Threads:
            t.join()
#Task Type
class Task(Flow):
    def __init__(self,Name,Type,Function,Inputs,Outputs=[],flowType="Sequential"):
        
        self.Name=Name
        self.Type=Type
        self.Function=Function
        self.Inputs=Inputs 
        self.Outputs=Outputs
        self.log=log[:]
        if(flowType=="Concurrent"):
            generateLogs("Entry",".".join(self.log)+"."+Name)
        else:
            generateLogs("Entry")
        if(Function=="TimeFunction"):
            self.TimeFunction(flowType)
        if(Function=="DataLoad"):
            self.TimeFunction(Inputs)

        if(flowType=="Concurrent"):
            generateLogs("Exit",".".join(self.log)+"."+Name)
        else:
            generateLogs("Exit")
            log.pop()

    #TimeFunction
    def TimeFunction(self,flowType="Sequential"):
        if(flowType=="Sequential"):
            generateLogs("Executing %s(%s,%s)"%(self.Function,list(self.Inputs.values())[0],list(self.Inputs.values())[-1]))
        else:
            generateLogs("Executing %s(%s,%s)"%(self.Function,list(self.Inputs.values())[0],list(self.Inputs.values())[-1]),".".join(self.log)+"."+self.Name)
        sleep(int(self.Inputs['ExecutionTime']))        

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
        
                
