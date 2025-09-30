
"""
    Pick Assistant Tool:
    Authors: djoneben (Ben Jones), grsjoshu (Joshua Green), & (Tri Nguyen) ftnguyen
    v1.17.
        Use OLAF instead of human annotations.
    v1.15:
        Added pod barcode database to automatically look up the pod name.
    v1.14:
        Fixed bug in string construction caused by a file not being generated on Ubuntu 24
    v1.13:
        Fixed bug in argument parser
    v1:12:
        Fixed cycle loop runaway logic.
        Reenabled SSH functionality.
    v1.11:
        Loop the program if missing data.
    v1.10:
        Revived v1.4 feature for not crashing durning planed recycle
    v1.9:
        Added in checks to alert potencial data loss.
    v1.8:
        Added Better comments.
        More accurate cycle count.
        Tidied up the code to make it readable.
"""

import json
import os
import argparse
import time

print ("PickAssistant v1.17")

WindowsDebug = False # switch to False if you are on a workcell.

podBarcodeDatabase = {
    "HB05101914818 H12-A" : "Ninja Turtle",
    "HB05101914818 H12-C" : "Ninja Turtle",
    "HB05109809243 H11-A" : "Ninja Turtle",
    "HB05109809243 H11-C" : "Ninja Turtle",
    "HB05109809241 H10-A" : "Ninja Turtle",
    "HB05109809241 H10-C" : "Ninja Turtle",
    "HB05102038798 H8-A" : "Ninja Turtle",
    "HB05102038798 H8-C" : "Ninja Turtle",
    "HB05109809234 H12-A" : "South Park",
    "HB05109809234 H12-C" : "South Park",
    "HB05109809233 H11-A" : "South Park",
    "HB05109809233 H11-C" : "South Park",
    "HB05103435481 H10-A" : "South Park",
    "HB05103435481 H10-C" : "South Park",
    "HB05109809242 H8-A" : "South Park",
    "HB05109809242 H8-C" : "South Park",
    "HB05100404700 H12-A" : "Ghost Busters",
    "HB05100404700 H12-C" : "Ghost Busters",
    "HB05100404680 H11-A" : "Ghost Busters",
    "HB05100404680 H11-C" : "Ghost Busters",
    "HB05109809235 H10-A" : "Ghost Busters",
    "HB05109809235 H10-C" : "Ghost Busters",
    "HB05100404695 H8-A" : "Ghost Busters",
    "HB05100404695 H8-C" : "Ghost Busters",
    "HB05100404690 H12-A" : "Power Rangers",
    "HB05100404690 H12-C" : "Power Rangers",
    "HB05100404681 H11-A" : "Power Rangers",
    "HB05100404681 H11-C" : "Power Rangers",
    "HB05100404683 H10-A" : "Power Rangers",
    "HB05100404683 H10-C" : "Power Rangers",
    "HB05100404694 H8-A" : "Power Rangers",
    "HB05100404694 H8-C" : "Power Rangers",
    "HB05100404693 H12-A" : "Ghosts",
    "HB05100404693 H12-C" : "Ghosts",
    "HB05103433784 H11-A" : "Ghosts",
    "HB05103433784 H11-C" : "Ghosts",
    "HB05100404684 H10-A" : "Ghosts",
    "HB05100404684 H10-C" : "Ghosts",
    "HB05100404696 H8-A" : "Ghosts",
    "HB05100404696 H8-C" : "Ghosts",
    "HB05100404688 H10-A" : "Clone",
    "HB05100404688 H10-C" : "Clone",
    "HB05100404686 H10-A" : "Goku",
    "HB05100404686 H10-C" : "Goku",
    "HB05100404685 H10-A" : "Pod Father",
    "HB05100404685 H10-C" : "Pod Father"
}

def read_json_file(file_path):
    try:
        with open(file_path, 'r') as file:
            data = json.load(file)
        return data
    except FileNotFoundError:
        print(f"Error: File '{file_path}' not found.")
    except json.JSONDecodeError:
        print(f"Error: Invalid JSON format in file '{file_path}'.")
    except Exception as e:
        print(f"An error occurred while reading the JSON file: {e}")

def printPod (PodFace,mode="int"):
    display=""
    if mode == "int":
        for row in PodFace:
            temp=("|")
            for col in row:
                t = col
                if t==0:
                    t = " "
                temp= temp+ (f"{t:^5}|")
            display+=temp+"\n"
            display+="-"*(len(row)*6 +1)+"\n"
    else:
        for bin in PodFace:
            print (bin)
            display += bin +"\n"
    return display

# To allow for PickAssistant to be called remotly via SSH.
parser = argparse.ArgumentParser()
parser.add_argument('-o',"--orchestrator",default='')
parser.add_argument('-p',"--podid",default='')
parser.add_argument('-n',"--podname",default='')
parser.add_argument('-c',"--cyclecount",default=0)

args = parser.parse_args()
orchestrator=args.orchestrator
podID=args.podid
PodName=args.podname
TrueCycleCount = int(args.cyclecount)

#Set Orchestrastor ID
if not WindowsDebug:
    while orchestrator =="":
        orchestrator = input("Enter the Orchestrator ID: ")
        podID=""
if "/" in orchestrator:
    parts=orchestrator.split("/")
    for part in parts:
        if "orchestrator_" in part:
            orchestrator=part
        elif "pod_" in part:
            podID=part
        elif "cycle_" in part:
            try:
                TrueCycleCount = int(part.split("_")[1])
            except Exception as e:
                TrueCycleCount = 0

#Set Pod ID / Check if Pod ID was not provided with the orchestrator. If not ask for the index with a default of 1.
if podID=="":
    inputt = input("What is the pod index? ")
    if inputt.isdigit():
        podID="pod_"+str(inputt)
    else:
        podID="pod_1"

#inquire about total cycles to prevent data loss.
if TrueCycleCount==0:
    inputt = input("Please enter the total cycle count: ")
    if inputt.isdigit():
        TrueCycleCount=int(inputt)

#Get the Pod Barcode from generated files. If the files do not exist then the program will print a crash report to terminal.
if WindowsDebug:
    file_path = ''+orchestrator+''
    podBarcode = read_json_file("pod_1\\cycle_1\\dynamic_1\\datamanager_triggers_load_data.data.json")
else:
    file_path = '/home/local/carbon/archive/'+orchestrator+'/'
    podBarcode = read_json_file(file_path+podID+"/cycle_1/dynamic_1/datamanager_triggers_load_data.data.json")

#Asks for user to input an alias identifier for the pod barcode, if not found in the barcode database.
if podBarcode in podBarcodeDatabase:
    PodName= podBarcodeDatabase[podBarcode]
if PodName=="":
    PodName = input("Please enter a Pod Identifier like NT or NinjaTurtles: ")
else:
    print (PodName," was found via the barcode")

#Get Pod barcode/ID
if WindowsDebug:
    temp="\\cycle_"
    pod_data_file_path = file_path+podID+'\\cycle_1\\dynamic_1\\workcell_metric_latest_pod_visit.data.json'
else:
    temp="/cycle_"
    pod_data_file_path = file_path+podID+'/cycle_1/dynamic_1/workcell_metric_latest_pod_visit.data.json'
PodData = read_json_file(pod_data_file_path)

#Loop through each cycle and gather data. | If missing data restart loop
isDone = False
while not isDone:
    i=1
    StowedItems={}
    AttemptedStows={}

    cycles = 0

    while os.path.isdir(file_path+podID+temp+str(i)):
        if WindowsDebug:
            annotation_file_path = file_path+podID+'\\cycle_'+str(i)+'\\auto_annotation\\_olaf_primary_annotation.data.json'
            stow_location_file_path = file_path+podID+'\\cycle_'+str(i)+'\\dynamic_1\\match_output.data.json'
        else:
            annotation_file_path = file_path+podID+'/cycle_'+str(i)+'/auto_annotation/_olaf_primary_annotation.data.json'
            stow_location_file_path = file_path+podID+'/cycle_'+str(i)+'/dynamic_1/match_output.data.json'
        AnnotationData = read_json_file(annotation_file_path)
        StowData = read_json_file(stow_location_file_path)

        #If data exists add it to the nested dictionary.
        if StowData:
            cycles+=1
            if StowData["binId"]:
                if AnnotationData and AnnotationData["isStowedItemInBin"]:
                    StowedItems['/cycle_'+str(i)]={"itemFcsku":StowData["itemFcsku"],"binId":StowData["binId"],"binScannableId":StowData["binScannableId"]}
                else:
                    AttemptedStows['/cycle_'+str(i)]={"itemFcsku":StowData["itemFcsku"],"binId":StowData["binId"],"binScannableId":StowData["binScannableId"]}
            else:
                print("cycle_"+str(i)+" does not have a bin ID.")
        i += 1
    if cycles >= TrueCycleCount and not os.path.isdir(file_path+podID+temp+str(i+1)):
        isDone = True
    else:
        print("Cycles missing (",cycles,"/",TrueCycleCount,"), retrying...")
        time.sleep(1)

stowedPodFace = [
        ["Pod","1","2","3","4"],
        ["m",[],[],[],[]],
        ["l",[],[],[],[]],
        ["k",[],[],[],[]],
        ["j",[],[],[],[]],
        ["i",[],[],[],[]],
        ["h",[],[],[],[]],
        ["g",[],[],[],[]],
        ["f",[],[],[],[]],
        ["e",[],[],[],[]],
        ["d",[],[],[],[]],
        ["c",[],[],[],[]],
        ["b",[],[],[],[]],
        ["a",[],[],[],[]]
    ]

StowedTotal= stowedPodFace

#Storing item barcodes in arrays associated with its bin location in a 2d array mockup of the pod.
for items in StowedItems:
    bin = StowedItems[items]["binId"][-2:]
    stowedPodFace[13-(ord(bin[1])-ord('a'))][int(bin[0])].append(StowedItems[items]["itemFcsku"])
    print (items," - ",StowedItems[items]["itemFcsku"]," - ",StowedItems[items]["binId"]," - ",StowedItems[items]["binScannableId"])

#Builds Bin toltals into a 2d Array.
for row in range(len(stowedPodFace)):
        if row > 0:
            for col in range (5):
                if col > 0:
                    if stowedPodFace[row][col] != None:
                        StowedTotal[row][col] = len(stowedPodFace[row][col])


output = "Pod: "+str(PodName)+"\n"+podBarcode+"\n\n"+"Orchestrator: "+str(orchestrator+"/"+podID)+"\n\n"+str(printPod(StowedTotal))
output += "\n"
#Adds / reorders the list of items into bin location by alphabetic order first then numerical.
itemss=[]
for item in StowedItems:
    itemss.append([StowedItems[item]["binId"],StowedItems[item]["itemFcsku"]])
itemss.sort(key=lambda x: x[0][-1]+x[0][-2])

#Adds / reorders the list of likely failed stows.
bitemss = []
for item in AttemptedStows:
    bitemss.append([AttemptedStows[item]["binId"],AttemptedStows[item]["itemFcsku"]])
bitemss.sort(key=lambda x: x[0][-1]+x[0][-2])

#Adds successfull stowed items to the output Barcode : Location
for i in itemss:
    output += f"{i[1]} : {i[0]}\n"
i_count = len(itemss)

#Adds stowed to adjacent bin items to
if bitemss:
    output += "\nBins where stows were attempted but likely not successful:\n"
    for item in bitemss:
        output += f"{item[1]} : {item[0]}\n"

output += f"\n{i_count}/{cycles} {round((i_count/cycles*100),2)}%"

print ("")
print (output)

if TrueCycleCount > cycles:
    print ("\n\n!!! Missing Cycle Data !!!   There are", (TrueCycleCount-cycles),"cycles unaccounted for.")

#Copy output to clipboard if xclip is installed.
if not WindowsDebug:
    from subprocess import Popen, PIPE
    def copy2clip(text):
        p = Popen(['xclip', '-selection','clipboard'],stdin=PIPE)
        p.communicate(input=text.encode('utf-8'))

    copy2clip(output)

#Upload to database
def upload_to_cleans_collection():
    try:
        from pymongo import MongoClient
        from datetime import datetime
        import uuid

        # MongoDB connection string with provided credentials
        connection_string = "mongodb+srv://workcellupload:VTRqz1YWdHreZT0t@podmanagement.yv8dt9t.mongodb.net?retryWrites=true&w=majority"

        # Connect to MongoDB
        client = MongoClient(connection_string)

        # Select database and collection
        db = client['podManagement']
        cleans_collection = db['cleans']

        # Prepare cleaning data document
        if podBarcode and " " in podBarcode and "-" in podBarcode:
            try:
                # Split by space and get the part after space, then split by hyphen and get first part
                after_space = podBarcode.split(" ")[1]
                podFace = after_space.split("-")[1]
                podType = after_space.split("-")[0]
            except (IndexError, AttributeError):
                podType = "Unknown"

        clean_document = {
            "_id": str(uuid.uuid4()),  # Generate unique ID
            "podBarcode": podBarcode,
            "podName": PodName,
            "orchestratorId": orchestrator,
            "podFace": podFace,
            "podType": podType,
            "stowedItems": [],
            "attemptedStows": [],
            "cleaningTimestamp": datetime.datetime.now(datetime.timezone.utc),
            "status": "incomplete",
            "totalItems": i_count
        }

        # Add stowed items data
        for item_data in itemss:
            clean_document["stowedItems"].append({
                "itemFcsku": item_data[1],
                "binId": item_data[0],
                "status": "stowed"
            })

        # Add attempted stows data
        for item_data in bitemss:
            clean_document["attemptedStows"].append({
                "itemFcsku": item_data[1],
                "binId": item_data[0],
                "status": "attempted"
            })

        # Insert document into cleans collection
        result = cleans_collection.insert_one(clean_document)

        print(f"\n✓ Successfully uploaded cleaning data to MongoDB 'cleans' collection")
        print(f"  Document ID: {result.inserted_id}")
        print(f"  Pod: {PodName} ({podBarcode})")
        print(f"  Items processed: {i_count}/{cycles}")

        # Close connection
        client.close()

        return True

    except ImportError:
        print("\n! PyMongo not installed. Install with: pip install pymongo")
        return False
    except Exception as e:
        print(f"\n! Error uploading to MongoDB cleans collection: {e}")
        return False

# Execute the upload function
if __name__ == "__main__":
    # Call the upload function after all data processing is complete
    upload_success = upload_to_cleans_collection()
