
"""
Pick Assistant Tool - Pod Stow Data Processing and Upload System

This tool analyzes pod stow data from orchestrator archives, processes the information,
and uploads it to a MongoDB database for tracking and management.

Authors:
    - djoneben (Ben Jones)
    - grsjoshu (Joshua Green)
    - ftnguyen (Tri Nguyen)
    - mathar (Matt Harrison)

Version History:
    v2.0: Major refactor for improved error handling, logging, and integration to webapp.
    v1.17: Use OLAF instead of human annotations.
    v1.15: Added pod barcode database to automatically look up the pod name.
    v1.14: Fixed bug in string construction caused by a file not being generated on Ubuntu 24
    v1.13: Fixed bug in argument parser
    v1.12: Fixed cycle loop runaway logic. Reenabled SSH functionality.
    v1.11: Loop the program if missing data.
    v1.10: Revived v1.4 feature for not crashing during planned recycle
    v1.9: Added in checks to alert potential data loss.
    v1.8: Added better comments. More accurate cycle count. Tidied up the code.

Usage:
    # Basic usage with orchestrator ID
    python PickAssistant.py -o orchestrator_20251007_123456/pod_1/cycle_50

    # Specify individual parameters
    python PickAssistant.py -o orchestrator_20251007_123456 -p pod_1 -c 50

    # Run in benchmark mode (continuous loop)
    python PickAssistant.py -bm -o orchestrator_20251007_123456 -p pod_1 -c 50

Environment Variables Required:
    MONGODB_URI: MongoDB connection string for database upload

"""

import json
import os
import argparse
import time
import logging
from enum import Enum

from typing import Dict, List, Optional, Tuple

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

print("PickAssistant v2.0")

# Configuration
WindowsDebug = False  # Switch to False if you are on a workcell.

# Workflow State Management
class WorkflowState(Enum):
    READING_FILES = "reading_files"
    READ_COMPLETE = "read_complete"
    READ_FAILED = "read_failed"
    GENERATING_CONTENT = "generating_content"
    GENERATION_COMPLETE = "generation_complete"
    GENERATION_FAILED = "generation_failed"
    UPLOADING_DATABASE = "uploading_database"
    UPLOAD_COMPLETE = "upload_complete"
    UPLOAD_FAILED = "upload_failed"

current_state = WorkflowState.READING_FILES
read_success = False
generation_success = False
upload_success = False

# Pod Barcode Database - Maps pod barcodes to friendly names
POD_BARCODE_DATABASE = {
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

def read_json_file(file_path: str) -> Optional[Dict]:
    """
    Read and parse a JSON file.

    Args:
        file_path: Path to the JSON file

    Returns:
        Parsed JSON data as dictionary, or None if error occurs
    """
    try:
        with open(file_path, 'r') as file:
            data = json.load(file)
        return data
    except FileNotFoundError:
        logger.error(f"File '{file_path}' not found.")
    except json.JSONDecodeError:
        logger.error(f"Invalid JSON format in file '{file_path}'.")
    except Exception as e:
        logger.error(f"An error occurred while reading the JSON file: {e}")
    return None


# To allow for PickAssistant to be called remotly via SSH.
parser = argparse.ArgumentParser(
    description='Pick Assistant Tool - Analyzes pod stow data from orchestrator archives',
    epilog='''
Examples:
  # Basic usage with orchestrator ID
  python PickAssistant.py -o orchestrator_20251007_123456/pod_1/cycle_50

  # Specify individual parameters
  python PickAssistant.py -o orchestrator_20251007_123456 -p pod_1 -c 50

  # Run in benchmark mode (continuous loop)
  python PickAssistant.py -bm -o orchestrator_20251007_123456 -p pod_1 -c 50

  # With custom pod name
  python PickAssistant.py -o orchestrator_20251007_123456 -n "Ninja Turtle" -c 50

For more information, contact: djoneben, grsjoshu, or ftnguyen
    ''',
    formatter_class=argparse.RawDescriptionHelpFormatter
)

parser.add_argument('-o', '--orchestrator',
                    default='',
                    metavar='ID',
                    help='Orchestrator ID (can include pod and cycle info, e.g., orchestrator_123/pod_1/cycle_50)')

parser.add_argument('-p', '--podid',
                    default='',
                    metavar='POD',
                    help='Pod ID (e.g., pod_1, pod_2). If not provided, will be prompted.')

parser.add_argument('-n', '--podname',
                    default='',
                    metavar='NAME',
                    help='Pod name/identifier (e.g., "Ninja Turtle", "South Park"). Auto-detected from barcode if available.')

parser.add_argument('-c', '--cycle',
                    default=0,
                    type=int,
                    metavar='CYCLE',
                    help='Total number of cycles to process. If not provided, will be prompted.')

parser.add_argument('-bm', '--benchmark',
                    action='store_true',
                    help='Run in benchmark mode (loops continuously until Ctrl+C is pressed)')

args = parser.parse_args()

# Wrap the main logic in a loop if benchmark mode is enabled
def run_pick_assistant(orchestrator_arg, pod_id_arg, pod_name_arg, cycle_count_arg, benchmark_mode=False):

    orchestrator = orchestrator_arg
    podID = pod_id_arg
    PodName = pod_name_arg
    TrueCycleCount = cycle_count_arg

    # Set Orchestrator ID
    if not WindowsDebug:
        while True:
            while orchestrator == "":
                orchestrator = input("Enter the Orchestrator ID: ").strip()
                podID = ""
            if "/" in orchestrator:
                parts = orchestrator.split("/")
                for part in parts:
                    if "orchestrator_" in part:
                        orchestrator = part
                    elif "pod_" in part:
                        podID = part
                    elif "cycle_" in part:
                        try:
                            TrueCycleCount = int(part.split("_")[1])
                        except (ValueError, IndexError) as e:
                            logger.warning(f"Could not parse cycle count from '{part}': {e}")
                            TrueCycleCount = 0
            # Check if orchestrator path exists
            orchestrator_path = '/home/local/carbon/archive/' + orchestrator + '/'
            if os.path.isdir(orchestrator_path):
                break
            else:
                print(f"Orchestrator path '{orchestrator_path}' does not exist. Please enter a valid Orchestrator ID.")
                orchestrator = ""

    # Set Pod ID / Check if Pod ID was not provided with the orchestrator. If not ask for the index with a default of 1.
    if podID == "":
        inputt = input("What is the pod index? ")
        if inputt.isdigit():
            podID = "pod_" + str(inputt)
        else:
            podID = "pod_1"

    # Inquire about total cycles to prevent data loss.
    if TrueCycleCount == 0:
        inputt = input("Please enter the total cycle count: ")
        if inputt.isdigit():
            TrueCycleCount = int(inputt)

    # Get the Pod Barcode from generated files. If the files do not exist then the program will print a crash report to terminal.
    if WindowsDebug:
        file_path = '' + orchestrator + ''
        barcode_file = "pod_1\\cycle_1\\dynamic_1\\datamanager_triggers_load_data.data.json"
    else:
        file_path = '/home/local/carbon/archive/' + orchestrator + '/'
        barcode_file = file_path + podID + "/cycle_1/dynamic_1/datamanager_triggers_load_data.data.json"

    if not os.path.exists(barcode_file):
        logger.error(f"Critical file not found: {barcode_file}. Workflow cannot continue.")
        if benchmark_mode:
            return False
        exit(1)

    podBarcode = read_json_file(barcode_file)
    if podBarcode is None:
        logger.error(f"Failed to read barcode file: {barcode_file}. Workflow cannot continue.")
        if benchmark_mode:
            return False
        exit(1)

    # Asks for user to input an alias identifier for the pod barcode, if not found in the barcode database.
    if podBarcode in POD_BARCODE_DATABASE:
        PodName = POD_BARCODE_DATABASE[podBarcode]
    if PodName == "":
        PodName = input("Please enter a Pod Identifier like NT or NinjaTurtles: ")
    else:
        print(PodName, " was found via the barcode")

    # # Get Pod barcode/ID
    # if WindowsDebug:
    #     temp = "\\cycle_"
    #     pod_data_file_path = file_path + podID + '\\cycle_1\\dynamic_1\\workcell_metric_latest_pod_visit.data.json'
    # else:
    #     temp = "/cycle_"
    #     pod_data_file_path = file_path + podID + '/cycle_1/dynamic_1/workcell_metric_latest_pod_visit.data.json'

    # if not os.path.exists(pod_data_file_path):
    #     logger.error(f"Critical file not found: {pod_data_file_path}. Workflow cannot continue.")
    #     if benchmark_mode:
    #         return False
    #     exit(1)

    # PodData = read_json_file(pod_data_file_path)
    # if PodData is None:
    #     logger.error(f"Failed to read pod data file: {pod_data_file_path}. Workflow cannot continue.")
    #     if benchmark_mode:
    #         return False
    #     exit(1)

    # # Loop through each cycle and gather data. | If missing data restart loop
    # global current_state
    # current_state = WorkflowState.READING_FILES
    # logger.info(f"State: {current_state.value}")

    isDone = False
    while not isDone:
        i = 1
        StowedItems = {}
        AttemptedStows = {}
        temp = "/cycle_"
        cycles = 0

        try:
            while os.path.isdir(file_path + podID + temp + str(i)):
                if WindowsDebug:
                    annotation_file_path = file_path + podID + '\\cycle_' + str(i) + '\\auto_annotation\\_olaf_primary_annotation.data.json'
                    stow_location_file_path = file_path + podID + '\\cycle_' + str(i) + '\\dynamic_1\\match_output.data.json'
                else:
                    annotation_file_path = file_path + podID + '/cycle_' + str(i) + '/auto_annotation/_olaf_primary_annotation.data.json'
                    stow_location_file_path = file_path + podID + '/cycle_' + str(i) + '/dynamic_1/match_output.data.json'

                if not os.path.exists(stow_location_file_path):
                    logger.error(f"Critical file not found: {stow_location_file_path}. Workflow cannot continue.")
                    if benchmark_mode:
                        return False
                    exit(1)

                AnnotationData = read_json_file(annotation_file_path)
                StowData = read_json_file(stow_location_file_path)

                if StowData is None:
                    logger.error(f"Failed to read stow data file: {stow_location_file_path}. Workflow cannot continue.")
                    if benchmark_mode:
                        return False
                    exit(1)

                # If data exists add it to the nested dictionary.
                if StowData:
                    cycles += 1
                    if StowData.get("binId"):
                        if AnnotationData and AnnotationData.get("isStowedItemInBin"):
                            StowedItems['/cycle_' + str(i)] = {
                                "itemFcsku": StowData.get("itemFcsku"),
                                "binId": StowData.get("binId"),
                                "binScannableId": StowData.get("binScannableId")
                            }
                        else:
                            AttemptedStows['/cycle_' + str(i)] = {
                                "itemFcsku": StowData.get("itemFcsku"),
                                "binId": StowData.get("binId"),
                                "binScannableId": StowData.get("binScannableId")
                            }
                    else:
                        print("cycle_" + str(i) + " does not have a bin ID.")
                i += 1
            if cycles >= TrueCycleCount and not os.path.isdir(file_path + podID + temp + str(i + 1)):
                isDone = True
                read_success = True
                current_state = WorkflowState.READ_COMPLETE
                logger.info(f"State: {current_state.value}")
            else:
                print("Cycles missing (", cycles, "/", TrueCycleCount, "), retrying...")
                time.sleep(1)
        except Exception as e:
            read_success = False
            current_state = WorkflowState.READ_FAILED
            logger.error(f"State: {current_state.value} - {e}")
            raise

    # Adds / reorders the list of items into bin location by alphabetic order first then numerical.
    if not read_success:
        logger.error("Cannot proceed to GENERATING_CONTENT: READ_COMPLETE not achieved")
        raise RuntimeError("State transition blocked: reading files did not complete successfully")

    current_state = WorkflowState.GENERATING_CONTENT
    logger.info(f"State: {current_state.value}")

    try:
        itemss = []
        for item in StowedItems:
            itemss.append([StowedItems[item]["binId"], StowedItems[item]["itemFcsku"]])
        itemss.sort(key=lambda x: x[0][-1] + x[0][-2])

        # Adds / reorders the list of likely failed stows.
        bitemss = []
        for item in AttemptedStows:
            bitemss.append([AttemptedStows[item]["binId"], AttemptedStows[item]["itemFcsku"]])
        bitemss.sort(key=lambda x: x[0][-1] + x[0][-2])

        i_count = len(itemss)

        generation_success = True
        current_state = WorkflowState.GENERATION_COMPLETE
        logger.info(f"State: {current_state.value}")
    except Exception as e:
        generation_success = False
        current_state = WorkflowState.GENERATION_FAILED
        logger.error(f"State: {current_state.value} - {e}")
        raise

    if TrueCycleCount > cycles:
        print("\n\n!!! Missing Cycle Data !!!   There are", (TrueCycleCount - cycles), "cycles unaccounted for.")

    # Upload to database
    def upload_to_cleans_collection():
        from datetime import datetime
        global current_state, upload_success

        if not generation_success:
            logger.error("Cannot proceed to UPLOADING_DATABASE: GENERATION_COMPLETE not achieved")
            return False

        current_state = WorkflowState.UPLOADING_DATABASE
        logger.info(f"State: {current_state.value}")

        # Prepare cleaning data document FIRST (before any DB connection)
        try:
            # Parse pod barcode information
            if podBarcode and " " in podBarcode and "-" in podBarcode:
                try:
                    after_space = podBarcode.split(" ")[1]
                    before_space = podBarcode.split(" ")[0]
                    podFace = after_space.split("-")[1]
                    podType = after_space.split("-")[0]
                except (IndexError, AttributeError):
                    podType = "Unknown"
                    podFace = "Unknown"
                    before_space = podBarcode
            else:
                podType = "Unknown"
                podFace = "Unknown"
                before_space = podBarcode

            orchestratorID = orchestrator + "/" + podID
            uploadedAT = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            # Get system environment variables
            user = os.environ.get('USER')
            station = os.environ.get('STATION')

            # If benchmark mode is active, always override station to "benchmark"
            if benchmark_mode:
                station = "benchmark"

            # Build the complete document
            clean_document = {
                "podBarcode": before_space,
                "podName": PodName,
                "orchestratorId": orchestratorID,
                "podType": podType,
                "podFace": podFace,
                "stowedItems": [],
                "attemptedStows": [],
                "uploadAt": uploadedAT,
                "status": "incomplete",
                "totalItems": i_count,
                "user": user,
                "station": station
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
        except Exception as e:
            upload_success = False
            logger.error(f"Error preparing document: {e}")
            return False

        # NOW attempt database connection and upload
        try:
            from pymongo import MongoClient

            logger.info("  Connecting to MongoDB...")

            # MongoDB connection string from environment variable for security
            # Set MONGODB_URI environment variable before running this script
            connection_string = os.environ.get('MONGODB_URI')
            if not connection_string:
                upload_success = False
                logger.error("MONGODB_URI environment variable not set")
                print("  Contact @ftnguyen to set it up")
                return False

            # Connect to MongoDB
            client = MongoClient(connection_string)

            # Select database and collection
            db = client['podManagement']
            cleans_collection = db['cleans']

            # Insert document into cleans collection
            result = cleans_collection.insert_one(clean_document)

            logger.info(f"  Pick list uploaded successfully")
            logger.info(f"  Document ID: {result.inserted_id}")
            # Close connection
            client.close()
            logger.info(f"  Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            logger.info(f"  Pod: {PodName} ({podBarcode})")
            logger.info(f"  Orchestrator: {orchestratorID}")
            if benchmark_mode:
                if podFace == "A":
                    logger.info(f"  Awaiting {PodName} C face")

            upload_success = True
            current_state = WorkflowState.UPLOAD_COMPLETE
            logger.info(f"State: {current_state.value}")
            return True

        except ImportError:
            upload_success = False
            current_state = WorkflowState.UPLOAD_FAILED
            logger.error(f"State: {current_state.value} - PyMongo not installed")
            print("  Document was prepared but not uploaded")
            return False
        except Exception as e:
            upload_success = False
            current_state = WorkflowState.UPLOAD_FAILED
            logger.error(f"State: {current_state.value} - {e}")
            print("  Document was prepared but upload failed")
            return False

    while True:
        result = upload_to_cleans_collection()
        if result:
            return True
        input("\nPress Enter to retry or Ctrl+C to cancel...")
        print("Retrying...")

# Execute the main function with benchmark mode support
if __name__ == "__main__":
    # Parse command line arguments
    orchestrator = args.orchestrator
    podID = args.podid
    PodName = args.podname
    TrueCycleCount = int(args.cycle)
    benchmark_mode = args.benchmark

    if benchmark_mode:
        print("\n*** BENCHMARK MODE ENABLED ***")
        print("Script will loop continuously. Press Ctrl+C to cancel.\n")
        run_count = 0
        try:
            while True:
                run_count += 1
                print(f"\n{'='*60}")
                print(f"Benchmark Run #{run_count}")
                print(f"{'='*60}\n")

                run_pick_assistant(orchestrator, podID, PodName, TrueCycleCount, benchmark_mode)
        except KeyboardInterrupt:
            print("Exiting...")
    else:
        # Normal single execution
        run_pick_assistant(orchestrator, podID, PodName, TrueCycleCount, benchmark_mode)
