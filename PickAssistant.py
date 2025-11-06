
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
    v2.2: Bug fix for out synced s3 timezone issue
    v2.1: Minor refactor for migrate to read from s3 bucket
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
import tempfile
from urllib.parse import urlparse
import uuid
import boto3
import subprocess
from enum import Enum

from typing import Dict, List, Optional, Tuple

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

print("PickAssistant v2.2")

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

#s3_uri_main = "s3://stow-carbon-copy/Atlas/${stationId}/${date}/${orchestrator}/${PodID}/cycle_${CycleID}/dynamic_1/"
#podID_uri = s3_uri_main + "3_scene_pod.data.json"
#match_output_uri = s3_uri_main + "match_output.data.json"

def get_json(s3_uri: str) -> Optional[Dict]:
    """
    Read and parse a JSON file from S3.

    Args:
        s3_uri: S3 URI (e.g., s3://bucket/path/to/file.json)

    Returns:
        Parsed JSON data as dictionary, or None if error occurs
    """
    try:
        s3 = boto3.client('s3')
        parsed = urlparse(s3_uri)
        bucket = parsed.netloc
        key = parsed.path.lstrip('/')
        
        response = s3.get_object(Bucket=bucket, Key=key)
        json_content = response['Body'].read().decode('utf-8')
        data = json.loads(json_content)
        return data
    except Exception as e:
        if 'NoSuchKey' in str(e):
            logger.info(f"S3 file '{s3_uri}' not found.")
        elif 'JSONDecodeError' in str(type(e).__name__):
            logger.error(f"Invalid JSON format in S3 file '{s3_uri}'.")
        else:
            logger.error(f"Error reading S3 file: {e}")
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

parser.add_argument('-n', '--podname',
                    default='',
                    metavar='NAME',
                    help='Pod name/identifier (e.g., "Ninja Turtle", "South Park"). Auto-detected from barcode if available.')

parser.add_argument('-bm', '--benchmark',
                    action='store_true',
                    help='Run in benchmark mode (loops continuously until Ctrl+C is pressed)')

parser.add_argument('-d', '--date',
                    default='',
                    metavar='DATE',
                    help='Custom date for upload (format: YYYY-MM-DD). If not provided, uses today\'s date.')

parser.add_argument('-s', '--station',
                    default='',
                    metavar='STATION',
                    help='Station identifier. If not provided, uses STATION environment variable.')

args = parser.parse_args()

# Station validation list
STATION_LIST = ['0206', '0207', '0208', '0303', '0306', '0307', '0308']

# Wrap the main logic in a loop if benchmark mode is enabled
def run_pick_assistant(orchestrator_arg, pod_name_arg, benchmark_mode=False, custom_date='', stationId=''):
    from datetime import datetime, timezone
    
    orchestrator = orchestrator_arg
    PodName = pod_name_arg
    podID = ""
    TrueCycleCount = 0

    # Main input loop with S3 validation
    while True:
        # Get Orchestrator ID first
        while orchestrator == "":
            orchestrator = input("Enter the ID: ").strip()
            if orchestrator == 'exit':
                exit_funct()
        
        # Parse orchestrator path if it contains slashes
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

        # Validate and prompt for station if not provided or invalid
        if not stationId or stationId not in STATION_LIST:
            if stationId and stationId not in STATION_LIST:
                print(f"Invalid station '{stationId}'. Must be one of: {', '.join(STATION_LIST)}")
            while True:
                stationId = input(f"Enter station: ").strip()
                if stationId in STATION_LIST:
                    break
                print(f"Invalid station. Please choose from: {', '.join(STATION_LIST)}")

        # Prompt for date if not provided
        if not custom_date:
            date_input = input("Enter date (YYYY-MM-DD) or press Enter for today: ").strip()
            custom_date = date_input if date_input else datetime.now(timezone.utc).strftime("%Y-%m-%d")
        print(f"Using date: {custom_date}")

        # Prompt for pod ID if not parsed
        if podID == "":
            inputt = input("Enter pod ID (e.g., pod_1): ").strip()
            if inputt.isdigit():
                podID = "pod_" + str(inputt)
            elif inputt.startswith("pod_"):
                podID = inputt
            else:
                podID = "pod_1"

        # Prompt for cycle count if not parsed
        if TrueCycleCount == 0:
            inputt = input("Enter total cycle count (default: 1): ").strip()
            if inputt.isdigit():
                TrueCycleCount = int(inputt)
            elif inputt == "":
                TrueCycleCount = 1
            else:
                print("Invalid cycle count. Using default: 1")
                TrueCycleCount = 1

        # Build S3 URI and validate
        s3_base = f"s3://stow-carbon-copy/Atlas/{stationId}/{custom_date}/{orchestrator}/{podID}/"
        pod_id_s3_uri = s3_base + "cycle_1/dynamic_1/scene_pod_pod_id.data.json"
        pod_type_s3_uri = s3_base + "cycle_1/dynamic_1/scene_pod_pod_fba_family.data.json"
        pod_face_s3_uri = s3_base + "cycle_1/dynamic_1/scene_pod_pod_face.data.json"
        logger.info(f"Checking S3 URI: {s3_base}")
        
        podId = get_json(pod_id_s3_uri)
        podType = get_json(pod_type_s3_uri)
        podFace = get_json(pod_face_s3_uri)
        
        if podId is not None and podType is not None and podFace is not None:
            podBarcode = podId + " " + podType + "-" + podFace
            logger.info(f"S3 URI is valid. Proceeding...")
            break
        
        # S3 validation failed - prompt for retry
        logger.error("Failed to read from S3. Missing files:")
        if podId is None:
            logger.error(f"  - {pod_id_s3_uri}")
        if podType is None:
            logger.error(f"  - {pod_type_s3_uri}")
        if podFace is None:
            logger.error(f"  - {pod_face_s3_uri}")
        retry = input("Retry with different inputs? (y/n): ").strip().lower()
        if retry != 'y':
            if benchmark_mode:
                return False
            exit(1)
        
        # Reset for retry
        orchestrator = ""
        podID = ""
        TrueCycleCount = 0
        stationId = ""
        custom_date = ""

    # Asks for user to input an alias identifier for the pod barcode, if not found in the barcode database.
    if podBarcode in POD_BARCODE_DATABASE:
        PodName = POD_BARCODE_DATABASE[podBarcode]
    if PodName == "":
        PodName = input("Please enter a Pod Identifier like NT or NinjaTurtles: ")
    else:
        logger.info(f"{PodName} was found via the barcode")

    isDone = False
    while not isDone:
        i = 1
        StowedItems = {}
        AttemptedStows = {}
        temp = "/cycle_"
        cycles = 0

        try:
            while True:
                annotation_s3_uri = s3_base + f"cycle_{i}/auto_annotation/_olaf_primary_annotation.data.json"
                stow_location_s3_uri = s3_base + f"cycle_{i}/dynamic_1/match_output.data.json"

                AnnotationData = get_json(annotation_s3_uri)
                StowData = get_json(stow_location_s3_uri)
                
                if AnnotationData is None and StowData is None:
                    break

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
                        logger.info(f"cycle_" + str(i) + " does not have a bin ID.")
                i += 1
            
            if cycles >= TrueCycleCount:
                isDone = True
                read_success = True
                current_state = WorkflowState.READ_COMPLETE
                logger.info(f"State: {current_state.value}")
            else:
                logger.info(f"Cycles missing ({cycles}/{TrueCycleCount}), retrying...")
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
            orchestratorID = orchestrator
            uploadedAT = datetime.now().strftime("%Y-%m-%d")

            # Get system environment variables
            user = os.environ.get('USER')
            station = stationId

            # Build the complete document
            clean_document = {
                "_id": str(uuid.uuid4()),
                "podBarcode": podId,
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

            logger.info(f"Pick list uploaded successfully")
            logger.info(f"Document ID: {result.inserted_id}")
            # Close connection
            client.close()
            logger.info(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            logger.info(f"Pod: {PodName} ({podBarcode})")
            logger.info(f"Orchestrator: {orchestratorID}")
            if benchmark_mode:
                if podFace == "A":
                    logger.info(f"Awaiting {PodName} C face")
            upload_success = True
            current_state = WorkflowState.UPLOAD_COMPLETE
            logger.info(f"State: {current_state.value}")
            return True

        except ImportError:
            upload_success = False
            current_state = WorkflowState.UPLOAD_FAILED
            logger.error(f"State: {current_state.value} - PyMongo not installed")
            logger.error(f"Document was prepared but not uploaded")
            return False
        except Exception as e:
            upload_success = False
            current_state = WorkflowState.UPLOAD_FAILED
            logger.error(f"State: {current_state.value} - {e}")
            logger.error(f"Document was prepared but upload failed")
            return False

    while True:
        result = upload_to_cleans_collection()
        if result:
            return True
        input("\nPress Enter to retry or Ctrl+C to cancel...")
        print("Retrying...")
def credentials_check():
    global result, check
    check = subprocess.run("aws sts get-caller-identity", shell=True, capture_output=True, text=True)
    result = check.returncode
    return result
def exit_funct():
    logger.info('Exiting...')
    exit(1)
# Execute the main function with benchmark mode support
if __name__ == "__main__":
    # Parse command line arguments first
    orchestrator = args.orchestrator
    PodName = args.podname
    benchmark_mode = args.benchmark
    custom_date = args.date
    stationId = args.station
    
    # Check AWS credentials
    if credentials_check() != 0:
        print("\nAWS credentials invalid. Launching refresh-adroit-credentials...\n")
        subprocess.run("zsh -i -c refresh-adroit-credentials", shell=True)
        if credentials_check() != 0:
            print("\nCredentials still invalid. Exiting.\n")
            exit(1)

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

                result = run_pick_assistant(orchestrator, PodName, benchmark_mode, custom_date, stationId)
                if not result:
                    break
                
                # Reset for next iteration to prompt for new inputs
                orchestrator = args.orchestrator
                PodName = args.podname
                custom_date = args.date
                stationId = args.station
        except KeyboardInterrupt:
            exit_funct()
    else:
        # Normal single execution
        run_pick_assistant(orchestrator, PodName, benchmark_mode, custom_date, stationId)