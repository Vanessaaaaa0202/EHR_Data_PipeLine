import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os
import warnings
from data_quality_checker  import DataQualityChecker
from data_quality_checker  import InvalidFormatCleaner, LogicalDateFixer
from data_quality_checker  import InvalidLatitudeLongitudeCleaner
from data_quality_checker  import MissingValueHandler
from data_quality_checker  import DuplicateHandler
from sqlalchemy import create_engine, text
import psycopg2

warnings.filterwarnings('ignore')
pd.set_option('display.max_colwidth', 50)
from tqdm import tqdm

# Airflow 默认设置
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime.now(),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# 定义DAG
dag = DAG(
    'fhir_data_pipeline',
    default_args=default_args,
    description='A DAG to process FHIR data and store in PostgreSQL',
    schedule_interval=timedelta(days=1),
)

# Initialize empty DataFrames for each resource type
patient_df = pd.DataFrame()
careplan_df = pd.DataFrame()
condition_df = pd.DataFrame()
encounter_df = pd.DataFrame()
immunization_df = pd.DataFrame()
observation_df = pd.DataFrame()

# Load data
folder_path = '/Users/vanessa/Desktop/fhir/d8/d8b'


# Process one file
def process_one_file(sample_df,
                     patient_df,
                     careplan_df,
                     condition_df,
                     encounter_df,
                     immunization_df,
                     observation_df):
    # Transform and extract info from json files
    for index, row in sample_df.iterrows():
        tempdf = pd.json_normalize(row['entry'])
        resource_type = str(tempdf['resource.resourceType'][0])

        # Extract patient ID if resource is Patient
        patient_id = None
        if resource_type == "Patient":
            patient_id = tempdf['resource.id'][0]
            tempdf['patient_id'] = patient_id
            patient_df = pd.concat([patient_df, tempdf], ignore_index=True)

        # For all other resource types, extract the resource and add patient_id
        else:
            # Get patient_id from the context or reference if available
            if 'resource.subject.reference' in tempdf.columns:
                patient_reference = tempdf['resource.subject.reference'][0]
                patient_id = patient_reference.replace('urn:uuid:', '')

            # Add patient_id to the tempdf
            tempdf['patient_id'] = patient_id

            # Check resource type and append to corresponding DataFrame
            if resource_type == "CarePlan":
                careplan_df = pd.concat([careplan_df, tempdf], ignore_index=True)

            elif resource_type == "Condition":
                condition_df = pd.concat([condition_df, tempdf], ignore_index=True)

            elif resource_type == "Encounter":
                encounter_df = pd.concat([encounter_df, tempdf], ignore_index=True)

            elif resource_type == "Immunization":
                immunization_df = pd.concat([immunization_df, tempdf], ignore_index=True)

            elif resource_type == "Observation":
                observation_df = pd.concat([observation_df, tempdf], ignore_index=True)

    return patient_df, careplan_df, condition_df, encounter_df, immunization_df, observation_df


# Loop through all files in the folder
for file_name in tqdm(os.listdir(folder_path)):
    file_path = os.path.join(folder_path, file_name)
    # Check if json?
    if os.path.isfile(file_path) and file_name.endswith('.json'):
        sample_df = pd.read_json(file_path)

        # Transform all json files to table structured
        patient_df, \
            careplan_df, \
            condition_df, \
            encounter_df, \
            immunization_df, \
            observation_df = process_one_file(sample_df,
                                              patient_df,
                                              careplan_df,
                                              condition_df,
                                              encounter_df,
                                              immunization_df,
                                              observation_df)


# Function to clean and rename columns for all dataframes
def clean_and_rename(patient_df,
                     careplan_df,
                     condition_df,
                     encounter_df,
                     immunization_df,
                     observation_df, ):
    # Remove 'urn:uuid:' prefix from relevant columns
    for df in [patient_df, observation_df, encounter_df]:
        if 'fullUrl' in df.columns:
            df['fullUrl'] = df['fullUrl'].str.replace('urn:uuid:', '')

    for df in [careplan_df, condition_df]:
        if 'resource.subject.reference' in df.columns:
            df['resource.subject.reference'] = df['resource.subject.reference'].str.replace('urn:uuid:', '')
        if 'resource.context.reference' in df.columns:
            df['resource.context.reference'] = df['resource.context.reference'].str.replace('urn:uuid:', '')

    for df in [encounter_df, immunization_df]:
        if 'resource.patient.reference' in df.columns:
            df['resource.patient.reference'] = df['resource.patient.reference'].str.replace('urn:uuid:', '')

    if 'resource.encounter.reference' in immunization_df.columns:
        immunization_df['resource.encounter.reference'] = immunization_df['resource.encounter.reference'].str.replace(
            'urn:uuid:', '')

    for df in [observation_df]:
        if 'resource.subject.reference' in df.columns:
            df['resource.subject.reference'] = df['resource.subject.reference'].str.replace('urn:uuid:', '')
        if 'resource.encounter.reference' in df.columns:
            df['resource.encounter.reference'] = df['resource.encounter.reference'].str.replace('urn:uuid:', '')

    return (patient_df, careplan_df, condition_df, encounter_df, immunization_df, observation_df)


# Clean and rename all dataframes before refining
patient_df, careplan_df, condition_df, encounter_df, immunization_df, observation_df = clean_and_rename(
    patient_df, careplan_df, condition_df, encounter_df, immunization_df, observation_df)


# Refine patient DataFrame to further process the data
def extract_patient(patient_df):
    # Drop any columns related to photos if they still exist
    patient_df = patient_df.loc[:, ~patient_df.columns.str.contains('photo', case=False)]
    if 'resource.multipleBirthInteger' not in patient_df.columns:
        patient_df['resource.multipleBirthInteger'] = None

    if 'resource.multipleBirthBoolean' not in patient_df.columns:
        patient_df['resource.multipleBirthBoolean'] = None

    if 'resource.address' not in patient_df.columns:
        patient_df['resource.address'] = None

    if 'resource.telecom' not in patient_df.columns:
        patient_df['resource.telecom'] = None

    if 'resource.name' not in patient_df.columns:
        patient_df['resource.name'] = None

    if 'resource.extension' not in patient_df.columns:
        patient_df['resource.extension'] = None

    if 'resource.maritalStatus.coding' not in patient_df.columns:
        patient_df['resource.maritalStatus.coding'] = None

    # Extract latitude and longitude from address information if they exist
    if 'resource.address' in patient_df.columns:
        latitudes = []
        longitudes = []
        full_addresses = []
        for address in patient_df['resource.address']:
            if isinstance(address, list) and 'extension' in address[0]:
                geolocation = address[0]['extension'][0]['extension']
                latitude = next((ext['valueDecimal'] for ext in geolocation if ext['url'] == 'latitude'), None)
                longitude = next((ext['valueDecimal'] for ext in geolocation if ext['url'] == 'longitude'), None)
                latitudes.append(latitude)
                longitudes.append(longitude)
                full_addresses.append(
                    ', '.join(address[0].get('line', [])) + ', ' + address[0].get('city', '') + ', ' + address[0].get(
                        'state', '') + ', ' + address[0].get('postalCode', ''))
            else:
                latitudes.append(None)
                longitudes.append(None)
                full_addresses.append(None)
        patient_df['latitude'] = latitudes
        patient_df['longitude'] = longitudes
        patient_df['full_address'] = full_addresses

    # Drop unnecessary address-related columns
    patient_df.drop(columns=['resource.address'], inplace=True, errors='ignore')

    # Extract phone information from telecom
    if 'resource.telecom' in patient_df.columns:
        phone_numbers = []
        for telecom in patient_df['resource.telecom']:
            if isinstance(telecom, list):
                phone_number = next((t['value'] for t in telecom if t['system'] == 'phone'), None)
                phone_numbers.append(phone_number)
            else:
                phone_numbers.append(None)
        patient_df['contact_number'] = phone_numbers
        patient_df.drop(columns=['resource.telecom'], inplace=True, errors='ignore')

    # Extract first and last names and combine into full name
    if 'resource.name' in patient_df.columns:
        first_names = []
        last_names = []
        full_names = []
        for name in patient_df['resource.name']:
            if isinstance(name, list):
                first_name = name[0].get('given', [None])[0]
                last_name = name[0].get('family', None)
                first_names.append(first_name)
                last_names.append(last_name)
                full_names.append(f"{first_name} {last_name}" if first_name and last_name else None)
            else:
                first_names.append(None)
                last_names.append(None)
                full_names.append(None)
        patient_df['first_name'] = first_names
        patient_df['last_name'] = last_names
        patient_df['full_name'] = full_names
        patient_df.drop(columns=['resource.name'], inplace=True, errors='ignore')

    # Extract race, ethnicity, and birthplace from extensions
    if 'resource.extension' in patient_df.columns:
        races = []
        race_codes = []
        ethnicities = []
        ethnicity_codes = []
        birthplaces = []
        for ext in patient_df['resource.extension']:
            race = None
            race_code = None
            ethnicity = None
            ethnicity_code = None
            birthplace = None
            if isinstance(ext, list):
                for e in ext:
                    if e['url'] == 'http://hl7.org/fhir/StructureDefinition/us-core-race':
                        race = e.get('valueCodeableConcept', {}).get('coding', [{}])[0].get('display', None)
                        race_code = e.get('valueCodeableConcept', {}).get('coding', [{}])[0].get('code', None)
                    elif e['url'] == 'http://hl7.org/fhir/StructureDefinition/us-core-ethnicity':
                        ethnicity = e.get('valueCodeableConcept', {}).get('coding', [{}])[0].get('display', None)
                        ethnicity_code = e.get('valueCodeableConcept', {}).get('coding', [{}])[0].get('code', None)
                    elif e['url'] == 'http://standardhealthrecord.org/fhir/extensions/placeOfBirth':
                        birthplace_info = e.get('valueAddress', {})
                        birthplace = f"{birthplace_info.get('city', '')}, {birthplace_info.get('state', '')}"
            races.append(race)
            race_codes.append(race_code)
            ethnicities.append(ethnicity)
            ethnicity_codes.append(ethnicity_code)
            birthplaces.append(birthplace)
        patient_df['race'] = races
        patient_df['race_code'] = race_codes
        patient_df['ethnicity'] = ethnicities
        patient_df['ethnicity_code'] = ethnicity_codes
        patient_df['birthplace'] = birthplaces
        patient_df.drop(columns=['resource.extension'], inplace=True, errors='ignore')

    # Extract marital status code
    if 'resource.maritalStatus.coding' in patient_df.columns:
        marital_status_codes = []
        for marital_status in patient_df['resource.maritalStatus.coding']:
            if isinstance(marital_status, list):
                marital_status_code = marital_status[0].get('code', None)
                marital_status_codes.append(marital_status_code)
            else:
                marital_status_codes.append(None)
        patient_df['maritalStatus'] = marital_status_codes
        patient_df.drop(columns=['resource.maritalStatus.coding'], inplace=True, errors='ignore')

    return patient_df


# Refine the patient DataFrame
patient_df = extract_patient(patient_df)


def extract_encounter(encounter_df):
    # Extract 'code', 'text' from 'type', and 'code', 'display' from 'reason'
    encounter_types = []
    encounter_texts = []
    reason_codes = []
    reason_displays = []

    for _, row in encounter_df.iterrows():
        # Extract 'code' and 'text' from 'type'
        if 'resource.type' in row and isinstance(row['resource.type'], list) and 'coding' in row['resource.type'][0]:
            encounter_code = row['resource.type'][0]['coding'][0].get('code', None)
            encounter_text = row['resource.type'][0].get('text', None)
        else:
            encounter_code = None
            encounter_text = None

        encounter_types.append(encounter_code)
        encounter_texts.append(encounter_text)

        # Extract 'code' and 'display' from 'reason.coding'
        if 'resource.reason.coding' in row and isinstance(row['resource.reason.coding'], list):
            reason_code = row['resource.reason.coding'][0].get('code', None)
            reason_display = row['resource.reason.coding'][0].get('display', None)
        else:
            reason_code = None
            reason_display = None

        reason_codes.append(reason_code)
        reason_displays.append(reason_display)

    # Ensure the lists have the same length as the dataframe
    encounter_df['encounter_type_code'] = encounter_types
    encounter_df['encounter_text'] = encounter_texts
    encounter_df['reason_code'] = reason_codes
    encounter_df['reason_display'] = reason_displays

    return encounter_df


# 调用函数来处理 encounter_df
encounter_df = extract_encounter(encounter_df)


# Function to extract display value from Condition resource
def extract_condition(condition_df):
    # Extract 'display' value and 'code' from 'Condition' resources
    condition_displays = []
    condition_codes = []
    for _, row in condition_df.iterrows():
        if 'resource.code.coding' in row:
            coding_list = row['resource.code.coding']
            if isinstance(coding_list, list):
                display = coding_list[0].get('display', None)
                code = coding_list[0].get('code', None)
                condition_displays.append(display)
                condition_codes.append(code)
            else:
                condition_displays.append(None)
                condition_codes.append(None)
        else:
            condition_displays.append(None)
            condition_codes.append(None)
    condition_df['condition_display'] = condition_displays
    condition_df['condition_code'] = condition_codes
    return condition_df


# Extract 'display' values from Condition DataFrame
condition_df = extract_condition(condition_df)


# Function to extract display value and code from Observation resource
def extract_observation(observation_df):
    # Extract 'display' value and 'code' from 'Observation' resources
    observation_displays = []
    observation_codes = []
    for _, row in observation_df.iterrows():
        if 'resource.code.coding' in row:
            coding_list = row['resource.code.coding']
            if isinstance(coding_list, list):
                display = coding_list[0].get('display', None)
                code = coding_list[0].get('code', None)
                observation_displays.append(display)
                observation_codes.append(code)
            else:
                observation_displays.append(None)
                observation_codes.append(None)
        else:
            observation_displays.append(None)
            observation_codes.append(None)
    observation_df['observation_type'] = observation_displays
    observation_df['observation_code'] = observation_codes
    return observation_df


# Extract 'display' and 'code' values from Observation DataFrame
observation_df = extract_observation(observation_df)


# Function to extract display value and code from Immunization resource
def extract_immunization(immunization_df):
    # Extract 'display' value and 'code' from 'Immunization' resources
    vaccine_displays = []
    vaccine_codes = []
    for _, row in immunization_df.iterrows():
        if 'resource.vaccineCode.coding' in row:
            coding_list = row['resource.vaccineCode.coding']
            if isinstance(coding_list, list):
                display = coding_list[0].get('display', None)
                code = coding_list[0].get('code', None)
                vaccine_displays.append(display)
                vaccine_codes.append(code)
            else:
                vaccine_displays.append(None)
                vaccine_codes.append(None)
        else:
            vaccine_displays.append(None)
            vaccine_codes.append(None)
    immunization_df['vaccine_type'] = vaccine_displays
    immunization_df['vaccine_code'] = vaccine_codes
    return immunization_df


# Extract 'display' and 'code' values from Immunization DataFrame
immunization_df = extract_immunization(immunization_df)


def extract_careplan(careplan_df):
    # Extract 'display' values for careplan name, activities, and codes
    careplan_names = []
    careplan_codes = []
    careplan_activities = []
    careplan_activity_codes = []

    for _, row in careplan_df.iterrows():
        # Extract CarePlan name (category display) and code
        if 'resource.category' in row:
            category_list = row['resource.category']
            if isinstance(category_list, list) and 'coding' in category_list[0]:
                careplan_name = category_list[0]['coding'][0].get('display', None)
                careplan_code = category_list[0]['coding'][0].get('code', None)
            else:
                careplan_name = None
                careplan_code = None
        else:
            careplan_name = None
            careplan_code = None

        careplan_names.append(careplan_name)
        careplan_codes.append(careplan_code)

        # Extract CarePlan activities (detail.code display) and codes
        activities = []
        activity_codes = []
        if 'resource.activity' in row:
            activity_list = row['resource.activity']
            if isinstance(activity_list, list):
                for activity in activity_list:
                    activity_detail = activity.get('detail', {}).get('code', {}).get('coding', [{}])[0]
                    activity_display = activity_detail.get('display', None)
                    activity_code = activity_detail.get('code', None)
                    if activity_display:
                        activities.append(activity_display)
                    if activity_code:
                        activity_codes.append(activity_code)
        careplan_activities.append(", ".join(activities) if activities else None)
        careplan_activity_codes.append(", ".join(activity_codes) if activity_codes else None)

    # Ensure the lists have the same length as the dataframe
    careplan_df['careplan_name'] = careplan_names
    careplan_df['careplan_code'] = careplan_codes
    careplan_df['careplan_activity'] = careplan_activities
    careplan_df['careplan_activity_code'] = careplan_activity_codes

    return careplan_df


# Extract 'display' values and codes from CarePlan DataFrame
careplan_df = extract_careplan(careplan_df)


def drop_columns(patient_df,
                 careplan_df,
                 condition_df,
                 encounter_df,
                 immunization_df,
                 observation_df):
    # Columns to drop from each dataframe
    careplan_cols_to_drop = ['resource.category', 'resource.activity', 'resource.resourceType', 'resource.addresses']
    condition_cols_to_drop = ['resource.code.coding', 'resource.resourceType']
    encounter_cols_to_drop = ['resource.resourceType', 'resource.type', 'resource.reason.coding']
    immunization_cols_to_drop = ['resource.resourceType', 'resource.vaccineCode.coding']
    observation_cols_to_drop = [
        'resource.resourceType', 'resource.code.coding', 'resource.valueQuantity.system',
        'resource.valueQuantity.code', 'resource.component', 'resource.valueCodeableConcept.coding'
    ]
    patient_cols_to_drop = ['resource.text.status', 'resource.text.div', 'resource.identifier', 'resource.resourceType']

    # Drop columns if they exist
    careplan_df.drop(columns=careplan_cols_to_drop, inplace=True, errors='ignore')
    condition_df.drop(columns=condition_cols_to_drop, inplace=True, errors='ignore')
    encounter_df.drop(columns=encounter_cols_to_drop, inplace=True, errors='ignore')
    immunization_df.drop(columns=immunization_cols_to_drop, inplace=True, errors='ignore')
    observation_df.drop(columns=observation_cols_to_drop, inplace=True, errors='ignore')
    patient_df.drop(columns=patient_cols_to_drop, inplace=True, errors='ignore')

    return (patient_df, careplan_df, condition_df, encounter_df, immunization_df, observation_df)


# Drop specified columns from dataframes
patient_df, careplan_df, condition_df, encounter_df, immunization_df, observation_df = drop_columns(
    patient_df, careplan_df, condition_df, encounter_df, immunization_df, observation_df
)


# Initial Quality Check
def initial_quality_check(patient_df, careplan_df, condition_df, observation_df, encounter_df, immunization_df):
    dfs = [patient_df, careplan_df, condition_df, observation_df, encounter_df, immunization_df]
    names = ['patient_df', 'careplan_df', 'condition_df', 'observation_df', 'encounter_df', 'immunization_df']

    initial_checker = DataQualityChecker(dfs, names)
    initial_missing_summary = initial_checker.missing_value_analysis()
    print("Initial Quality Check - Missing Value Analysis:")
    for name, summary in initial_missing_summary.items():
        print(f"{name}:\n{summary}\n")

    initial_quality_summary_df = initial_checker.run_quality_checks()
    print("Initial Quality Check - Detailed Analysis:")
    print(initial_quality_summary_df)
    return patient_df, careplan_df, condition_df, observation_df, encounter_df, immunization_df


#################### Cleaning Process####################
# Clean and process patient DataFrame using class-based approach
def clean_patient_df(patient_df):
    print("\nBefore cleaning:")
    print(f"Number of rows: {len(patient_df)}")
    print(f"Missing values before cleaning:\n{patient_df.isna().sum()}")

    # Step 1: 处理缺失值，使用 MissingValueHandler 类
    missing_handler = MissingValueHandler()
    patient_df = missing_handler.clean_patient_df(patient_df)

    # Step 2: 处理无效格式，使用 InvalidFormatCleaner 类
    invalid_format_cleaner = InvalidFormatCleaner([patient_df], ['patient_df'])
    cleaned_dfs = invalid_format_cleaner.clean_invalid_formats()
    patient_df = cleaned_dfs[0]  # 只处理patient_df

    # Step 3: 去重，使用 DuplicateHandler 类
    duplicate_handler = DuplicateHandler()
    patient_df = duplicate_handler.deduplicate(patient_df, 'resource.id', 'patient_df')

    # Step 4: 清理无效的纬度和经度，使用 InvalidLatitudeLongitudeCleaner 类
    lat_lon_cleaner = InvalidLatitudeLongitudeCleaner([patient_df], ['patient_df'])
    cleaned_dfs = lat_lon_cleaner.clean_invalid_lat_lon()
    patient_df = cleaned_dfs[0]  # 只处理patient_df

    print("\nAfter cleaning:")
    print(f"Number of rows: {len(patient_df)}")
    print(f"Missing values after cleaning:\n{patient_df.isna().sum()}")

    # 返回清理后的 patient_df
    return patient_df


def clean_encounter_df(encounter_df):
    print("\nBefore cleaning Encounter:")
    print(f"Number of rows: {len(encounter_df)}")
    print(f"Missing values before cleaning:\n{encounter_df.isna().sum()}")

    # Step 1: 处理缺失值，使用 MissingValueHandler 类
    missing_handler = MissingValueHandler()
    encounter_df = missing_handler.clean_encounter_df(encounter_df)

    # Step 2: 处理无效格式，使用 InvalidFormatCleaner 类
    invalid_format_cleaner = InvalidFormatCleaner([encounter_df], ['encounter_df'])
    cleaned_dfs = invalid_format_cleaner.clean_invalid_formats()
    encounter_df = cleaned_dfs[0]

    # Step 3: 逻辑日期修复，使用 LogicalDateFixer 类
    logical_date_fixer = LogicalDateFixer([encounter_df], ['encounter_df'])
    fixed_dfs = logical_date_fixer.fix_logical_dates()
    encounter_df = fixed_dfs[0]

    # Step 4: 去重，使用 DuplicateHandler 类
    duplicate_handler = DuplicateHandler()
    encounter_df = duplicate_handler.deduplicate(encounter_df, 'resource.id', 'encounter_df')

    print("\nAfter cleaning Encounter:")
    print(f"Number of rows: {len(encounter_df)}")
    print(f"Missing values after cleaning:\n{encounter_df.isna().sum()}")

    return encounter_df


def clean_careplan_df(careplan_df):
    print("\nBefore cleaning CarePlan:")
    print(f"Number of rows: {len(careplan_df)}")
    print(f"Missing values before cleaning:\n{careplan_df.isna().sum()}")

    # Step 2: 处理无效格式，使用 InvalidFormatCleaner 类
    invalid_format_cleaner = InvalidFormatCleaner([careplan_df], ['careplan_df'])
    cleaned_dfs = invalid_format_cleaner.clean_invalid_formats()
    careplan_df = cleaned_dfs[0]

    # Step 3: 逻辑日期修复，使用 LogicalDateFixer 类
    logical_date_fixer = LogicalDateFixer([careplan_df], ['careplan_df'])
    fixed_dfs = logical_date_fixer.fix_logical_dates()
    careplan_df = fixed_dfs[0]

    print("\nAfter cleaning CarePlan:")
    print(f"Number of rows: {len(careplan_df)}")
    print(f"Missing values after cleaning:\n{careplan_df.isna().sum()}")

    return careplan_df


def clean_condition_df(condition_df):
    print("\nBefore cleaning Condition:")
    print(f"Number of rows: {len(condition_df)}")
    print(f"Missing values before cleaning:\n{condition_df.isna().sum()}")

    # Step 1: 处理缺失值，使用 MissingValueHandler 类
    missing_handler = MissingValueHandler()
    condition_df = missing_handler.clean_condition_df(condition_df)

    # Step 2: 处理无效格式，使用 InvalidFormatCleaner 类
    invalid_format_cleaner = InvalidFormatCleaner([condition_df], ['condition_df'])
    cleaned_dfs = invalid_format_cleaner.clean_invalid_formats()
    condition_df = cleaned_dfs[0]

    # Step 3: 逻辑日期修复，使用 LogicalDateFixer 类
    logical_date_fixer = LogicalDateFixer([condition_df], ['condition_df'])
    fixed_dfs = logical_date_fixer.fix_logical_dates()
    condition_df = fixed_dfs[0]

    # Step 4: 去重，使用 DuplicateHandler 类
    duplicate_handler = DuplicateHandler()
    condition_df = duplicate_handler.deduplicate(condition_df, 'resource.id', 'condition_df')

    print("\nAfter cleaning Condition:")
    print(f"Number of rows: {len(condition_df)}")
    print(f"Missing values after cleaning:\n{condition_df.isna().sum()}")

    return condition_df


def clean_observation_df(observation_df):
    print("\nBefore cleaning Observation:")
    print(f"Number of rows: {len(observation_df)}")
    print(f"Missing values before cleaning:\n{observation_df.isna().sum()}")

    # Step 1: 处理缺失值，使用 MissingValueHandler 类
    missing_handler = MissingValueHandler()
    observation_df = missing_handler.clean_observation_df(observation_df)

    # Step 2: 处理无效格式，使用 InvalidFormatCleaner 类
    invalid_format_cleaner = InvalidFormatCleaner([observation_df], ['observation_df'])
    cleaned_dfs = invalid_format_cleaner.clean_invalid_formats()
    observation_df = cleaned_dfs[0]

    # Step 3: 去重，使用 DuplicateHandler 类
    duplicate_handler = DuplicateHandler()
    observation_df = duplicate_handler.deduplicate(observation_df, 'resource.id', 'observation_df')

    print("\nAfter cleaning Observation:")
    print(f"Number of rows: {len(observation_df)}")
    print(f"Missing values after cleaning:\n{observation_df.isna().sum()}")

    return observation_df


def clean_immunization_df(immunization_df):
    print("\nBefore cleaning Immunization:")
    print(f"Number of rows: {len(immunization_df)}")
    print(f"Missing values before cleaning:\n{immunization_df.isna().sum()}")

    # Step 1: 处理缺失值，使用 MissingValueHandler 类
    missing_handler = MissingValueHandler()
    immunization_df = missing_handler.clean_immunization_df(immunization_df)

    # Step 2: 处理无效格式，使用 InvalidFormatCleaner 类
    invalid_format_cleaner = InvalidFormatCleaner([immunization_df], ['immunization_df'])
    cleaned_dfs = invalid_format_cleaner.clean_invalid_formats()
    immunization_df = cleaned_dfs[0]

    print("\nAfter cleaning Immunization:")
    print(f"Number of rows: {len(immunization_df)}")
    print(f"Missing values after cleaning:\n{immunization_df.isna().sum()}")

    return immunization_df


patient_df = clean_patient_df(patient_df)
encounter_df = clean_encounter_df(encounter_df)
careplan_df = clean_careplan_df(careplan_df)
condition_df = clean_condition_df(condition_df)
observation_df = clean_observation_df(observation_df)
immunization_df = clean_immunization_df(immunization_df)


# Task 15: Final Data Quality Check
def final_quality_check(patient_df, careplan_df, condition_df, observation_df, encounter_df, immunization_df):
    # 将所有需要检查的 DataFrame 和名称放在一起
    dfs = [patient_df, careplan_df, condition_df, observation_df, encounter_df, immunization_df]
    names = ['patient_df', 'careplan_df', 'condition_df', 'observation_df', 'encounter_df', 'immunization_df']

    # 创建 DataQualityChecker 实例
    final_checker = DataQualityChecker(dfs, names)

    # 执行缺失值分析
    final_missing_summary = final_checker.missing_value_analysis()
    print("Final Quality Check - Missing Value Analysis:")
    for name, summary in final_missing_summary.items():
        print(f"{name}:\n{summary}\n")

    # 执行详细的数据质量检查
    final_quality_summary_df = final_checker.run_quality_checks()
    print("Final Quality Check - Detailed Analysis:")
    print(final_quality_summary_df)

    # 返回处理后的 DataFrames
    return patient_df, careplan_df, condition_df, observation_df, encounter_df, immunization_df


#################### Load Data ####################
# PostgreSQL连接参数
DB_PARAMS = {
    "dbname": "postgres",
    "user": "postgres",
    "password": "leijietong",
    "host": "localhost"
}


# SQL table creation queries
def create_tables():
    """使用psycopg2创建数据表"""
    conn = psycopg2.connect(**DB_PARAMS)
    try:
        with conn.cursor() as cur:
            # Create tables
            # Patient table (需要先创建，因为其他表依赖它)
            cur.execute("""
                CREATE TABLE patient (
                    fullUrl TEXT,
                    resource_id TEXT,
                    resource_gender TEXT,
                    resource_birthDate DATE,
                    resource_multipleBirthBoolean BOOLEAN,
                    patient_id TEXT PRIMARY KEY,
                    resource_deceasedDateTime TEXT,
                    resource_multipleBirthInteger INT,
                    latitude FLOAT,
                    longitude FLOAT,
                    full_address TEXT,
                    contact_number TEXT,
                    first_name TEXT,
                    last_name TEXT,
                    full_name TEXT,
                    race TEXT,
                    race_code TEXT,
                    ethnicity TEXT,
                    ethnicity_code TEXT,
                    birthplace TEXT,
                    maritalStatus TEXT
                );
            """)

            # CarePlan table
            cur.execute("""
                CREATE TABLE careplan (
                    resource_status TEXT,
                    resource_subject_reference TEXT,
                    resource_context_reference TEXT,
                    resource_period_start TEXT,
                    patient_id TEXT,
                    resource_period_end TEXT,
                    careplan_name TEXT,
                    careplan_code TEXT,
                    careplan_activity TEXT,
                    careplan_activity_code TEXT
                );
            """)

            # Condition table
            cur.execute("""
                CREATE TABLE condition (
                    fullUrl TEXT,
                    resource_id TEXT,
                    resource_clinicalStatus TEXT,
                    resource_verificationStatus TEXT,
                    resource_subject_reference TEXT,
                    resource_context_reference TEXT,
                    resource_onsetDateTime TEXT,
                    patient_id TEXT,
                    resource_abatementDateTime TEXT,
                    condition_display TEXT,
                    condition_code TEXT
                );
            """)

            # Encounter table
            cur.execute("""
                CREATE TABLE encounter (
                    fullUrl TEXT,
                    resource_id TEXT,
                    resource_status TEXT,
                    resource_class_code TEXT,
                    resource_patient_reference TEXT,
                    resource_period_start TEXT,
                    resource_period_end TEXT,
                    patient_id TEXT,
                    encounter_type_code TEXT,
                    encounter_text TEXT,
                    reason_code TEXT,
                    reason_display TEXT
                );
            """)

            # Immunization table
            cur.execute("""
                CREATE TABLE immunization (
                    resource_status TEXT,
                    resource_date DATE,
                    resource_patient_reference TEXT,
                    resource_wasNotGiven BOOLEAN,
                    resource_primarySource BOOLEAN,
                    resource_encounter_reference TEXT,
                    patient_id TEXT,
                    vaccine_type TEXT,
                    vaccine_code TEXT
                );
            """)

            # Observation table
            cur.execute("""
                CREATE TABLE observation (
                    fullUrl TEXT,
                    resource_id TEXT,
                    resource_status TEXT,
                    resource_subject_reference TEXT,
                    resource_encounter_reference TEXT,
                    resource_effectiveDateTime TEXT,
                    resource_valueQuantity_value FLOAT,
                    resource_valueQuantity_unit TEXT,
                    patient_id TEXT,
                    observation_type TEXT,
                    observation_code TEXT
                );
            """)

            conn.commit()
            print("Successfully created all tables!")

    except Exception as e:
        print(f"Error creating tables: {str(e)}")
        conn.rollback()
        raise
    finally:
        conn.close()


def load_dataframe_to_table(df, table_name, conn):
    """使用psycopg2加载DataFrame到指定表"""
    # 复制DataFrame避免修改原始数据
    df_copy = df.copy()

    # 将列名中的点替换为下划线
    df_copy.columns = df_copy.columns.str.replace('.', '_')

    # 获取列名
    columns = df_copy.columns.tolist()

    # 修改SQL插入语句，使用execute_values的特殊语法
    insert_sql = f"""
        INSERT INTO {table_name} ({','.join(columns)}) 
        VALUES %s
    """

    try:
        with conn.cursor() as cur:
            # 将DataFrame转换为元组列表
            tuples = [tuple(x) for x in df_copy.values]
            # 使用execute_values进行批量插入
            from psycopg2.extras import execute_values
            execute_values(cur, insert_sql, tuples)
        conn.commit()
        print(f"Successfully loaded data into {table_name}")
    except Exception as e:
        print(f"Error loading data into {table_name}: {str(e)}")
        conn.rollback()
        raise


def load_data_to_postgresql():
    """主数据加载函数"""
    global patient_df, careplan_df, condition_df, encounter_df, immunization_df, observation_df

    try:
        # 创建表
        create_tables()

        # 创建新的连接用于数据加载
        conn = psycopg2.connect(**DB_PARAMS)

        try:
            print("Loading data into PostgreSQL...")

            # 按照依赖顺序加载数据
            print("Loading patient data...")
            load_dataframe_to_table(patient_df, 'patient', conn)

            print("Loading careplan data...")
            load_dataframe_to_table(careplan_df, 'careplan', conn)

            print("Loading condition data...")
            load_dataframe_to_table(condition_df, 'condition', conn)

            print("Loading encounter data...")
            load_dataframe_to_table(encounter_df, 'encounter', conn)

            print("Loading immunization data...")
            load_dataframe_to_table(immunization_df, 'immunization', conn)

            print("Loading observation data...")
            load_dataframe_to_table(observation_df, 'observation', conn)

            print("All data successfully loaded into PostgreSQL database.")

        finally:
            conn.close()

    except Exception as e:
        print(f"Error in load_data_to_postgresql: {str(e)}")
        raise


# Airflow 任务
def load_data():
    global patient_df, careplan_df, condition_df, encounter_df, immunization_df, observation_df
    folder_path = '/Users/vanessa/Desktop/fhir/d8/d8b'
    # Your existing code to load data from folder and process one file
    for file_name in tqdm(os.listdir(folder_path)):
        file_path = os.path.join(folder_path, file_name)
        if os.path.isfile(file_path) and file_name.endswith('.json'):
            sample_df = pd.read_json(file_path)
            patient_df, careplan_df, condition_df, encounter_df, immunization_df, observation_df = process_one_file(
                sample_df, patient_df, careplan_df, condition_df, encounter_df, immunization_df, observation_df
            )


# Task 2: Clean and Rename Columns
def clean_and_rename_columns():
    global patient_df, careplan_df, condition_df, encounter_df, immunization_df, observation_df
    patient_df, careplan_df, condition_df, encounter_df, immunization_df, observation_df = clean_and_rename(
        patient_df, careplan_df, condition_df, encounter_df, immunization_df, observation_df
    )


# Task 3: Refine Patient Data
def extract_patient_data():
    global patient_df
    patient_df = extract_patient(patient_df)


# Task 4: Split_encounter_type
def extract_encounter_data():
    global encounter_df
    encounter_df = extract_encounter(encounter_df)

    # Task for extracting CarePlan data


def extract_careplan_data():
    global careplan_df
    careplan_df = extract_careplan(careplan_df)


# Task for extracting Condition data
def extract_condition_data():
    global condition_df
    condition_df = extract_condition(condition_df)


# Task for extracting Observation data
def extract_observation_data():
    global observation_df
    observation_df = extract_observation(observation_df)


def extract_immunization_data():
    global immunization_df
    immunization_df = extract_immunization(immunization_df)


# Task 5: Drop Unnecessary Columns
def drop_columns_from_dfs():
    global patient_df, careplan_df, condition_df, encounter_df, immunization_df, observation_df
    patient_df, careplan_df, condition_df, encounter_df, immunization_df, observation_df = drop_columns(
        patient_df, careplan_df, condition_df, encounter_df, immunization_df, observation_df
    )


def task_initial_quality_check(**context):
    global patient_df, careplan_df, condition_df, observation_df, encounter_df, immunization_df
    patient_df, careplan_df, condition_df, observation_df, encounter_df, immunization_df = initial_quality_check(
        patient_df, careplan_df, condition_df, observation_df, encounter_df, immunization_df
    )


################ Data Cleaning ###############
def task_clean_patient_df():
    global patient_df
    patient_df = clean_patient_df(patient_df)


def task_clean_encounter_df():
    global encounter_df
    encounter_df = clean_encounter_df(encounter_df)


# Task: Clean CarePlan DataFrame
def task_clean_careplan_df():
    global careplan_df
    careplan_df = clean_careplan_df(careplan_df)


# Task: Clean Condition DataFrame
def task_clean_condition_df():
    global condition_df
    condition_df = clean_condition_df(condition_df)


# Task: Clean Observation DataFrame
def task_clean_observation_df():
    global observation_df
    observation_df = clean_observation_df(observation_df)


# Task: Clean Immunization DataFrame
def task_clean_immunization_df():
    global immunization_df
    immunization_df = clean_immunization_df(immunization_df)


################ Data Cleaning ###############


def task_final_quality_check():
    global patient_df, careplan_df, condition_df, observation_df, encounter_df, immunization_df
    patient_df, careplan_df, condition_df, observation_df, encounter_df, immunization_df = final_quality_check(
        patient_df, careplan_df, condition_df, observation_df, encounter_df, immunization_df
    )


# Task 8: Output DataFrames to CSV on Desktop

def output_to_csv():
    global patient_df, careplan_df, condition_df, encounter_df, immunization_df, observation_df
    output_folder = '/Users/vanessa/Desktop/fhir_processed_output'
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)

    print("\nBefore saving to CSV:")
    print("Birth fields state:")
    print(patient_df[['resource.multipleBirthInteger', 'resource.multipleBirthBoolean']].isnull().sum())

    # 输出到CSV
    patient_df.to_csv(os.path.join(output_folder, 'patient_df.csv'), index=False)
    careplan_df.to_csv(os.path.join(output_folder, 'careplan_df.csv'), index=False)
    condition_df.to_csv(os.path.join(output_folder, 'condition_df.csv'), index=False)
    encounter_df.to_csv(os.path.join(output_folder, 'encounter_df.csv'), index=False)
    immunization_df.to_csv(os.path.join(output_folder, 'immunization_df.csv'), index=False)
    observation_df.to_csv(os.path.join(output_folder, 'observation_df.csv'), index=False)

    print("DataFrames have been successfully saved to:", output_folder)


# Airflow Tasks

load_data_task = PythonOperator(
    task_id='load_data_task',
    python_callable=load_data,
    dag=dag
)

clean_and_rename_columns_task = PythonOperator(
    task_id='clean_and_rename_columns_task',
    python_callable=clean_and_rename_columns,
    dag=dag
)

extract_patient_data_task = PythonOperator(
    task_id='extract_patient_data_task',
    python_callable=extract_patient_data,
    dag=dag
)

extract_encounter_types_task = PythonOperator(
    task_id='extract_encounter_types_task',
    python_callable=extract_encounter_data,
    dag=dag
)

extract_careplan_data_task = PythonOperator(
    task_id='extract_careplan_data_task',
    python_callable=extract_careplan_data,
    dag=dag
)

extract_condition_data_task = PythonOperator(
    task_id='extract_condition_data_task',
    python_callable=extract_condition_data,
    dag=dag
)

extract_observation_data_task = PythonOperator(
    task_id='extract_observation_data_task',
    python_callable=extract_observation_data,
    dag=dag
)

extract_immunization_data_task = PythonOperator(
    task_id='extract_immunization_data_task',
    python_callable=extract_immunization_data,
    dag=dag
)

drop_columns_task = PythonOperator(
    task_id='drop_columns_task',
    python_callable=drop_columns_from_dfs,
    dag=dag
)

# Task 9: Initial Data Quality Check
initial_quality_check_task = PythonOperator(
    task_id='initial_quality_check_task',
    python_callable=task_initial_quality_check,
    provide_context=True,
    dag=dag
)

#################Data Cleaning#################
task_clean_patient_df = PythonOperator(
    task_id='task_clean_patient_df',
    python_callable=task_clean_patient_df,
    dag=dag
)

# Task for cleaning Encounter Data
task_clean_encounter_df = PythonOperator(
    task_id='task_clean_encounter_df',
    python_callable=task_clean_encounter_df,
    dag=dag
)

# Task for cleaning CarePlan Data
task_clean_careplan_df = PythonOperator(
    task_id='task_clean_careplan_df',
    python_callable=task_clean_careplan_df,
    dag=dag
)

# Task for cleaning Condition Data
task_clean_condition_df = PythonOperator(
    task_id='task_clean_condition_df',
    python_callable=task_clean_condition_df,
    dag=dag
)

# Task for cleaning Observation Data
task_clean_observation_df = PythonOperator(
    task_id='task_clean_observation_df',
    python_callable=task_clean_observation_df,
    dag=dag
)

# Task for cleaning Immunization Data
task_clean_immunization_df = PythonOperator(
    task_id='task_clean_immunization_df',
    python_callable=task_clean_immunization_df,
    dag=dag
)

# Task 15: Final Data Quality Check
final_quality_check_task = PythonOperator(
    task_id='final_quality_check_task',
    python_callable=task_final_quality_check,
    dag=dag
)

# New task: Load data to PostgreSQL
load_data_to_postgresql_task = PythonOperator(
    task_id='load_data_to_postgresql_task',
    python_callable=load_data_to_postgresql,
    dag=dag
)

# 定义输出任务的 Airflow Task
output_to_csv_task = PythonOperator(
    task_id='output_to_csv_task',
    python_callable=output_to_csv,
    dag=dag
)

###################### DAG ######################
# 数据加载和初步处理流程
load_data_task >> clean_and_rename_columns_task >> [
    extract_patient_data_task,
    extract_encounter_types_task,
    extract_careplan_data_task,
    extract_condition_data_task,
    extract_observation_data_task,
    extract_immunization_data_task
] >> drop_columns_task

# 初始数据质量检查
drop_columns_task >> initial_quality_check_task

# 清理任务链条
initial_quality_check_task >> [
    task_clean_patient_df,
    task_clean_encounter_df,
    task_clean_careplan_df,
    task_clean_condition_df,
    task_clean_observation_df,
    task_clean_immunization_df
]

# 最终数据质量检查和输出
[
    task_clean_patient_df,
    task_clean_encounter_df,
    task_clean_careplan_df,
    task_clean_condition_df,
    task_clean_observation_df,
    task_clean_immunization_df
] >> final_quality_check_task >> load_data_to_postgresql_task >> output_to_csv_task