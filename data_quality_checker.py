import pandas as pd
import re

class DataQualityChecker:
    def __init__(self, dfs, names):
        self.dfs = dfs
        self.names = names

    # General function to perform missing value analysis on multiple DataFrames
    def missing_value_analysis(self):
        results = {}
        for df, name in zip(self.dfs, self.names):
            # Calculate total missing values for each column
            missing_values = df.isna().sum()

            # Calculate percentage of missing values for each column
            missing_percentage = (missing_values / len(df)) * 100

            # Create a DataFrame to summarize missing value analysis
            missing_summary_df = pd.DataFrame({
                'Total Missing Values': missing_values,
                'Percentage Missing (%)': missing_percentage
            })

            # Sort the summary by percentage of missing values
            missing_summary_df.sort_values(by='Percentage Missing (%)', ascending=False, inplace=True)

            # Store the result in a dictionary
            results[name] = missing_summary_df

        return results

    # General function to validate column values based on a condition
    def validate_column(self, df, column_name, validation_fn, error_type, df_name):
        error_count = 0
        for _, row in df.iterrows():
            value = row[column_name]
            if not pd.isna(value) and not validation_fn(value):
                error_count += 1

        # Calculate the percentage of errors
        total_rows = len(df)
        error_percentage = (error_count / total_rows) * 100

        return [df_name, error_type, error_count, error_percentage]

    # Validation functions
    @staticmethod
    def is_valid_datetime(value):
        return bool(re.match(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}[-+]\d{2}:\d{2}$", value))

    @staticmethod
    def is_valid_date(value):
        return bool(re.match(r"^\d{4}-\d{2}-\d{2}$", value))

    @staticmethod
    def is_valid_birthdate(value):
        return bool(re.match(r"^\d{4}-\d{2}-\d{2}$", value))

    @staticmethod
    def is_valid_deceased_datetime(value):
        return bool(re.match(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}[-+]\d{2}:\d{2}$", value))

    @staticmethod
    def is_valid_contact_number(value):
        patterns = [
            r"^\(\d{3}\)\s?\d{3}-\d{4}( x\d+)?$",  # (xxx) xxx-xxxx or (xxx)xxx-xxxx, optional extension
            r"^\d{3}-\d{3}-\d{4}( x\d+)?$",  # xxx-xxx-xxxx, optional extension
            r"^\d{3}\.\d{3}\.\d{4}( x\d+)?$",  # xxx.xxx.xxxx, optional extension
            r"^1-\d{3}-\d{3}-\d{4}( x\d+)?$"  # 1-xxx-xxx-xxxx, optional extension
        ]
        return any(re.match(pattern, value) for pattern in patterns)

    @staticmethod
    def is_valid_latitude(value):
        return -90 <= value <= 90

    @staticmethod
    def is_valid_longitude(value):
        return -180 <= value <= 180

    # Logical check function for start and end date
    def validate_logical_date(self, df, start_column, end_column, error_type, df_name):
        logical_issue_count = 0
        for _, row in df.iterrows():
            start_value = row[start_column]
            end_value = row[end_column]
            if not pd.isna(start_value) and not pd.isna(end_value) and start_value > end_value:
                logical_issue_count += 1

        total_rows = len(df)
        logical_error_percentage = (logical_issue_count / total_rows) * 100

        return [df_name, error_type, logical_issue_count, logical_error_percentage]

    # General function to check duplicate values
    def validate_duplicates(self, df, column_names, error_type, df_name):
        duplicate_count = df.duplicated(subset=column_names, keep=False).sum()
        total_rows = len(df)
        duplicate_percentage = (duplicate_count / total_rows) * 100

        return [df_name, error_type, duplicate_count, duplicate_percentage]

    # Function to perform all data quality checks
    def run_quality_checks(self):
        results = []

        # Iterate over all DataFrames and perform checks based on their names
        for df, name in zip(self.dfs, self.names):
            if name == 'condition_df':
                condition_checks = [
                    self.validate_column(df, 'resource.onsetDateTime', self.is_valid_datetime, 'Invalid OnsetDateTime Format', name),
                    self.validate_column(df, 'resource.abatementDateTime', self.is_valid_datetime, 'Invalid AbatementDateTime Format', name),
                    self.validate_duplicates(df, ['fullUrl', 'resource.id'], 'Duplicate Check', name),
                    self.validate_logical_date(df, 'resource.onsetDateTime', 'resource.abatementDateTime', 'Logical Date Compliance', name)
                ]
                results.extend(condition_checks)

            elif name == 'encounter_df':
                encounter_checks = [
                    self.validate_column(df, 'resource.period.start', self.is_valid_datetime, 'Invalid Period Start Format', name),
                    self.validate_column(df, 'resource.period.end', self.is_valid_datetime, 'Invalid Period End Format', name),
                    self.validate_duplicates(df, ['resource.id'], 'Duplicate Resource.ID', name)
                ]
                results.extend(encounter_checks)

            elif name == 'careplan_df':
                careplan_checks = [
                    self.validate_column(df, 'resource.period.start', self.is_valid_date, 'Invalid Period Start Format', name),
                    self.validate_column(df, 'resource.period.end', self.is_valid_date, 'Invalid Period End Format', name),
                    self.validate_logical_date(df, 'resource.period.start', 'resource.period.end', 'Logical Date Compliance', name)
                ]
                results.extend(careplan_checks)

            elif name == 'patient_df':
                patient_checks = [
                    self.validate_column(df, 'resource.birthDate', self.is_valid_birthdate, 'BirthDate Errors', name),
                    self.validate_column(df, 'resource.deceasedDateTime', self.is_valid_deceased_datetime, 'DeceasedDateTime Errors', name),
                    self.validate_column(df, 'contact_number', self.is_valid_contact_number, 'Contact Number Errors', name),
                    self.validate_column(df, 'latitude', self.is_valid_latitude, 'Latitude Errors', name),
                    self.validate_column(df, 'longitude', self.is_valid_longitude, 'Longitude Errors', name),
                    self.validate_logical_date(df, 'resource.birthDate', 'resource.deceasedDateTime', 'Birthdate vs DeceasedDateTime Logical Compliance', name),
                    self.validate_duplicates(df, ['resource.id'], 'Duplicate Resource.ID', name)
                ]
                results.extend(patient_checks)

            elif name == 'observation_df':
                observation_checks = [
                    self.validate_column(df, 'resource.effectiveDateTime', self.is_valid_datetime, 'Invalid EffectiveDateTime Format', name),
                    self.validate_duplicates(df, ['resource.id'], 'Duplicate Resource.ID', name)
                ]
                results.extend(observation_checks)

            elif name == 'immunization_df':
                immunization_checks = [
                    self.validate_column(df, 'resource.date', self.is_valid_datetime, 'Invalid Resource.Date Format', name)
                ]
                results.extend(immunization_checks)

        # Convert results to DataFrame for better visualization
        summary_df = pd.DataFrame(results, columns=['DataFrame Name', 'Error Type', 'Count', 'Percentage'])

        return summary_df


# Class to clean invalid date and contact number entries
class InvalidFormatCleaner:
    def __init__(self, dfs, names):
        self.dfs = dfs
        self.names = names

    # Function to clean invalid date and contact number entries
    def clean_invalid_formats(self):
        for df, name in zip(self.dfs, self.names):
            if name in ['condition_df', 'encounter_df', 'careplan_df', 'patient_df', 'observation_df', 'immunization_df']:
                # Clean invalid datetime formats
                date_columns = ['resource.onsetDateTime', 'resource.abatementDateTime', 'resource.period.start', 'resource.period.end', 'resource.effectiveDateTime', 'resource.date', 'resource.birthDate', 'resource.deceasedDateTime']
                for column in date_columns:
                    if column in df.columns:
                        df.loc[~df[column].apply(lambda x: pd.isna(x) or DataQualityChecker.is_valid_datetime(x) or DataQualityChecker.is_valid_date(x)), column] = None

            if name == 'patient_df':
                # Clean invalid contact numbers
                if 'contact_number' in df.columns:
                    df.loc[~df['contact_number'].apply(lambda x: pd.isna(x) or DataQualityChecker.is_valid_contact_number(x)), 'contact_number'] = None

        return self.dfs


# Class to fix logical date order issues
class LogicalDateFixer:
    def __init__(self, dfs, names):
        self.dfs = dfs
        self.names = names

    # Function to swap start and end dates if start date is greater than end date
    def fix_logical_dates(self):
        for df, name in zip(self.dfs, self.names):
            if name in ['careplan_df', 'encounter_df']:
                # Fix period start and end dates
                if 'resource.period.start' in df.columns and 'resource.period.end' in df.columns:
                    mask = (df['resource.period.start'] > df['resource.period.end']) & ~df['resource.period.start'].isna() & ~df['resource.period.end'].isna()
                    df.loc[mask, ['resource.period.start', 'resource.period.end']] = df.loc[mask, ['resource.period.end', 'resource.period.start']].values

            if name == 'condition_df':
                # Fix onsetDateTime and abatementDateTime
                if 'resource.onsetDateTime' in df.columns and 'resource.abatementDateTime' in df.columns:
                    mask = (df['resource.onsetDateTime'] > df['resource.abatementDateTime']) & ~df['resource.onsetDateTime'].isna() & ~df['resource.abatementDateTime'].isna()
                    df.loc[mask, ['resource.onsetDateTime', 'resource.abatementDateTime']] = df.loc[mask, ['resource.abatementDateTime', 'resource.onsetDateTime']].values

        return self.dfs

class InvalidLatitudeLongitudeCleaner:
    def __init__(self, dfs, names):
        self.dfs = dfs
        self.names = names

    # Function to clean invalid latitude and longitude entries
    def clean_invalid_lat_lon(self):
        for df, name in zip(self.dfs, self.names):
            if name == 'patient_df':
                # Clean invalid latitude values
                if 'latitude' in df.columns:
                    df.loc[~df['latitude'].apply(lambda x: pd.isna(x) or DataQualityChecker.is_valid_latitude(x)), 'latitude'] = None

                # Clean invalid longitude values
                if 'longitude' in df.columns:
                    df.loc[~df['longitude'].apply(lambda x: pd.isna(x) or DataQualityChecker.is_valid_longitude(x)), 'longitude'] = None

        return self.dfs

class MissingValueHandler:
    def __init__(self):
        # Initialize the handler with dictionaries to track removed rows, filled values, and boolean updates
        self.removed_rows_count = {}
        self.filled_values_count = {}
        self.updated_boolean_count = {}

    # Function to clean patient DataFrame
    def clean_patient_df(self, patient_df):
        initial_patient_rows = len(patient_df)

        # Remove rows where resource.id is missing
        patient_df = patient_df.dropna(subset=['resource.id'])

        # Initialize counter for how many True values are set in multipleBirthBoolean
        true_count = 0

        # Handle multipleBirthBoolean and multipleBirthInteger logic
        for index, row in patient_df.iterrows():
            # If multipleBirthInteger has a value, set multipleBirthBoolean to True
            if pd.notna(row['resource.multipleBirthInteger']):
                if pd.isna(row['resource.multipleBirthBoolean']):
                    patient_df.at[index, 'resource.multipleBirthBoolean'] = True
                    true_count += 1  # Count the number of True assignments
            # If both are missing, do nothing
            elif pd.isna(row['resource.multipleBirthBoolean']) and pd.isna(row['resource.multipleBirthInteger']):
                continue

        # Track how many True values were set for multipleBirthBoolean
        self.updated_boolean_count['patient_df.multipleBirthBoolean'] = true_count

        # Track how many missing values were in multipleBirthInteger before filling
        missing_before_fill = patient_df['resource.multipleBirthInteger'].isna().sum()

        # Fill missing values in multipleBirthInteger with 0
        patient_df['resource.multipleBirthInteger'].fillna(0, inplace=True)

        # Track how many missing values were filled with 0
        self.filled_values_count['patient_df.multipleBirthInteger'] = missing_before_fill

        # Calculate number of rows removed from patient_df
        self.removed_rows_count['patient_df'] = initial_patient_rows - len(patient_df)

        return patient_df

    # Function to clean condition DataFrame
    def clean_condition_df(self, condition_df):
        initial_condition_rows = len(condition_df)

        # Remove rows where either resource.id or patient_id is missing
        condition_df = condition_df.dropna(subset=['resource.id', 'patient_id'])

        # Calculate number of rows removed from condition_df
        self.removed_rows_count['condition_df'] = initial_condition_rows - len(condition_df)

        return condition_df

    # Function to clean observation DataFrame
    def clean_observation_df(self, observation_df):
        initial_observation_rows = len(observation_df)

        # Remove rows where either resource.id or patient_id is missing
        observation_df = observation_df.dropna(subset=['resource.id', 'patient_id'])

        # Calculate number of rows removed from observation_df
        self.removed_rows_count['observation_df'] = initial_observation_rows - len(observation_df)

        return observation_df

    # Function to clean encounter DataFrame
    def clean_encounter_df(self, encounter_df):
        initial_encounter_rows = len(encounter_df)

        # Remove rows where either resource.id or resource.patient.reference is missing
        encounter_df = encounter_df.dropna(subset=['resource.id', 'resource.patient.reference'])

        # Calculate number of rows removed from encounter_df
        self.removed_rows_count['encounter_df'] = initial_encounter_rows - len(encounter_df)

        return encounter_df

    # Function to clean immunization DataFrame
    def clean_immunization_df(self, immunization_df):
        initial_immunization_rows = len(immunization_df)

        # Remove rows where either resource.encounter.reference or resource.patient.reference is missing
        immunization_df = immunization_df.dropna(subset=['resource.encounter.reference', 'resource.patient.reference'])

        # Calculate number of rows removed from immunization_df
        self.removed_rows_count['immunization_df'] = initial_immunization_rows - len(immunization_df)

        return immunization_df

    # Function to return how many rows were removed, missing values filled, and True values set
    def get_missing_value_stats(self):
        return {
            'removed_rows': self.removed_rows_count,
            'filled_values': self.filled_values_count,
            'updated_booleans': self.updated_boolean_count
        }


class DuplicateHandler:
    def __init__(self):
        # Initialize the handler with a dictionary to track removed duplicates
        self.duplicates_removed_count = {}

    # Deduplication logic for any DataFrame
    def deduplicate(self, df, column_name, df_name):
        initial_rows = len(df)

        # Drop duplicates based on the column
        df = df.drop_duplicates(subset=[column_name])

        # Calculate how many duplicates were removed
        duplicates_removed = initial_rows - len(df)

        # Store the count of duplicates removed for this DataFrame
        self.duplicates_removed_count[df_name] = duplicates_removed

        return df

    # Function to return how many duplicates were removed
    def get_duplicates_removed_stats(self):
        return self.duplicates_removed_count