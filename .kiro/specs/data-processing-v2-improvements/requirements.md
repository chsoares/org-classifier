# Requirements Document

## Introduction

This feature implements version 2.0 improvements for the data processing system, focusing on better home organization value cleaning, multi-file input processing, and improved data structure organization. The improvements address current limitations in fuzzy matching normalization and prepare the system to handle multiple input files with proper output organization.

## Requirements

### Requirement 1

**User Story:** As a data analyst, I want improved home organization value cleaning with better fuzzy matching, so that similar organization names are properly unified and duplicate entries are minimized.

#### Acceptance Criteria

1. WHEN processing home organization values THEN the system SHALL strip all double quotes from organization names
2. WHEN applying fuzzy matching normalization THEN the system SHALL catch and unify organization names with typos and small variations that are currently missed
3. WHEN generating organizations.csv THEN the system SHALL produce alphabetically sorted unique normalized values with minimal duplicates
4. WHEN comparing similar organization names THEN the system SHALL use improved fuzzy matching algorithms to identify variations like "World Bank Group" vs "The World Bank Group"

### Requirement 2

**User Story:** As a data processor, I want the system to handle multiple Excel input files, so that I can process data from different events (COP29, COP28, etc.) in a single workflow.

#### Acceptance Criteria

1. WHEN the system starts processing THEN it SHALL ingest all .xlsx files found in data/raw directory
2. WHEN merging multiple files THEN the system SHALL create a merged_data.csv file containing all input data
3. WHEN processing multiple files THEN the system SHALL add a "File" column before the "Type" column containing the original filename without extension
4. WHEN creating merged_data.csv THEN the system SHALL preserve all original data while adding file source tracking

### Requirement 3

**User Story:** As a data analyst, I want the organizations.csv output to track participant counts per source file, so that I can analyze participation patterns across different events.

#### Acceptance Criteria

1. WHEN creating organizations.csv THEN the system SHALL replace the single participant_count column with multiple columns
2. WHEN processing n input files THEN the system SHALL create n+1 participant count columns: participants_{filename-1}, ..., participants_{filename-n}, participants_total
3. WHEN calculating participant counts THEN the system SHALL accurately count participants from each source file separately
4. WHEN generating the total count THEN the system SHALL sum all individual file counts for participants_total

### Requirement 4

**User Story:** As a data analyst, I want the people.csv file to have a single normalized Home Organization column, so that the data structure is cleaner and more consistent.

#### Acceptance Criteria

1. WHEN generating people.csv THEN the system SHALL include only one "Home Organization" column with normalized values
2. WHEN processing people data THEN the system SHALL add a "File" column to track source file origin
3. WHEN normalizing organization names THEN the system SHALL apply the improved fuzzy matching from Requirement 1
4. WHEN creating people.csv THEN the system SHALL NOT include duplicate home organization columns

### Requirement 5

**User Story:** As a user of the Streamlit interface, I want the application to work correctly with the new column structure, so that I can visualize and analyze the processed data without errors.

#### Acceptance Criteria

1. WHEN the Streamlit app loads THEN it SHALL handle the new "Home Organization" column name correctly
2. WHEN displaying organization data THEN the app SHALL show the normalized values without referencing old column names
3. WHEN filtering or searching THEN the app SHALL work with the updated data structure
4. WHEN the app encounters the new multi-file structure THEN it SHALL display file source information appropriately