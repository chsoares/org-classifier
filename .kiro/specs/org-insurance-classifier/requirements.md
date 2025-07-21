# Requirements Document

## Introduction

This feature implements an automated system to classify organizations from COP29 participant data to identify those directly related to the insurance industry. The system processes Excel data containing participant information across multiple spreadsheets, cleans and normalizes organization names, and uses web scraping combined with AI classification to determine insurance industry relationships.

The solution addresses the challenge of manually reviewing thousands of organization entries to identify insurance-related entities, providing automated classification with detailed tracking and reporting capabilities.

## Requirements

### Requirement 1

**User Story:** As a data analyst, I want to load and merge participant data from multiple Excel spreadsheets, so that I can work with a unified dataset containing all participant information.

#### Acceptance Criteria

1. WHEN the system loads the COP29_FLOP_On-site.xlsx file THEN it SHALL read all spreadsheets except "temporary passes" and "media"
2. WHEN processing each spreadsheet THEN the system SHALL extract only the "Nominated by", "Name", and "Home Organization" columns
3. WHEN merging spreadsheets THEN the system SHALL add a "Type" column containing the originating spreadsheet name
4. WHEN the merge is complete THEN the system SHALL produce a single DataFrame with all participant data
5. IF any spreadsheet fails to load THEN the system SHALL log the error and continue processing remaining spreadsheets

### Requirement 2

**User Story:** As a data analyst, I want to clean and normalize organization names, so that similar organizations are grouped together and processed efficiently.

#### Acceptance Criteria

1. WHEN the system identifies unique "Home Organization" values THEN it SHALL create a separate organizations DataFrame
2. WHEN processing organization names THEN the system SHALL use fuzzy matching to identify similar entries
3. WHEN similar organizations are found THEN the system SHALL rename them to the most frequently occurring variant
4. WHEN normalization is complete THEN the system SHALL update both the organizations DataFrame and the main dataset with normalized organization names
5. WHEN counting occurrences THEN the system SHALL track how many times each organization appears in the dataset
6. WHEN updating the main dataset THEN the system SHALL replace all instances of original names with their normalized variants

### Requirement 3

**User Story:** As a data analyst, I want to automatically find organization websites through web search, so that I can gather information for classification without manual research.

#### Acceptance Criteria

1. WHEN searching for an organization website THEN the system SHALL search using Google as primary search engine
2. WHEN Google search fails THEN the system SHALL fallback to DuckDuckGo search
3. WHEN DuckDuckGo fails THEN the system SHALL fallback to Bing search
4. WHEN a valid website is found THEN the system SHALL record the URL and search method used
5. WHEN no website is found THEN the system SHALL mark the organization as "website_not_found"
6. WHEN search encounters errors THEN the system SHALL implement exponential backoff retry logic

### Requirement 4

**User Story:** As a data analyst, I want to extract relevant content from organization websites, so that I have sufficient information for accurate classification.

#### Acceptance Criteria

1. WHEN accessing an organization website THEN the system SHALL extract text content from the main page
2. WHEN the main page is processed THEN the system SHALL search for and extract "About" or similar sections
3. WHEN content is extracted THEN the system SHALL clean and normalize the text removing irrelevant elements
4. WHEN content length exceeds 2000 characters THEN the system SHALL prioritize most relevant sections
5. WHEN scraping fails THEN the system SHALL record the specific error and mark as "scraping_failed"
6. WHEN content is too short or irrelevant THEN the system SHALL validate content quality before proceeding

### Requirement 5

**User Story:** As a data analyst, I want to classify organizations as insurance-related or not using AI analysis, so that I can identify relevant entities automatically.

#### Acceptance Criteria

1. WHEN organization content is available THEN the system SHALL send it to AI classifier with insurance-specific prompt
2. WHEN AI processes the content THEN it SHALL return only "Yes" or "No" for insurance classification
3. WHEN classification is successful THEN the system SHALL record the result and mark as "completed"
4. WHEN classification fails THEN the system SHALL record the error and mark as "classification_failed"
5. WHEN AI response is ambiguous THEN the system SHALL implement response cleaning and validation
6. WHEN processing multiple organizations THEN the system SHALL implement rate limiting to avoid API limits

### Requirement 6

**User Story:** As a data analyst, I want detailed tracking of the classification process, so that I can monitor progress and identify failure points for debugging.

#### Acceptance Criteria

1. WHEN processing each organization THEN the system SHALL create a tracking record with all process stages
2. WHEN each stage completes THEN the system SHALL update the corresponding status fields
3. WHEN errors occur THEN the system SHALL record specific error messages and timestamps
4. WHEN processing is complete THEN the system SHALL generate a comprehensive progress report
5. WHEN the process runs THEN the system SHALL log detailed information to both console and file
6. WHEN tracking data is available THEN the system SHALL allow export of processing statistics

### Requirement 7

**User Story:** As a data analyst, I want to view classification results through an interactive web interface, so that I can explore and analyze the insurance organization data effectively.

#### Acceptance Criteria

1. WHEN the Streamlit app loads THEN it SHALL display summary statistics of the classification results
2. WHEN viewing results THEN the user SHALL be able to filter organizations by classification status
3. WHEN exploring data THEN the user SHALL see detailed processing information for each organization
4. WHEN reviewing classifications THEN the user SHALL be able to see the website content that was analyzed
5. WHEN analysis is complete THEN the user SHALL be able to export results in CSV format
6. WHEN errors occurred THEN the user SHALL be able to view error details and retry failed classifications

### Requirement 8

**User Story:** As a data analyst, I want the system to handle errors gracefully and provide recovery options, so that processing can continue even when individual organizations fail.

#### Acceptance Criteria

1. WHEN any processing stage fails THEN the system SHALL continue with remaining organizations
2. WHEN network errors occur THEN the system SHALL implement retry logic with exponential backoff
3. WHEN API limits are reached THEN the system SHALL pause and resume processing automatically
4. WHEN the process is interrupted THEN the system SHALL save progress and allow resumption
5. WHEN errors are encountered THEN the system SHALL provide detailed logging for troubleshooting
6. WHEN processing resumes THEN the system SHALL skip already completed organizations

### Requirement 9

**User Story:** As a data analyst, I want to cache processing results, so that I can avoid reprocessing organizations and improve system efficiency.

#### Acceptance Criteria

1. WHEN an organization is successfully processed THEN the system SHALL cache the results
2. WHEN processing the same organization again THEN the system SHALL use cached results if available
3. WHEN cache entries exist THEN the system SHALL check timestamps and refresh stale data
4. WHEN cache is used THEN the system SHALL log that cached results were retrieved
5. WHEN cache becomes corrupted THEN the system SHALL detect and rebuild cache automatically

### Requirement 10

**User Story:** As a data analyst, I want to merge classification results back to the original dataset, so that I can have the final insurance classification for all participants.

#### Acceptance Criteria

1. WHEN classification is complete THEN the system SHALL add an "is_insurance" column to the main dataset
2. WHEN merging results THEN the system SHALL match organizations using normalized names
3. WHEN an organization was not classified THEN the system SHALL mark it as None in the is_insurance column
4. WHEN merging is complete THEN the system SHALL validate that all rows have been processed
5. WHEN final dataset is ready THEN the system SHALL save it in multiple formats (CSV, Excel)