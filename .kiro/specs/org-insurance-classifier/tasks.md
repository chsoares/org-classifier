# Implementation Plan

- [x] 1. Set up project structure and basic configuration
  - Create directory structure for the project
  - Set up logging system based on existing refs/classifier/logger_config.py
  - Create basic configuration file for API keys and settings
  - _Requirements: 1.1, 1.2, 1.3, 1.4, 1.5_

- [x] 2. Implement Excel data loading and processing
  - Create function to load COP29_FLOP_On-site.xlsx file
  - Extract data from all spreadsheets except "temporary passes", "media", "parties", and "party overflow" (government entities excluded as never insurance-related)
  - Keep only "Nominated by", "Name", and "Home organization" columns
  - Add "Type" column with spreadsheet name for each row
  - For "Party overflow" type: combine "Nominated by" + "Home organization" when country name not already included
  - Merge all spreadsheets into single DataFrame
  - _Requirements: 1.1, 1.2, 1.3, 1.4, 1.5_

- [x] 3. Create organization name normalization system
  - Extract unique "Home Organization" values from merged data
  - Implement fuzzy string matching to find similar organization names
  - Create mapping from similar names to most frequent variant
  - Update both main dataset and organizations list with normalized names
  - Count occurrences of each organization in the dataset
  - _Requirements: 2.1, 2.2, 2.3, 2.4, 2.5, 2.6_

- [x] 4. Build organization tracking DataFrame
  - Create DataFrame structure to track processing status for each organization
  - Include columns for website search, scraping, and classification results
  - Add error tracking and timestamp fields
  - Implement functions to update tracking status at each processing stage
  - _Requirements: 6.1, 6.2, 6.3, 6.4, 6.5, 6.6_

- [x] 5. Implement website search functionality
  - Create web search function that tries Google first, then DuckDuckGo, then Bing
  - Parse search results to find organization's official website
  - Filter out social media and irrelevant sites
  - Record which search method was successful
  - Handle search failures and network errors gracefully
  - _Requirements: 3.1, 3.2, 3.3, 3.4, 3.5, 3.6_

- [x] 6. Adapt existing web scraping code for organization content
  - Modify refs/classifier/web_extractor.py for organization-specific content
  - Extract main page content and "About" sections
  - Clean and normalize extracted text
  - Limit content length to 2000 characters while keeping most relevant parts
  - Validate that extracted content is relevant to the organization
  - _Requirements: 4.1, 4.2, 4.3, 4.4, 4.5, 4.6_

- [x] 7. Create insurance-specific AI classification system
  - Adapt refs/classifier/sector_classifier.py for insurance classification
  - Create specific prompt for insurance industry identification
  - Implement response cleaning to ensure only "Yes" or "No" answers
  - Add retry logic for failed API calls
  - Include rate limiting to avoid API quota issues
  - _Requirements: 5.1, 5.2, 5.3, 5.4, 5.5, 5.6_

- [x] 8. Create test dataset and validation system
  - Create test dataset with 10 known organizations (3 insurance, 7 non-insurance)
  - Include organizations like "Allianz", "Swiss Re", "Lloyd's of London" for insurance
  - Include organizations like "Microsoft", "Harvard University", "Red Cross" for non-insurance
  - Run complete pipeline on test dataset
  - Validate classification accuracy against known results
  - Adjust prompts and thresholds based on test results
  - _Requirements: 5.1, 5.2, 5.3, 5.4, 5.5, 5.6_

- [x] 9. Build main processing pipeline
  - Create main function that processes each organization through all stages
  - Implement error handling so failures don't stop entire process
  - Add progress tracking and logging for each processing step
  - Update tracking DataFrame with results from each stage
  - Generate processing statistics and error reports
  - _Requirements: 8.1, 8.2, 8.3, 8.4, 8.5, 8.6_

- [ ] 10. Implement simple result caching system
  - Save successful processing results to avoid reprocessing
  - Check for existing results before starting processing
  - Allow manual cache clearing when needed
  - Add timestamps to track when results were generated
  - _Requirements: 9.1, 9.2, 9.3, 9.4, 9.5_

- [ ] 11. Create result merging functionality
  - Add "is_insurance" column to main participant dataset
  - Match classification results using normalized organization names
  - Handle cases where organizations were not successfully classified
  - Validate that all rows have been processed correctly
  - Export final dataset in CSV and Excel formats
  - _Requirements: 10.1, 10.2, 10.3, 10.4, 10.5_

- [ ] 12. Build Streamlit web interface
  - Create main dashboard showing processing statistics
  - Add filters to view organizations by classification status
  - Display detailed processing information for each organization
  - Show website content that was analyzed for classification
  - Include error details and retry options for failed classifications
  - Add export functionality for results and processing data
  - _Requirements: 7.1, 7.2, 7.3, 7.4, 7.5, 7.6_

- [ ] 13. Add comprehensive logging and error reporting
  - Implement detailed logging using existing logger_config.py pattern
  - Log each processing stage with appropriate detail level
  - Create error summary reports for troubleshooting
  - Add progress indicators for long-running processes
  - Include timing information for performance monitoring
  - _Requirements: 6.5, 8.5_

- [ ] 14. Create batch processing and resume functionality
  - Process organizations in manageable batches
  - Save progress after each batch to allow resumption
  - Skip already processed organizations when resuming
  - Handle interruptions gracefully without losing work
  - Add command-line options for batch size and resume
  - _Requirements: 8.4, 8.6_

- [ ] 15. Implement data validation and quality checks
  - Validate Excel file structure before processing
  - Check for required columns and data types
  - Verify API connectivity before starting classification
  - Validate final results for completeness and consistency
  - Generate data quality report with statistics
  - _Requirements: 1.5, 10.4_

- [ ] 16. Add configuration management and documentation
  - Create configuration file for all settings and parameters
  - Add environment variable support for API keys
  - Write user documentation for running the system
  - Create troubleshooting guide for common issues
  - Add example usage and sample outputs
  - _Requirements: All requirements for system usability_