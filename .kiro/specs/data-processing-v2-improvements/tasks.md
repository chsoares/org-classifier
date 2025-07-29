# Implementation Plan

- [x] 1. Enhance OrganizationNormalizer with improved fuzzy matching and quote stripping
  - Implement quote stripping functionality in `_clean_organization_name` method
  - Improve fuzzy matching algorithm with better similarity validation
  - Add enhanced conflict detection for organizations with similar names but different meanings
  - Write unit tests for improved normalization functionality
  - _Requirements: 1.1, 1.2, 1.3, 1.4_

- [ ] 2. Modify DataProcessor to handle multiple Excel files
  - Create `process_multiple_excel_files` method to scan data/raw directory for all .xlsx files
  - Implement `add_file_source_column` method to add File column before Type column
  - Update `merge_spreadsheets` method to handle multiple files with source tracking
  - Write unit tests for multi-file processing functionality
  - _Requirements: 2.1, 2.2, 2.3, 2.4_

- [ ] 3. Create enhanced ResultMerger for multi-file output generation
  - Implement `create_multi_file_organizations_csv` method to generate participant counts per file
  - Create logic to generate participants_{filename} columns dynamically based on input files
  - Add participants_total column calculation across all source files
  - Write unit tests for multi-file organization CSV generation
  - _Requirements: 3.1, 3.2, 3.3_

- [ ] 4. Implement simplified people.csv generation with single normalized column
  - Create `create_simplified_people_csv` method in ResultMerger
  - Remove duplicate Home organization columns and keep only normalized version
  - Add File column to people.csv for source tracking
  - Write unit tests for simplified people CSV generation
  - _Requirements: 4.1, 4.2, 4.3, 4.4_

- [ ] 5. Update Streamlit app to handle new data structure
  - Modify column references from 'Home organization_normalized' to 'Home organization'
  - Update data loading logic to handle new CSV structure
  - Add display functionality for file source information
  - Fix filtering and search functionality with updated column names
  - _Requirements: 5.1, 5.2, 5.3, 5.4_

- [ ] 6. Update main processing pipeline to use enhanced components
  - Modify main orchestrator to use new multi-file DataProcessor
  - Update pipeline to use improved OrganizationNormalizer
  - Integrate enhanced ResultMerger into processing flow
  - Ensure backward compatibility with existing single-file processing
  - _Requirements: 2.1, 2.2, 2.3, 2.4_

- [ ] 7. Create integration tests for end-to-end multi-file processing
  - Write test cases for processing multiple Excel files
  - Test complete pipeline execution with multi-file inputs
  - Validate output file structure and content accuracy
  - Test data consistency across different file combinations
  - _Requirements: 2.1, 2.2, 2.3, 2.4, 3.1, 3.2, 3.3, 4.1, 4.2, 4.3, 4.4_

- [ ] 8. Update configuration and documentation for version 2.0
  - Update configuration files to support multi-file processing
  - Modify README and documentation to reflect new capabilities
  - Update example usage and command-line interface
  - Create migration guide for users upgrading from version 1.0
  - _Requirements: 2.1, 2.2, 2.3, 2.4_