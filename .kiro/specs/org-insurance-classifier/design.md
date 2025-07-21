# Design Document

## Overview

The Organization Insurance Classifier is a Python-based system that processes COP29 participant data to identify insurance-related organizations through automated web scraping and AI classification. The system follows a pipeline architecture with distinct stages for data processing, web scraping, classification, and result presentation.

The design emphasizes robustness, observability, and error recovery, with comprehensive logging and tracking at each stage. The system is built to handle large datasets efficiently while providing detailed feedback on processing status and failure points.

## Architecture

### High-Level Architecture

```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   Excel Data    │───▶│  Data Processor  │───▶│ Organizations   │
│  (COP29 File)   │    │   (Merge & Clean)│    │   DataFrame     │
└─────────────────┘    └──────────────────┘    └─────────────────┘
                                                         │
                                                         ▼
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│  Streamlit UI   │◀───│   Result Merger  │◀───│ Classification  │
│   (Results)     │    │                  │    │    Pipeline     │
└─────────────────┘    └──────────────────┘    └─────────────────┘
                                                         │
                                                         ▼
                       ┌──────────────────┐    ┌─────────────────┐
                       │     Cache        │◀───│  Web Scraper    │
                       │   Management     │    │   & Classifier  │
                       └──────────────────┘    └─────────────────┘
```

### Component Architecture

```
src/
├── core/
│   ├── data_processor.py      # Excel loading and data cleaning
│   ├── org_normalizer.py      # Fuzzy matching and normalization
│   └── result_merger.py       # Merge results back to main dataset
├── scraping/
│   ├── web_searcher.py        # Google/DuckDuckGo/Bing search
│   ├── content_extractor.py   # Website content extraction
│   └── content_validator.py   # Content quality validation
├── classification/
│   ├── insurance_classifier.py # AI-based insurance classification
│   └── prompt_manager.py      # Classification prompts
├── pipeline/
│   ├── org_processor.py       # Main processing pipeline
│   ├── cache_manager.py       # Result caching and persistence
│   └── progress_tracker.py    # Processing status tracking
├── utils/
│   ├── logger_config.py       # Logging configuration
│   ├── config_manager.py      # Application configuration
│   └── error_handler.py       # Error handling utilities
└── ui/
    └── streamlit_app.py       # Web interface
```

## Components and Interfaces

### 1. Data Processor (`core/data_processor.py`)

**Purpose:** Load and merge Excel data from multiple spreadsheets.

**Key Methods:**
```python
class DataProcessor:
    def load_excel_data(self, file_path: str) -> Dict[str, pd.DataFrame]
    def merge_spreadsheets(self, sheets: Dict[str, pd.DataFrame]) -> pd.DataFrame
    def extract_relevant_columns(self, df: pd.DataFrame, sheet_name: str) -> pd.DataFrame
    def validate_data_quality(self, df: pd.DataFrame) -> bool
```

**Input:** COP29_FLOP_On-site.xlsx file
**Output:** Unified DataFrame with columns: Type, Nominated by, Name, Home Organization

### 2. Organization Normalizer (`core/org_normalizer.py`)

**Purpose:** Clean and normalize organization names using fuzzy matching.

**Key Methods:**
```python
class OrganizationNormalizer:
    def extract_unique_organizations(self, df: pd.DataFrame) -> pd.DataFrame
    def find_similar_organizations(self, org_list: List[str]) -> Dict[str, str]
    def normalize_organization_names(self, df: pd.DataFrame, mapping: Dict[str, str]) -> pd.DataFrame
    def update_main_dataset(self, main_df: pd.DataFrame, mapping: Dict[str, str]) -> pd.DataFrame
```

**Dependencies:** `fuzzywuzzy` or `rapidfuzz` for string matching
**Input:** DataFrame with organization names
**Output:** Normalized organization mapping and updated DataFrames

### 3. Web Searcher (`scraping/web_searcher.py`)

**Purpose:** Find organization websites through multiple search engines.

**Key Methods:**
```python
class WebSearcher:
    def search_organization_website(self, org_name: str) -> Tuple[Optional[str], str]
    def search_google(self, query: str) -> Optional[str]
    def search_duckduckgo(self, query: str) -> Optional[str]
    def search_bing(self, query: str) -> Optional[str]
    def validate_website_url(self, url: str) -> bool
```

**Search Strategy:**
1. Google Search (primary)
2. DuckDuckGo (fallback)
3. Bing (final fallback)

**Output:** (website_url, search_method) or (None, "failed")

### 4. Content Extractor (`scraping/content_extractor.py`)

**Purpose:** Extract relevant content from organization websites.

**Key Methods:**
```python
class ContentExtractor:
    def extract_content(self, url: str) -> Optional[str]
    def find_about_sections(self, soup: BeautifulSoup) -> List[str]
    def extract_main_content(self, soup: BeautifulSoup) -> str
    def clean_and_normalize_text(self, text: str) -> str
    def prioritize_relevant_content(self, content: str, max_length: int) -> str
```

**Based on:** Existing `refs/classifier/web_extractor.py` with enhancements
**Features:**
- Multi-language "About" section detection
- Content quality validation
- Text cleaning and normalization
- Intelligent content prioritization

### 5. Insurance Classifier (`classification/insurance_classifier.py`)

**Purpose:** Classify organizations as insurance-related using AI.

**Key Methods:**
```python
class InsuranceClassifier:
    def classify_organization(self, content: str, org_name: str) -> str
    def get_insurance_prompt(self, content: str, org_name: str) -> str
    def clean_ai_response(self, response: str) -> str
    def validate_classification_result(self, result: str) -> bool
```

**Based on:** Existing `refs/classifier/sector_classifier.py` adapted for insurance
**AI Integration:** OpenRouter API with Gemini 2.0 Flash
**Output:** "Yes" or "No" classification result

### 6. Organization Processor (`pipeline/org_processor.py`)

**Purpose:** Main processing pipeline orchestrating all components.

**Key Methods:**
```python
class OrganizationProcessor:
    def process_organization(self, org_name: str) -> Dict[str, Any]
    def process_batch(self, org_list: List[str]) -> pd.DataFrame
    def handle_processing_error(self, org_name: str, error: Exception) -> Dict[str, Any]
    def generate_processing_report(self) -> Dict[str, Any]
```

**Processing Flow:**
1. Check cache for existing results
2. Search for organization website
3. Extract website content
4. Validate content quality
5. Classify using AI
6. Update tracking DataFrame
7. Cache successful results

### 7. Cache Manager (`pipeline/cache_manager.py`)

**Purpose:** Manage result caching and persistence.

**Key Methods:**
```python
class CacheManager:
    def get_cached_result(self, org_name: str) -> Optional[Dict[str, Any]]
    def cache_result(self, org_name: str, result: Dict[str, Any]) -> None
    def is_cache_valid(self, org_name: str, max_age_days: int = 30) -> bool
    def clear_cache(self) -> None
    def export_cache_stats(self) -> Dict[str, Any]
```

**Storage:** JSON files in `data/cache/` directory
**Features:**
- Timestamp-based cache validation
- Automatic cache cleanup
- Cache statistics and reporting

### 8. Progress Tracker (`pipeline/progress_tracker.py`)

**Purpose:** Track processing status and generate reports.

**Key Methods:**
```python
class ProgressTracker:
    def create_tracking_dataframe(self, org_list: List[str]) -> pd.DataFrame
    def update_organization_status(self, org_name: str, status_data: Dict[str, Any]) -> None
    def get_processing_statistics(self) -> Dict[str, Any]
    def export_tracking_data(self, file_path: str) -> None
```

**Tracking Fields:**
- Organization identification and normalization
- Website search results
- Content extraction status
- Classification results
- Error messages and timestamps
- Processing duration metrics

## Data Models

### Main Dataset Schema
```python
main_df = pd.DataFrame({
    'Type': str,                    # Originating spreadsheet name
    'Nominated by': str,            # Nominating entity
    'Name': str,                    # Participant name
    'Home Organization': str,       # Original organization name
    'Home Organization_Normalized': str,  # Normalized organization name
    'is_insurance': Optional[bool]  # Final classification result
})
```

### Organizations Tracking Schema
```python
organizations_df = pd.DataFrame({
    'home_organization': str,           # Original organization name
    'normalized_name': str,             # Normalized name (post fuzzy matching)
    'occurrence_count': int,            # Frequency in main dataset
    
    # Website Search Stage
    'website_found': bool,              # Website discovery success
    'website_url': Optional[str],       # Discovered website URL
    'website_search_method': str,       # 'google', 'duckduckgo', 'bing', 'failed'
    
    # Content Extraction Stage
    'scraping_success': bool,           # Content extraction success
    'content_length': int,              # Length of extracted content
    'scraping_error': Optional[str],    # Error message if failed
    
    # Classification Stage
    'classification_success': bool,     # AI classification success
    'classification_result': Optional[str],  # 'Yes', 'No', or None
    'classification_error': Optional[str],   # Error message if failed
    
    # Final Results
    'is_insurance': Optional[bool],     # Final boolean result
    'process_status': str,              # 'completed', 'website_not_found', 'scraping_failed', 'classification_failed'
    'processing_duration': float,       # Total processing time in seconds
    'last_updated': datetime           # Timestamp for cache management
})
```

### Cache Data Schema
```python
cache_entry = {
    'org_name': str,
    'result': {
        'website_url': Optional[str],
        'content': Optional[str],
        'classification': Optional[str],
        'is_insurance': Optional[bool],
        'process_status': str
    },
    'timestamp': datetime,
    'processing_duration': float
}
```

## Error Handling

### Error Categories and Responses

**1. Data Loading Errors**
- Missing Excel file → Log error, exit gracefully
- Corrupted spreadsheet → Skip sheet, continue with others
- Missing required columns → Log warning, use available data

**2. Network and Scraping Errors**
- Connection timeout → Retry with exponential backoff
- HTTP errors (4xx, 5xx) → Log error, mark as failed
- Content parsing errors → Try alternative extraction methods

**3. AI Classification Errors**
- API rate limits → Implement backoff and retry
- Invalid API response → Clean response and retry
- API service unavailable → Queue for later processing

**4. System Errors**
- Memory issues → Process in smaller batches
- Disk space issues → Clean cache and temporary files
- Permission errors → Log detailed error information

### Error Recovery Strategies

```python
class ErrorHandler:
    def handle_network_error(self, error: Exception, retry_count: int) -> bool
    def handle_api_error(self, error: Exception, org_name: str) -> Dict[str, Any]
    def handle_system_error(self, error: Exception) -> None
    def create_error_report(self, errors: List[Exception]) -> str
```

**Recovery Mechanisms:**
- Exponential backoff for network errors
- Alternative search engines for failed searches
- Content validation and re-extraction
- Graceful degradation with partial results

## Testing Strategy

### Unit Testing
- **Data Processing:** Test Excel loading, column extraction, data validation
- **Normalization:** Test fuzzy matching algorithms with known similar names
- **Web Scraping:** Mock HTTP responses, test content extraction logic
- **Classification:** Test AI prompt generation and response cleaning
- **Caching:** Test cache storage, retrieval, and validation

### Integration Testing
- **End-to-End Pipeline:** Process sample organizations through complete workflow
- **Error Scenarios:** Test error handling and recovery mechanisms
- **Performance:** Test processing speed with various dataset sizes
- **Cache Behavior:** Test cache hits, misses, and invalidation

### Test Data
- Sample Excel file with known organization types
- Mock web responses for consistent testing
- Known insurance and non-insurance organizations for validation

### Performance Testing
- **Throughput:** Measure organizations processed per minute
- **Memory Usage:** Monitor memory consumption during large batch processing
- **API Limits:** Test rate limiting and backoff mechanisms
- **Cache Efficiency:** Measure cache hit rates and performance impact

## Configuration Management

### Application Configuration (`config.yaml`)
```yaml
# Data Processing
data:
  excel_file: "COP29_FLOP_On-site.xlsx"
  excluded_sheets: ["temporary passes", "media"]
  required_columns: ["Nominated by", "Name", "Home Organization"]

# Web Scraping
scraping:
  timeout: 10
  max_retries: 3
  retry_delay: 2
  max_content_length: 2000
  user_agent: "Mozilla/5.0 (compatible; OrgClassifier/1.0)"

# AI Classification
classification:
  api_provider: "openrouter"
  model: "google/gemini-2.0-flash-001"
  max_tokens: 100
  temperature: 0.1

# Caching
cache:
  enabled: true
  max_age_days: 30
  cache_directory: "data/cache"

# Logging
logging:
  level: "INFO"
  log_to_file: true
  log_directory: "logs"
  max_log_files: 10

# Processing
processing:
  batch_size: 50
  parallel_workers: 4
  enable_progress_bar: true
```

### Environment Variables
```bash
OPENROUTER_API_KEY=your_api_key_here
OPENROUTER_BASE_URL=https://openrouter.ai/api/v1
LOG_LEVEL=INFO
CACHE_ENABLED=true
```

This design provides a robust, scalable, and maintainable solution for the organization classification problem, with comprehensive error handling, caching, and monitoring capabilities.