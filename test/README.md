# immich_tools Test Framework

Comprehensive test framework for validating immich_tools functionality.

## Overview

This test framework:
1. **Generates test data** using `setup_test_data.py`
2. **Populates database** by running `media_analyzer.py`
3. **Runs test scenarios** with `media_query.py` commands
4. **Captures outputs** (stdout and generated files)
5. **Compares results** with ground truth (golden) files
6. **Reports differences** with colored diffs

## Quick Start

### First Time Setup (Generate Ground Truth)

```bash
# Generate ground truth files for all tests
python test/run_tests.py --generate-ground-truth

# This creates etalon files in test/ground_truth/
```

### Running Tests

```bash
# Run all tests
python test/run_tests.py

# Example output:
# [09:52:57] SUCCESS: Test basic_stats PASSED
# [09:52:57] ERROR: Test corrupted_files FAILED
# 
# === DIFF for corrupted_files STDOUT ===
# --- expected
# +++ actual
# @@ -1,1 +1,1 @@
# -📊 Media file statistics
# +📊 MODIFIED Media file statistics
# === END DIFF ===
```

### Test Specific Scenarios

```bash
# Run specific test
python test/run_tests.py --test-name basic_stats

# Run with verbose output
python test/run_tests.py --verbose --test-name raw_files
```

## Usage

### Generate Ground Truth Files (First Time Setup)

```bash
# Generate ground truth files for all tests
python test/run_tests.py --generate-ground-truth

# Generate ground truth for specific test pattern
python test/run_tests.py --generate-ground-truth --test-name basic_stats
```

### Run Tests

```bash
# Run all tests
python test/run_tests.py

# Run specific test
python test/run_tests.py --test-name corrupted_files

# Run with verbose output
python test/run_tests.py --verbose

# Keep test data after tests (for debugging)
python test/run_tests.py --keep-test-data
```

## Test Scenarios

The framework includes the following test scenarios:

### Database Query Tests
- **basic_stats**: Basic statistics query (`--stats`)
- **corrupted_files**: List corrupted files (`--corrupted`)
- **large_files**: Find large files (`--size-filter 1KB+`)
- **images_only**: Filter images only (`--media-type image`)
- **videos_only**: Filter videos only (`--media-type video`)

### Grouping Tests
- **video_codecs**: Group by video codec (`--group-by codec`)
- **resolution_groups**: Group by resolution (`--group-by resolution`)

### Export Tests
- **export_dirs_text**: Export directory structure as text
- **export_dirs_json**: Export directory structure as JSON
- **export_dirs_console**: Export directory structure to console with colors

### Filter Tests
- **date_range_2023**: Files from 2023 (`--date-range 2023-01-01 2023-12-31`)
- **pattern_search**: Search files by pattern (`--pattern IMG_001`)

## Directory Structure

```
test/
├── run_tests.py           # Main test framework
├── setup_test_data.py     # Test data generator
├── ground_truth/          # Ground truth (golden) files
│   ├── basic_stats_stdout.txt
│   ├── basic_stats_metadata.json
│   └── ...
├── results/               # Test run results (temporary)
└── test_data/             # Generated test data (temporary)
    ├── photos/
    ├── videos/
    └── test_analysis.db
```

## Ground Truth Files

For each test scenario, the framework generates:
- `{test_name}_stdout.txt`: Captured stdout
- `{test_name}_stderr.txt`: Captured stderr (if any)
- `{test_name}_{output_file}`: Generated output files (e.g., JSON exports)
- `{test_name}_metadata.json`: Test metadata (command, timestamp, etc.)

## Adding New Tests

To add a new test scenario, edit `run_tests.py` and add to the `get_test_scenarios()` method:

```python
'new_test_name': {
    'description': 'Description of the test',
    'cmd': [sys.executable, 'media_query.py', db_rel, '--your-args'],
    'output_files': ['optional_output_file.txt']
}
```

## Test Data

The framework uses `setup_test_data.py` to generate comprehensive test data including:
- RAW photos (DNG format) with EXIF metadata
- JPEG photos with and without metadata
- Modern videos (H264/MP4) and legacy videos (AVI/MJPEG)
- Corrupted media files
- System files and mixed content
- Subdirectories for testing file organization tools

## Continuous Integration

This framework is designed to be used in CI/CD pipelines:

```bash
# In CI pipeline
python test/run_tests.py --verbose
```

## Troubleshooting

### Test Failures

When tests fail, the framework shows colored diffs:
- **Red lines**: Lines removed from expected output
- **Green lines**: Lines added to actual output
- **Blue lines**: File headers
- **Cyan lines**: Diff context markers

### Regenerating Ground Truth

If legitimate changes are made to the tools, regenerate ground truth:

```bash
python test/run_tests.py --generate-ground-truth
```

### Debugging Tests

Use verbose mode and keep test data for investigation:

```bash
python test/run_tests.py --verbose --keep-test-data --test-name failing_test
```

## Dependencies

The test framework requires:
- Python 3.7+
- colorama (for colored output)
- All immich_tools dependencies

## Performance

- Test data generation: ~5-10 seconds
- Media analysis: ~10-30 seconds  
- Test scenarios: ~30-60 seconds
- Total runtime: ~1-2 minutes

The framework is optimized for speed while maintaining comprehensive coverage.