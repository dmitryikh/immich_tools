#!/usr/bin/env python3
"""
Comprehensive test framework for immich_tools

This framework:
1. Generates test data using setup_test_data.py
2. Runs media_analyzer.py to populate database
3. Runs various media_query.py scenarios and captures outputs
4. Compares outputs with ground truth (golden) files
5. Reports differences if any

Usage:
    python test/run_tests.py                    # Run all tests
    python test/run_tests.py --generate-ground-truth  # Generate new ground truth files
    python test/run_tests.py --test-name basic_stats  # Run specific test
    python test/run_tests.py --verbose          # Verbose output
    python test/run_tests.py --keep-test-data   # Don't cleanup test data after tests
"""

import os
import sys
import subprocess
import argparse
import shutil
import json
import difflib
from pathlib import Path
from datetime import datetime
from colorama import Fore, Style, init

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

# Initialize colorama
init(autoreset=True)

class TestFramework:
    """Main test framework class"""
    
    def __init__(self, generate_ground_truth=False, verbose=False, keep_test_data=False):
        self.generate_ground_truth = generate_ground_truth
        self.verbose = verbose
        self.keep_test_data = keep_test_data
        
        # Paths
        self.test_dir = Path(__file__).parent
        self.root_dir = self.test_dir.parent
        self.test_data_dir = self.test_dir / "test_data"
        self.ground_truth_dir = self.test_dir / "ground_truth"
        self.results_dir = self.test_dir / "results"
        self.database_path = self.test_data_dir / "test_analysis.db"
        
        # Test results
        self.passed_tests = []
        self.failed_tests = []
        
        # Ensure directories exist
        self.ground_truth_dir.mkdir(exist_ok=True)
        self.results_dir.mkdir(exist_ok=True)
    
    def log(self, message, level="INFO"):
        """Log message with timestamp and level"""
        timestamp = datetime.now().strftime("%H:%M:%S")
        if level == "INFO":
            color = Fore.BLUE
        elif level == "SUCCESS":
            color = Fore.GREEN
        elif level == "WARNING":
            color = Fore.YELLOW
        elif level == "ERROR":
            color = Fore.RED
        else:
            color = ""
        
        print(f"{color}[{timestamp}] {level}: {message}{Style.RESET_ALL}")
    
    def setup_test_data(self):
        """Generate test data and populate database"""
        self.log("Setting up test data...")
        
        # Run setup_test_data.py
        cmd = [
            sys.executable, str(self.test_dir / "setup_test_data.py"),
            "--output-dir", str(self.test_data_dir),
            "--cleanup"
        ]
        
        try:
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=120)
            if result.returncode != 0:
                self.log(f"Failed to generate test data: {result.stderr}", "ERROR")
                return False
            
            if self.verbose:
                self.log("Test data generation output:", "INFO")
                print(result.stdout)
                
        except subprocess.TimeoutExpired:
            self.log("Test data generation timed out", "ERROR")
            return False
        except Exception as e:
            self.log(f"Error generating test data: {e}", "ERROR")
            return False
        
        # Run media_analyzer.py to populate database
        self.log("Analyzing media files...")
        
        cmd = [
            sys.executable, str(self.root_dir / "media_analyzer.py"),
            str(self.test_data_dir),
            "--database", str(self.database_path),
            "--force",
            "--workers", "2"  # Use fewer workers for testing
        ]
        
        try:
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=300)
            if result.returncode != 0:
                self.log(f"Failed to analyze media files: {result.stderr}", "ERROR")
                return False
            
            if self.verbose:
                self.log("Media analysis output:", "INFO")
                print(result.stdout)
                
        except subprocess.TimeoutExpired:
            self.log("Media analysis timed out", "ERROR")
            return False
        except Exception as e:
            self.log(f"Error analyzing media files: {e}", "ERROR")
            return False
        
        self.log("Test data setup complete", "SUCCESS")
        return True
    
    def run_command(self, cmd, test_name):
        """Run a command and capture stdout/stderr and any output files"""
        try:
            # Change to root directory for command execution
            result = subprocess.run(
                cmd, 
                capture_output=True, 
                text=True, 
                timeout=60,
                cwd=self.root_dir
            )
            
            return {
                'stdout': result.stdout,
                'stderr': result.stderr,
                'returncode': result.returncode,
                'cmd': ' '.join(cmd)
            }
            
        except subprocess.TimeoutExpired:
            self.log(f"Command timed out: {' '.join(cmd)}", "ERROR")
            return None
        except Exception as e:
            self.log(f"Error running command: {e}", "ERROR")
            return None
    
    def get_test_scenarios(self):
        """Define all test scenarios"""
        test_data_rel = os.path.relpath(self.test_data_dir, self.root_dir)
        db_rel = os.path.relpath(self.database_path, self.root_dir)
        
        scenarios = {
            'basic_stats': {
                'description': 'Basic statistics query',
                'cmd': [sys.executable, 'media_analyzer.py', test_data_rel, '--database', db_rel, '--stats'],
                'output_files': []
            },
            
            'corrupted_files': {
                'description': 'Export files without metadata',
                'cmd': [sys.executable, 'media_query.py', '--database', db_rel, '--export-list', 'no_metadata_files.txt', '--export-no-metadata'],
                'output_files': ['no_metadata_files.txt']
            },
            
            'raw_files': {
                'description': 'Export RAW files',
                'cmd': [sys.executable, 'media_query.py', '--database', db_rel, '--export-list', 'raw_files.txt', '--export-raw'],
                'output_files': ['raw_files.txt']
            },
            
            'old_videos': {
                'description': 'Export old video formats',
                'cmd': [sys.executable, 'media_query.py', '--database', db_rel, '--export-list', 'old_videos.txt', '--export-old-video'],
                'output_files': ['old_videos.txt']
            },
            
            'duplicate_files': {
                'description': 'Export duplicate files',
                'cmd': [sys.executable, 'media_query.py', '--database', db_rel, '--export-list', 'duplicates.txt', '--export-duplicates'],
                'output_files': ['duplicates.txt']
            },
            
            'high_bitrate_videos': {
                'description': 'Export high bitrate videos (>1 Mbps)',
                'cmd': [sys.executable, 'media_query.py', '--database', db_rel, '--export-list', 'high_bitrate.txt', '--min-bitrate', '1'],
                'output_files': ['high_bitrate.txt']
            },
            
            'suffix_files': {
                'description': 'Export files with _720p suffix',
                'cmd': [sys.executable, 'media_query.py', '--database', db_rel, '--export-list', 'suffix_files.txt', '--suffix', '_720p'],
                'output_files': ['suffix_files.txt']
            },
            
            'export_dirs_text': {
                'description': 'Export directory structure as text',
                'cmd': [sys.executable, 'media_query.py', '--database', db_rel, '--export-dirs', 'dirs_structure.txt'],
                'output_files': ['dirs_structure.txt']
            },
            
            'export_dirs_console': {
                'description': 'Export directory structure to console with colors',
                'cmd': [sys.executable, 'media_query.py', '--database', db_rel, '--export-dirs', '--console'],
                'output_files': []
            },
            
            'pattern_search_camera': {
                'description': 'Search files by camera pattern',
                'cmd': [sys.executable, 'media_query.py', '--database', db_rel, '--export-list', 'camera_files.txt', '--export-pattern', 'camera'],
                'output_files': ['camera_files.txt']
            },
            
            'short_format_raw': {
                'description': 'Export RAW files in short format',
                'cmd': [sys.executable, 'media_query.py', '--database', db_rel, '--export-list', 'raw_short.txt', '--export-raw', '--short'],
                'output_files': ['raw_short.txt']
            },
            
            'min_dir_size': {
                'description': 'Export directories with minimum size (1MB)',
                'cmd': [sys.executable, 'media_query.py', '--database', db_rel, '--export-dirs', 'large_dirs.txt'],
                'output_files': ['large_dirs.txt']
            }
        }
        
        return scenarios
    
    def save_result(self, test_name, result, output_files_content):
        """Save test result to ground truth or results directory"""
        if self.generate_ground_truth:
            output_dir = self.ground_truth_dir
        else:
            output_dir = self.results_dir
        
        # Save stdout
        stdout_file = output_dir / f"{test_name}_stdout.txt"
        with open(stdout_file, 'w', encoding='utf-8') as f:
            f.write(result['stdout'])
        
        # Save stderr if not empty
        if result['stderr'].strip():
            stderr_file = output_dir / f"{test_name}_stderr.txt"
            with open(stderr_file, 'w', encoding='utf-8') as f:
                f.write(result['stderr'])
        
        # Save output files
        for filename, content in output_files_content.items():
            output_file = output_dir / f"{test_name}_{filename}"
            if isinstance(content, str):
                with open(output_file, 'w', encoding='utf-8') as f:
                    f.write(content)
            else:
                with open(output_file, 'wb') as f:
                    f.write(content)
        
        # Save metadata
        metadata = {
            'test_name': test_name,
            'timestamp': datetime.now().isoformat(),
            'cmd': result['cmd'],
            'returncode': result['returncode'],
            'output_files': list(output_files_content.keys())
        }
        
        metadata_file = output_dir / f"{test_name}_metadata.json"
        with open(metadata_file, 'w', encoding='utf-8') as f:
            json.dump(metadata, f, indent=2)
    
    def compare_results(self, test_name):
        """Compare test results with ground truth"""
        ground_truth_stdout = self.ground_truth_dir / f"{test_name}_stdout.txt"
        result_stdout = self.results_dir / f"{test_name}_stdout.txt"
        
        if not ground_truth_stdout.exists():
            self.log(f"No ground truth found for {test_name}", "WARNING")
            return False
        
        if not result_stdout.exists():
            self.log(f"No result found for {test_name}", "ERROR")
            return False
        
        # Compare stdout
        with open(ground_truth_stdout, 'r', encoding='utf-8') as f:
            ground_truth_content = f.read()
        
        with open(result_stdout, 'r', encoding='utf-8') as f:
            result_content = f.read()
        
        if ground_truth_content != result_content:
            self.log(f"STDOUT differs for {test_name}", "ERROR")
            self.show_diff(ground_truth_content, result_content, f"{test_name} STDOUT")
            return False
        
        # Compare output files
        metadata_file = self.ground_truth_dir / f"{test_name}_metadata.json"
        if metadata_file.exists():
            with open(metadata_file, 'r', encoding='utf-8') as f:
                metadata = json.load(f)
            
            for output_file in metadata.get('output_files', []):
                ground_truth_file = self.ground_truth_dir / f"{test_name}_{output_file}"
                result_file = self.results_dir / f"{test_name}_{output_file}"
                
                if ground_truth_file.exists() and result_file.exists():
                    with open(ground_truth_file, 'r', encoding='utf-8') as f:
                        gt_content = f.read()
                    with open(result_file, 'r', encoding='utf-8') as f:
                        res_content = f.read()
                    
                    if gt_content != res_content:
                        self.log(f"Output file {output_file} differs for {test_name}", "ERROR")
                        self.show_diff(gt_content, res_content, f"{test_name} {output_file}")
                        return False
        
        return True
    
    def show_diff(self, expected, actual, context=""):
        """Show colored diff between expected and actual content"""
        print(f"\n{Fore.RED}=== DIFF for {context} ==={Style.RESET_ALL}")
        
        expected_lines = expected.splitlines(keepends=True)
        actual_lines = actual.splitlines(keepends=True)
        
        diff = difflib.unified_diff(
            expected_lines, 
            actual_lines, 
            fromfile="expected", 
            tofile="actual", 
            lineterm=""
        )
        
        for line in diff:
            if line.startswith('+++') or line.startswith('---'):
                print(f"{Fore.BLUE}{line}{Style.RESET_ALL}", end='')
            elif line.startswith('+'):
                print(f"{Fore.GREEN}{line}{Style.RESET_ALL}", end='')
            elif line.startswith('-'):
                print(f"{Fore.RED}{line}{Style.RESET_ALL}", end='')
            elif line.startswith('@@'):
                print(f"{Fore.CYAN}{line}{Style.RESET_ALL}", end='')
            else:
                print(line, end='')
        
        print(f"{Fore.RED}=== END DIFF ==={Style.RESET_ALL}\n")
    
    def run_test(self, test_name, scenario):
        """Run a single test scenario"""
        self.log(f"Running test: {test_name} - {scenario['description']}")
        
        # Run the command
        result = self.run_command(scenario['cmd'], test_name)
        if result is None:
            self.log(f"Failed to run test {test_name}", "ERROR")
            return False
        
        # Collect output files
        output_files_content = {}
        for output_file in scenario['output_files']:
            file_path = self.root_dir / output_file
            if file_path.exists():
                try:
                    with open(file_path, 'r', encoding='utf-8') as f:
                        output_files_content[output_file] = f.read()
                    # Clean up the file
                    file_path.unlink()
                except Exception as e:
                    self.log(f"Error reading output file {output_file}: {e}", "WARNING")
        
        # Save results
        self.save_result(test_name, result, output_files_content)
        
        # Compare with ground truth (unless generating ground truth)
        if not self.generate_ground_truth:
            if self.compare_results(test_name):
                self.log(f"Test {test_name} PASSED", "SUCCESS")
                self.passed_tests.append(test_name)
                return True
            else:
                self.log(f"Test {test_name} FAILED", "ERROR")
                self.failed_tests.append(test_name)
                return False
        else:
            self.log(f"Ground truth saved for {test_name}", "SUCCESS")
            return True
    
    def run_all_tests(self, test_filter=None):
        """Run all test scenarios"""
        scenarios = self.get_test_scenarios()
        
        if test_filter:
            scenarios = {k: v for k, v in scenarios.items() if test_filter in k}
        
        if not scenarios:
            self.log("No tests to run", "WARNING")
            return False
        
        self.log(f"Running {len(scenarios)} test scenarios...")
        
        for test_name, scenario in scenarios.items():
            try:
                self.run_test(test_name, scenario)
            except Exception as e:
                self.log(f"Exception in test {test_name}: {e}", "ERROR")
                self.failed_tests.append(test_name)
        
        return True
    
    def cleanup(self):
        """Clean up test data"""
        if not self.keep_test_data and self.test_data_dir.exists():
            self.log("Cleaning up test data...")
            shutil.rmtree(self.test_data_dir)
    
    def print_summary(self):
        """Print test results summary"""
        total_tests = len(self.passed_tests) + len(self.failed_tests)
        
        print(f"\n{Fore.CYAN}=== TEST SUMMARY ==={Style.RESET_ALL}")
        print(f"Total tests: {total_tests}")
        print(f"{Fore.GREEN}Passed: {len(self.passed_tests)}{Style.RESET_ALL}")
        print(f"{Fore.RED}Failed: {len(self.failed_tests)}{Style.RESET_ALL}")
        
        if self.passed_tests:
            print(f"\n{Fore.GREEN}Passed tests:{Style.RESET_ALL}")
            for test in self.passed_tests:
                print(f"  ✓ {test}")
        
        if self.failed_tests:
            print(f"\n{Fore.RED}Failed tests:{Style.RESET_ALL}")
            for test in self.failed_tests:
                print(f"  ✗ {test}")
        
        if self.generate_ground_truth:
            print(f"\n{Fore.YELLOW}Ground truth files generated in: {self.ground_truth_dir}{Style.RESET_ALL}")
        
        return len(self.failed_tests) == 0


def main():
    parser = argparse.ArgumentParser(
        description='Comprehensive test framework for immich_tools',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__
    )
    
    parser.add_argument(
        '--generate-ground-truth',
        action='store_true',
        help='Generate ground truth files instead of running tests'
    )
    
    parser.add_argument(
        '--test-name',
        help='Run only tests matching this name pattern'
    )
    
    parser.add_argument(
        '--verbose', '-v',
        action='store_true',
        help='Verbose output'
    )
    
    parser.add_argument(
        '--keep-test-data',
        action='store_true',
        help='Keep test data after tests complete'
    )
    
    args = parser.parse_args()
    
    # Create test framework
    framework = TestFramework(
        generate_ground_truth=args.generate_ground_truth,
        verbose=args.verbose,
        keep_test_data=args.keep_test_data
    )
    
    try:
        # Setup test data
        if not framework.setup_test_data():
            framework.log("Failed to setup test data", "ERROR")
            return 1
        
        # Run tests
        if not framework.run_all_tests(args.test_name):
            framework.log("Failed to run tests", "ERROR")
            return 1
        
        # Print summary
        success = framework.print_summary()
        
        return 0 if success else 1
        
    except KeyboardInterrupt:
        framework.log("Tests interrupted by user", "WARNING")
        return 1
    except Exception as e:
        framework.log(f"Unexpected error: {e}", "ERROR")
        return 1
    finally:
        framework.cleanup()


if __name__ == "__main__":
    sys.exit(main())