import sys
sys.path.append('.')
from extraction.snowflake_extractor import SnowflakeExtractor, extract_single_table

def test_connection():
    """Test Snowflake connection"""
    print("Testing connection...")
    try:
        with SnowflakeExtractor() as extractor:
            print("Connection successful!")
            return True
    except Exception as e:
        print(f"Connection failed: {e}")
        return False

def test_table_info():
    print("Testing table info...")
    try:
        with SnowflakeExtractor() as extractor:
            info = extractor.get_table_info('REGION')
            print(f"REGION: {info['row_count']} rows, {len(info['columns'])} columns")
            return True
    except Exception as e:
        print(f"Table info failed: {e}")
        return False

def test_small_extraction():
    print("Testing small table extraction...")
    try:
        df = extract_single_table('REGION')
        print(f"Extracted REGION: {len(df)} rows")
        print(f"Columns: {df.columns[:5]}...")  # Show first 5 columns
        return True
    except Exception as e:
        print(f"Extraction failed: {e}")
        return False

def test_medium_extraction():
    print("Testing medium table extraction...")
    try:
        df = extract_single_table('SUPPLIER')
        print(f"Extracted SUPPLIER: {len(df)} rows")
        return True
    except Exception as e:
        print(f"Medium extraction failed: {e}")
        return False

def main():
    print("Snowflake Extractor Tests")
    print("=" * 40)
    
    tests = [
        test_connection,
        test_table_info,
        test_small_extraction,
        test_medium_extraction
    ]
    
    passed = 0
    for test in tests:
        if test():
            passed += 1
        print()
    
    print(f"Results: {passed}/{len(tests)} tests passed")
    
    if passed == len(tests):
        print("All tests passed! Ready for full extraction.")
    else:
        print("Some tests failed. Check configuration.")

if __name__ == "__main__":
    main()