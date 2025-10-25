#!/usr/bin/env python3
"""
Script to verify that all UDF implementations are registered in mount_udfs function.

This script:
1. Finds all structs that implement ScalarUDFImpl or AggregateUDFImpl
2. Checks if they are registered in the mount_udfs function
3. Reports any missing registrations
"""

import re
import sys
from pathlib import Path

# Path to the lib.rs file
LIB_RS_PATH = Path("rust/geodatafusion/src/lib.rs")

# Regex patterns to find UDF implementations
SCALAR_UDF_PATTERN = r'impl\s+ScalarUDFImpl\s+for\s+(\w+)'
AGGREGATE_UDF_PATTERN = r'impl\s+AggregateUDFImpl\s+for\s+(\w+)'

# Regex pattern to find registrations in mount_udfs
REGISTRATION_PATTERN = r'register_udf\(crate::udf::[^)]+::(\w+)::default\(\)\.into\(\)\)'
REGISTRATION_UDAF_PATTERN = r'register_udaf\(crate::udf::[^)]+::(\w+)::default\(\)\.into\(\)\)'


def find_udf_implementations():
    """Find all structs that implement ScalarUDFImpl or AggregateUDFImpl"""
    udf_dir = Path("rust/geodatafusion/src/udf")
    udf_implementations = set()

    for rust_file in udf_dir.rglob("*.rs"):
        content = rust_file.read_text()

        # Find scalar UDF implementations
        for match in re.finditer(SCALAR_UDF_PATTERN, content):
            struct_name = match.group(1)
            # Skip commented out implementations
            if not is_commented_out(content, match.start()):
                # Skip UDFs marked with #[allow(dead_code)] on their struct definition
                # or UDFs that are only pub(super) and not publicly exported
                if (not has_allow_dead_code(content, struct_name) and
                    not is_pub_super_only(content, struct_name)):
                    udf_implementations.add(struct_name)

        # Find aggregate UDF implementations
        for match in re.finditer(AGGREGATE_UDF_PATTERN, content):
            struct_name = match.group(1)
            # Skip commented out implementations
            if not is_commented_out(content, match.start()):
                # Skip UDFs marked with #[allow(dead_code)] on their struct definition
                # or UDFs that are only pub(super) and not publicly exported
                if (not has_allow_dead_code(content, struct_name) and
                    not is_pub_super_only(content, struct_name)):
                    udf_implementations.add(struct_name)

    return udf_implementations


def has_allow_dead_code(content, struct_name):
    """Check if a struct has #[allow(dead_code)] annotation"""
    # Pattern to find struct definitions with #[allow(dead_code)]
    # This matches:
    # #[allow(dead_code)]
    # struct StructName
    # or
    # #[allow(dead_code)] struct StructName
    pattern = rf'#\[allow\(dead_code\)\][^\n]*\bstruct\s+{struct_name}\b'

    # Also check for multi-line patterns where the attribute is on a separate line
    # This handles cases like:
    # #[allow(dead_code)]
    # pub(super) struct StructName
    multi_line_pattern = rf'#\[allow\(dead_code\)\][^\n]*\n[^\n]*\bstruct\s+{struct_name}\b'

    return bool(re.search(pattern, content)) or bool(re.search(multi_line_pattern, content))


def is_pub_super_only(content, struct_name):
    """Check if a struct is only pub(super) and not publicly exported"""
    # Pattern to find struct definitions that are only pub(super)
    # This matches:
    # pub(super) struct StructName
    # but not:
    # pub struct StructName
    # or
    # pub(crate) struct StructName
    pub_super_pattern = rf'pub\(super\)\s+struct\s+{struct_name}\b'
    pub_pattern = rf'pub\s+struct\s+{struct_name}\b'
    pub_crate_pattern = rf'pub\(crate\)\s+struct\s+{struct_name}\b'

    # Return True if it's pub(super) but not pub or pub(crate)
    return (bool(re.search(pub_super_pattern, content)) and
            not bool(re.search(pub_pattern, content)) and
            not bool(re.search(pub_crate_pattern, content)))


def is_commented_out(content, position):
    """Check if the implementation at the given position is commented out"""
    # Look backwards from the position to find the start of the line
    line_start = content.rfind('\n', 0, position) + 1
    line_content = content[line_start:position]

    # Check if the line starts with // or is inside a block comment
    if line_content.strip().startswith('//'):
        return True

    # Check for block comments - this is a simplified check
    # Count the number of /* before and after the position
    block_comments_before = content.count('/*', 0, position)
    block_comments_after = content.count('*/', 0, position)

    # If there are unmatched /* before the position, it's inside a block comment
    return block_comments_before > block_comments_after


def find_registered_udfs():
    """Find all UDFs registered in mount_udfs function"""
    if not LIB_RS_PATH.exists():
        print(f"Error: {LIB_RS_PATH} not found")
        sys.exit(1)

    content = LIB_RS_PATH.read_text()
    registered_udfs = set()

    # Find scalar UDF registrations
    for match in re.finditer(REGISTRATION_PATTERN, content):
        registered_udfs.add(match.group(1))

    # Find aggregate UDF registrations
    for match in re.finditer(REGISTRATION_UDAF_PATTERN, content):
        registered_udfs.add(match.group(1))

    return registered_udfs


def main():
    print("Checking UDF registration in mount function...")

    # Find all UDF implementations
    udf_implementations = find_udf_implementations()
    print(f"Found {len(udf_implementations)} UDF implementations:")
    for udf in sorted(udf_implementations):
        print(f"  - {udf}")

    # Find registered UDFs
    registered_udfs = find_registered_udfs()
    print(f"\nFound {len(registered_udfs)} registered UDFs:")
    for udf in sorted(registered_udfs):
        print(f"  - {udf}")

    # Find missing registrations
    missing_udfs = udf_implementations - registered_udfs

    if missing_udfs:
        print(f"\n❌ ERROR: Found {len(missing_udfs)} UDF implementations not registered in mount:")
        for udf in sorted(missing_udfs):
            print(f"  - {udf}")
        print("\nPlease add the missing registrations to the mount function in lib.rs")
        sys.exit(1)
    else:
        print("\n✅ SUCCESS: All UDF implementations are properly registered in mount!")
        sys.exit(0)


if __name__ == "__main__":
    main()
