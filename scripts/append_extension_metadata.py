#!/usr/bin/env python3
"""Append DuckDB extension metadata footer to a shared library.

This is a Python port of DuckDB's CMake metadata append script:
https://github.com/duckdb/duckdb/blob/v1.4.4/scripts/append_metadata.cmake

This script appends the required 534-byte footer to a Mojo-compiled shared
library so that DuckDB recognizes it as a valid extension.

Usage:
    python3 scripts/append_extension_metadata.py <extension_file> [options]

Options:
    --platform PLATFORM       Platform string (default: auto-detect)
    --capi-version VERSION    C API version for stable ABI (default: v1.2.0)
    --duckdb-version VERSION  DuckDB version for unstable ABI (default: auto-detect)
    --extension-version VER   Extension version string (default: "")
    --abi-type ABI            ABI type: C_STRUCT or C_STRUCT_UNSTABLE (default: C_STRUCT)

The version field in the metadata footer depends on the ABI type:
  - C_STRUCT (stable):   uses --capi-version (the C API version, e.g. v1.2.0)
  - C_STRUCT_UNSTABLE:   uses --duckdb-version (the DuckDB version, e.g. v1.4.4)

Example:
    python3 scripts/append_extension_metadata.py demo_mojo.duckdb_extension
    python3 scripts/append_extension_metadata.py ext.duckdb_extension --abi-type C_STRUCT_UNSTABLE
"""

import argparse
import platform
import shutil
import struct
import subprocess
import sys


def detect_platform() -> str:
    """Detect the DuckDB platform string for the current system."""
    machine = platform.machine().lower()
    system = platform.system().lower()

    arch_map = {
        "x86_64": "amd64",
        "amd64": "amd64",
        "aarch64": "arm64",
        "arm64": "arm64",
    }

    system_map = {
        "darwin": "osx",
        "linux": "linux",
        "windows": "windows",
    }

    arch = arch_map.get(machine, machine)
    os_name = system_map.get(system, system)

    return f"{os_name}_{arch}"


def detect_duckdb_version() -> str | None:
    """Try to detect the DuckDB version by running `duckdb --version`."""
    duckdb_path = shutil.which("duckdb")
    if duckdb_path is None:
        return None
    try:
        result = subprocess.run(
            [duckdb_path, "--version"],
            capture_output=True,
            text=True,
            timeout=5,
        )
        # Output is like "v1.4.4 abc1234567"
        version = result.stdout.strip().split()[0]
        if version.startswith("v"):
            return version
    except (subprocess.TimeoutExpired, OSError, IndexError):
        pass
    return None


def resolve_version_field(
    abi_type: str,
    capi_version: str | None,
    duckdb_version: str | None,
) -> str:
    """Resolve the version field based on ABI type.

    For C_STRUCT (stable), the version field is the C API version.
    For C_STRUCT_UNSTABLE, the version field is the DuckDB version.
    See: https://github.com/duckdb/duckdb/blob/v1.4.4/CMakeLists.txt#L975-L983
    """
    if abi_type == "C_STRUCT":
        version = capi_version or "v1.2.0"
    else:  # C_STRUCT_UNSTABLE
        version = duckdb_version
        if version is None:
            version = detect_duckdb_version()
        if version is None:
            print(
                "Error: --duckdb-version is required for C_STRUCT_UNSTABLE "
                "(could not auto-detect)",
                file=sys.stderr,
            )
            sys.exit(1)
    return version


def pad32(s: str) -> bytes:
    """Pad or truncate a string to exactly 32 bytes with null padding."""
    b = s.encode("utf-8")
    return (b + b"\x00" * 32)[:32]


def create_metadata_footer(
    platform_str: str,
    version: str,
    extension_version: str = "",
    abi_type: str = "C_STRUCT",
) -> bytes:
    """Create the 534-byte DuckDB extension metadata footer.

    The footer consists of:
    - 22 bytes: WebAssembly custom section header (compatibility layer)
    - 256 bytes: 8 metadata fields of 32 bytes each (written in reverse order)
    - 256 bytes: Signature (all zeros for unsigned extensions)

    Args:
        platform_str: Platform string (e.g. "osx_arm64").
        version: Version field. For C_STRUCT this is the C API version
            (e.g. "v1.2.0"). For C_STRUCT_UNSTABLE this is the DuckDB
            version (e.g. "v1.4.4").
        extension_version: Extension's own version string.
        abi_type: "C_STRUCT" or "C_STRUCT_UNSTABLE".
    """
    # WebAssembly custom section header (22 bytes)
    wasm_header = (
        b"\x00"  # custom section marker
        b"\x93\x04"  # LEB128 of 531 (total payload length)
        b"\x10"  # length of name (16)
        b"duckdb_signature"  # section name (16 bytes)
        b"\x80\x04"  # LEB128 of 512 (metadata + signature length)
    )

    # Build the 8 metadata fields (32 bytes each)
    meta1_magic = pad32("4")  # Magic value
    meta2_platform = pad32(platform_str)  # Platform
    meta3_version = pad32(version)  # CAPI version (stable) or DuckDB version (unstable)
    meta4_ext_version = pad32(extension_version)  # Extension version
    meta5_abi_type = pad32(abi_type)  # ABI type
    meta6_reserved = pad32("")  # Reserved
    meta7_reserved = pad32("")  # Reserved
    meta8_reserved = pad32("")  # Reserved

    # Fields are written in REVERSE order
    metadata = (
        meta8_reserved
        + meta7_reserved
        + meta6_reserved
        + meta5_abi_type
        + meta4_ext_version
        + meta3_version
        + meta2_platform
        + meta1_magic
    )

    # Unsigned signature (256 zero bytes)
    signature = b"\x00" * 256

    footer = wasm_header + metadata + signature
    assert len(footer) == 534, f"Footer should be 534 bytes, got {len(footer)}"
    return footer


def append_metadata(
    extension_path: str,
    platform_str: str | None = None,
    capi_version: str | None = None,
    duckdb_version: str | None = None,
    extension_version: str = "",
    abi_type: str = "C_STRUCT",
) -> None:
    """Append DuckDB extension metadata to a shared library file."""
    if platform_str is None:
        platform_str = detect_platform()

    version = resolve_version_field(abi_type, capi_version, duckdb_version)

    footer = create_metadata_footer(
        platform_str=platform_str,
        version=version,
        extension_version=extension_version,
        abi_type=abi_type,
    )

    with open(extension_path, "ab") as f:
        f.write(footer)

    version_label = "CAPI version" if abi_type == "C_STRUCT" else "DuckDB version"
    print(f"Appended extension metadata to {extension_path}")
    print(f"  Platform:          {platform_str}")
    print(f"  {version_label + ':':19s}{version}")
    print(f"  Extension version: {extension_version or '(none)'}")
    print(f"  ABI type:          {abi_type}")
    print(f"  Footer size:       {len(footer)} bytes")


def main():
    parser = argparse.ArgumentParser(
        description="Append DuckDB extension metadata to a shared library"
    )
    parser.add_argument("extension", help="Path to the .duckdb_extension file")
    parser.add_argument(
        "--platform",
        default=None,
        help="Platform string (default: auto-detect)",
    )
    parser.add_argument(
        "--capi-version",
        default=None,
        help="C API version for stable ABI (default: v1.2.0)",
    )
    parser.add_argument(
        "--duckdb-version",
        default=None,
        help="DuckDB version for unstable ABI (default: auto-detect via `duckdb --version`)",
    )
    parser.add_argument(
        "--extension-version",
        default="",
        help="Extension version string",
    )
    parser.add_argument(
        "--abi-type",
        default="C_STRUCT",
        choices=["C_STRUCT", "C_STRUCT_UNSTABLE"],
        help="ABI type (default: C_STRUCT)",
    )

    args = parser.parse_args()
    append_metadata(
        extension_path=args.extension,
        platform_str=args.platform,
        capi_version=args.capi_version,
        duckdb_version=args.duckdb_version,
        extension_version=args.extension_version,
        abi_type=args.abi_type,
    )


if __name__ == "__main__":
    main()
