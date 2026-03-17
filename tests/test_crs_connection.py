#!/usr/bin/env python3
"""
Test script for CRS board connection.

This script attempts to connect to a CRS board and verify that the connection
infrastructure is working properly.

Usage:
    python test_crs_connection.py --serial 0016
    python test_crs_connection.py --serial 0016 --firmware-path /path/to/firmware
"""

import argparse
import sys
import logging

def test_imports():
    """Test that all required modules can be imported."""
    print("=" * 60)
    print("Testing imports...")
    print("=" * 60)
    
    try:
        from corriscope import pocket_correlator
        print("✓ corriscope.pocket_correlator imported successfully")
    except ImportError as e:
        print(f"✗ Failed to import corriscope.pocket_correlator: {e}")
        return False
    
    try:
        from corriscope import fpga_array
        print("✓ corriscope.fpga_array imported successfully")
    except ImportError as e:
        print(f"✗ Failed to import corriscope.fpga_array: {e}")
        return False
    
    try:
        from corriscope import network_utils
        print("✓ corriscope.network_utils imported successfully")
    except ImportError as e:
        print(f"✗ Failed to import corriscope.network_utils: {e}")
        return False
    
    try:
        from corriscope import mdns_discovery
        print("✓ corriscope.mdns_discovery imported successfully")
    except ImportError as e:
        print(f"✗ Failed to import corriscope.mdns_discovery: {e}")
        return False
    
    try:
        from corriscope.hardware.crs import CRS
        print("✓ corriscope.hardware.crs.CRS imported successfully")
    except ImportError as e:
        print(f"✗ Failed to import corriscope.hardware.crs.CRS: {e}")
        return False
    
    print("\nAll imports successful!\n")
    return True


def test_network_settings():
    """Test network configuration for UDP data reception."""
    print("=" * 60)
    print("Testing network settings...")
    print("=" * 60)
    
    try:
        from corriscope.network_utils import verify_network_settings, show_network_status
        
        # Show current network status
        show_network_status()
        print()
        
        # Verify settings
        all_ok, errors, fix_commands = verify_network_settings()
        
        if all_ok:
            print("✓ Network settings are properly configured for UDP data reception")
            return True
        else:
            print("✗ Network configuration issues detected:")
            for error in errors:
                print(f"  - {error}")
            print("\nTo fix these issues, run:")
            for cmd in fix_commands:
                print(f"  {cmd}")
            return False
            
    except Exception as e:
        print(f"✗ Error checking network settings: {e}")
        return False


def test_program_crs(serial_number, mode='corr8', firmware_path=None):
    """Test CRS board programming using FPGAArray."""
    print("=" * 60)
    print(f"Testing CRS board programming (SN {serial_number})...")
    print("=" * 60)
    
    try:
        from corriscope.fpga_array import FPGAArray
        
        hwm = f'crs {serial_number}'
        print(f"Creating FPGAArray with hwm='{hwm}', mode='{mode}'")
        
        if firmware_path:
            print(f"Using firmware path: {firmware_path}")
        else:
            print("No firmware path specified - will use default locations")
        
        # Build FPGAArray arguments
        fpga_args = {
            'hwm': hwm,
            'mode': mode,
            'prog': 2,  # Force programming
            'stderr_log_level': 'debug',  # Enable debug logging
        }
        
        if firmware_path:
            fpga_args['bitfile'] = firmware_path
        
        print("\nAttempting to initialize and program the FPGA...")
        
        # Create and initialize the FPGAArray
        ca = FPGAArray(**fpga_args)
        
        # If we get here, initialization was successful
        print(f"\n✓ Successfully created FPGAArray with {len(ca.ib)} board(s)")
        
        if ca.ib:
            for ib in ca.ib:
                print(f"  - Board: {ib}")
                print(f"    Serial: {ib.serial}")
                print(f"    Hostname: {ib.hostname}")
                if hasattr(ib, 'fpga') and ib.fpga:
                    print(f"    FPGA: Programmed and connected")
        
        return True
        
    except Exception as e:
        print(f"\n✗ Error during CRS board programming: {e}")
        import traceback
        traceback.print_exc()
        return False


def main():
    parser = argparse.ArgumentParser(
        description='Test CRS board connection infrastructure',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python test_crs_connection.py --serial 0016
  python test_crs_connection.py --serial 0016 --firmware-path /path/to/firmware
  python test_crs_connection.py --serial 0016 --skip-network-check
        """
    )
    
    parser.add_argument('--serial', type=str, required=True,
                        help='Serial number of CRS board (e.g., 0016)')
    parser.add_argument('--firmware-path', type=str,
                        help='Path to firmware directory (optional)')
    parser.add_argument('--skip-network-check', action='store_true',
                        help='Skip network settings verification')
    parser.add_argument('--log-level', type=str, default='INFO',
                        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
                        help='Set logging level (default: INFO)')
    
    args = parser.parse_args()
    
    # Configure logging
    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%H:%M:%S'
    )
    
    print("\n")
    print("╔" + "═" * 58 + "╗")
    print("║" + " " * 58 + "║")
    print("║" + "  CRS Board Connection Test".center(58) + "║")
    print("║" + " " * 58 + "║")
    print("╚" + "═" * 58 + "╝")
    print("\n")
    
    results = {}
    
    # Test 1: Imports
    results['imports'] = test_imports()
    
    # Test 2: Network settings (optional)
    if not args.skip_network_check:
        results['network'] = test_network_settings()
    else:
        print("Skipping network settings check (--skip-network-check)\n")
        results['network'] = None
    
    # Test 3: CRS board programming
    results['programming'] = test_program_crs(
        args.serial,
        mode='corr8',
        firmware_path=args.firmware_path
    )
    
    # Summary
    print("\n")
    print("=" * 60)
    print("TEST SUMMARY")
    print("=" * 60)
    
    for test_name, result in results.items():
        if result is None:
            status = "SKIPPED"
            symbol = "○"
        elif result:
            status = "PASSED"
            symbol = "✓"
        else:
            status = "FAILED"
            symbol = "✗"
        
        print(f"{symbol} {test_name.upper()}: {status}")
    
    # Overall result
    print("=" * 60)
    
    # Count only non-skipped tests
    tested_results = {k: v for k, v in results.items() if v is not None}
    
    if tested_results and all(tested_results.values()):
        print("✓ ALL TESTS PASSED")
        print("\nThe CRS connection infrastructure appears to be working correctly.")
        print(f"CRS board SN{args.serial} was successfully programmed and connected.")
        return 0
    elif not tested_results:
        print("○ ALL TESTS SKIPPED")
        return 0
    else:
        print("✗ SOME TESTS FAILED")
        print("\nPlease review the errors above and fix any issues.")
        return 1


if __name__ == '__main__':
    sys.exit(main())
