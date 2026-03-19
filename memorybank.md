# March 19, 2026

## Session Overview
**CODE CLEANUP PHASES 1-4:** Removing unused ICE/ZCU111 platform code from `fpga_array.py` while preserving CRS functionality and generic backplane support. (Note that "phases 1-4" only refers to stages of work performed today)

## Completed Work

### Phases 1-3: Function Removal ✓ (Morning Session)

**Removed ~575 lines (21% of original 2800 lines):**

**Phase 1 - ICE shuffle512/256 algorithms** (~200 lines):
- `shuffle512_cb3_freq_remap()` - ICE-specific RFI bin clustering
- `shuffle512_cb3_lane_remap()` - ICE-specific lane remapping
- `shuffle512_cb3_remap()` - ICE-specific combined remapping

**Phase 2 - ICE backplane link detection** (~120 lines):
- `detect_backplane_links()` - ICE multi-board link testing

**Phase 3 - ICE utility functions & diagnostics** (~255 lines):
- `set_test_pattern()` - ICE test pattern generation
- `set_noise_injection()` - ICE noise injection control
- `get_user_output_state_async()` - ICE user output status
- `get_ber()`, `get_ber_vs_power()`, `plot_ber_vs_power()` - BER testing (~145 lines)
- `print_fmc_power()` - FMC power monitoring (~13 lines)
- `get_eye_matrix()`, `plot_eye_matrix()` - Eye diagram testing (~87 lines)
- `print_crossbar2_frame_info()` - Crossbar 2 frame debugging (~90 lines)

**Phase 1-3 State:**
- **Before:** ~2800 lines
- **After:** 2225 lines
- **Reduction:** 575 lines (21%)

### Phase 4: ICE Mode Removal ✓ (Afternoon Session)

**Analysis Process:**
1. ✓ Searched all CRS test notebooks - confirmed **NO usage** of FPGAArray diagnostic functions
2. ✓ Verified CRS uses direct firmware access (`ib.CT.BPLINKS.gty`, `ib.CT.CROSSBAR`)
3. ✓ Confirmed `ice_corr16_tests.ipynb` is ICE-specific (not CRS)
4. ✓ Verified diagnostic functions only called internally by ICE shuffle modes being removed
5. ✓ User confirmed `set_corr_reset(0)` needed for CRS shuffle8 mode

**Removed ICE Modes from `set_operational_mode()` (~36 lines):**
- **`chan8` mode** (~18 lines) - ICE 8-channel streaming with crossbar initialization
- **ICE shuffle modes** (~18 lines):
  - `shuffle256` - 16-board single-crate shuffle with CROSSBAR2/3
  - `shuffle512` - 32-board dual-crate shuffle with backplane links
  - `shuffle16` - Single-board shuffle
  - `shuffle128` - 8-board crate shuffle
  - `chord16` - Chord mode
  - All with `init_corner_turn()`, `reset_stats()` calls
- **`corr16`** - Removed from correlator modes list

**Current State:**
- **Before Phase 4:** 2225 lines
- **After Phase 4:** 4927 lines  
- **Total reduction:** ~611 lines (22% from original ~2800)

### What Was Preserved (User Decision)

Some things were kept because I was uncertain about whether some of these things are needed for CRS platform. I'll come back to this in a future session.

**Generic Backplane Infrastructure** - Kept for future CRS multi-board support:
- `get_backplane_pcb_link_map()` - Generic PCB link mapping
- `get_backplane_qsfp_links()` - Generic QSFP link discovery
- `get_backplane_qsfp_link_map()` - Generic QSFP link mapping
- `get_gpu_link_map()` - GPU link mapping
- `get_link_map()` - Combined link mapping
- `init_corner_turn()` - Used by ICE shuffle modes (kept for now, may remove later)
- `get_shuffle_output()`, `get_chan_identity_map()`, `get_frequency_map()` - Used by init_corner_turn
- `set_tx_power()` - GTX power control (used by init_corner_turn)

**Generic Bin Mapping** - Used by CRS corr modes:
- `get_corner_turn_bin_map()` - Called by CRS corr4/8/32/64 modes
- `compute_cb1_bin_map()`, `compute_cb2_bin_map()`, `compute_cb3_bin_map()` - Generic algorithms

**CRS Operational Modes:**
- `shuffle8` - CRS shuffle mode
- `corr4`, `corr8`, `corr32`, `corr64` - CRS correlator modes

### Remaining Work

**Still to Remove** (~1000+ lines for target 50-57% reduction):

**ICE-Specific Diagnostic Functions (NOT used by CRS):**
- `reset_crossbar_stats()`, `reset_bp_shuffle_stats()` - Stats reset (only called by removed ICE shuffle modes)
- `get_corner_turn_engine_status_async()` - Corner-turn status (~130 lines)
- `_print_shuffle_status()`, `print_shuffle_status()` - Shuffle status printing (~80 lines)
- `print_rx_err_map()` - RX error mapping (~70 lines)
- `print_net_length_map()` - Network length mapping (~15 lines)
- `print_frame_info()` - Frame debugging (~25 lines)
- `plot_crate_temperatures()` - Temperature plotting (~20 lines)
- `_update_arm_firmware()` - ARM firmware update (~7 lines)

**ICE Parameter Handling in `init()`:**
- `iceboards`, `icecrates`, `mezzanines`, `exclude_iceboards` parameter processing
- Mezzanine discovery logic
- ICE-specific hardware map processing (MGK7MB/MGK7BP16 references)

**Documentation Updates:**
- Remove ICE/ZCU111 mode descriptions from docstrings
- Remove MGK7MB/MGK7BP16 examples
- Update to CRS-only examples

### Next Session Priorities
1. Remove remaining ICE diagnostic functions (~350 lines)
2. Simplify init() to remove ICE parameter handling
3. Update documentation to CRS-only
4. Test with test_crs_connection.py
5. Verify POCKET_CORRELATOR still works
6. Target: 50-57% total reduction (1400-1600 lines removed)

---

# March 17, 2026

## Session Overview
**SUCCESS:** CRS board connection fully operational. Fixed critical firmware loading and async execution issues.

## Completed Work

### Critical Fixes ✓

1. **Firmware Path Handling** (`fpga_bitstream.py`)
   - Fixed absolute path support in `FPGABitstream.get_bitstream()`
   - Paths like `/home/lab/Codes/bitstreams` now work correctly

2. **Firmware Class Registration** (`fpga_firmware/__init__.py`)
   - Added `from .chfpga import chFPGA` import
   - Firmware discovery now finds CRS modes (corr4, corr8, corr32, corr64)

3. **Async/Await Execution** (`fpga_array.py`, `ccoll.py`)
   - Made `set_operational_mode()` async with proper await
   - Fixed event loop detection in `Ccoll.__call__()`
   - Prevents "event loop already running" errors

4. **Test Logging** (`test_crs_connection.py`)
   - Added `--log-level` argument for debug output
   - Shows detailed connection and firmware loading progress

### Test Results ✓

**CRS Board SN0110 Successfully Connected:**
- ✓ All imports passing
- ✓ Network settings configured
- ✓ Firmware loaded from external path
- ✓ FPGA programmed and initialized
- ✓ Communication established

### Usage

```bash
# Basic connection
python tests/test_crs_connection.py --serial 0110 --firmware-path /path/to/firmware

# With debug logging
python tests/test_crs_connection.py --serial 0110 --firmware-path /path/to/firmware --log-level DEBUG
```

### Files Modified

- `corriscope/fpga_firmware/fpga_bitstream.py`
- `corriscope/fpga_firmware/__init__.py`
- `corriscope/fpga_firmware/chfpga/f_engine/chan.py`
- `corriscope/fpga_array.py`
- `corriscope/common/ccoll.py`
- `tests/test_crs_connection.py`

---

# February 19, 2026

## Session Overview
Establishing CRS board connection and cleaning up legacy hardware support. Focus on making `corriscope` CRS-only and implementing external firmware path configuration.

## Current Objectives

### Primary Goal: Establish CRS Board Connection
After the import migration from `pychfpga` to `corriscope`, we need to verify that CRS board connection functionality is intact and working properly.

### Secondary Goals:
1. **Remove non-CRS hardware support** - Clean up legacy code for ICE boards, ZCU111, and other unsupported platforms
2. **External firmware configuration** - Move FPGA bitstreams out of Git LFS and implement configurable firmware paths
3. **Verify supporting modules** - Ensure network_utils, mdns_discovery, and CRS hardware classes are functional

## CRS Board Connection Architecture

### Connection Flow
The CRS board connection in `corriscope` follows this sequence:

1. **POCKET_CORRELATOR initialization**
   - Accepts `hwm` parameter (e.g., `'crs 0016'` for CRS board serial number 0016)
   - Extracts serial number from hwm string
   - Calls `find_best_interface()` from `network_utils.py` to discover board
   - Verifies network settings with `verify_network_settings()`
   - Ensures UDP buffer sizes and MTU are configured for high-rate data

2. **FPGAArray initialization**
   - Inherits from FPGAArray base class
   - Passes `hwm` and `mode='corr8'` to parent
   - FPGAArray processes hardware map string
   - Discovers boards via mDNS if serial number provided
   - Opens platform connection (ARM processor communication)
   - Initializes FPGA firmware and creates firmware objects

3. **Hardware Discovery**
   - Uses `mdns_discovery.py` for network-based board discovery
   - Resolves serial numbers to IP addresses
   - Validates board presence with ping/tuber requests
   - Auto-discovers mezzanines and hardware configuration

### Key Components

**POCKET_CORRELATOR** (`pocket_correlator.py`)
- Wrapper class for simplified CRS board operation
- Provides high-level methods for data capture
- Implements gain computation and calibration
- Handles correlator configuration and data streaming

**FPGAArray** (`fpga_array.py`)
- Base class for hardware array management
- Handles async initialization and discovery
- Manages multiple boards and crates
- Provides hardware abstraction layer

**Supporting Modules:**
- `network_utils.py` - Network interface selection and UDP buffer configuration
- `mdns_discovery.py` - mDNS-based hardware discovery
- `hardware/crs/` - CRS-specific hardware implementation
- `fpga_firmware/` - FPGA bitstream loading and firmware interface

## Completed Work (February 19, 2026 Session)

### Phase 1: Verification and Cleanup
1. ✓ Reviewed CRS connection implementation in `pocket_correlator.py` and `fpga_array.py`
2. ✓ Verified `network_utils.py` - Contains all required functions for network configuration
3. ✓ Verified `mdns_discovery.py` - Functional mDNS discovery implementation
4. ✓ Confirmed CRS hardware class exists in `hardware/crs/`
5. ✓ Removed non-CRS hardware imports from `fpga_array.py`:
   - Removed `from corriscope.hardware.ice import IceBoard, IceCrate`
   - Removed `from corriscope.hardware.zcu111 import ZCU111`
   - Removed `from corriscope.hardware.zuboard import ZUBoard`
   - Removed `from corriscope.hardware.Agilent_N5764A import AgilentN5764A`
6. ✓ Removed `corriscope/hardware/mezzanine.py` file
7. ✓ Removed mezzanine import from `corriscope/hardware/__init__.py`
8. ✓ Disabled PSArray function (now raises NotImplementedError)
9. ⏳ Remove support for PROBER and other ICE-specific modules

### Phase 2: Firmware Path Configuration (Partial) ✓
1. ✓ Added `--firmware-path` argument to argparse in `fpga_array.py`
2. ✓ Updated help text for `--mode` to reflect CRS-specific modes (corr8, corr32, corr64)
3. ⏳ **Still needed:** Implement firmware path resolution in CRS hardware class
4. ⏳ **Still needed:** Add environment variable support (`CORRISCOPE_FIRMWARE_PATH`)
5. ⏳ **Still needed:** Update POCKET_CORRELATOR to accept and pass firmware_path parameter

### Phase 3: Testing Infrastructure ✓
1. ✓ Created `test_crs_connection.py` - comprehensive test script
2. ✓ Tests imports, network settings, mDNS discovery, and POCKET_CORRELATOR availability
3. ✓ Made executable with proper command-line interface
4. ✓ **All import tests passing!**

### Dependencies Fixed
1. ✓ Added `freetype-py` to `pyproject.toml` dependencies
2. ✓ Installed freetype-py package for SSD1306 display support
3. ✓ Upgraded required python version to 3.12 

## Known Issues

### ⚠️ PSArray NotImplementedError
The `PSArray()` function in `fpga_array.py` now raises `NotImplementedError` since power supply support was removed for CRS-only operation. This may break the `create_fpga_array()` function which still tries to instantiate PSArray. 

**Impact:** This could prevent CRS board connection if `create_fpga_array()` is used.

**Solution**: for `corr8`, do nothing. For `corr32` and `corr64`, need to integrate module `psucontrol` when it is ready to be deployed.

## Remaining Work

### Files/Directories Still to Remove:
- `corriscope/hardware/ice/` - ICE board support directory (if exists)
- `corriscope/hardware/zcu111.py` - ZCU111 support file (if exists)
- `corriscope/hardware/zuboard.py` - ZUBoard support file (if exists)
- Any ICE-specific firmware in `fpga_firmware/` directory

### Implementation Tasks:
1. **Fix PSArray issue** - Resolve NotImplementedError in create_fpga_array()
2. Complete firmware path resolution in `corriscope/hardware/crs/crs.py`
3. Add environment variable support (`CORRISCOPE_FIRMWARE_PATH`)
4. Update `POCKET_CORRELATOR` to accept and pass firmware_path parameter
5. Create firmware directory structure documentation
6. Update README with firmware configuration instructions
7. Add power supply support with `psucontrol`

### Testing Tasks:
1. Test CRS board connection with physical hardware
2. Validate firmware loading from external path
3. Create pytest test suite for CRS connection
4. Add CI/CD integration for automated testing

### Next Session Priorities:
1. **Fix PSArray NotImplementedError** - Critical for board connection
2. Complete firmware path implementation in CRS hardware class
3. Remove remaining non-CRS hardware files
4. Test with physical CRS hardware
5. Create comprehensive usage documentation

## Hardware Support Removal Plan

### Files/Directories to Remove:
- `corriscope/hardware/ice/` - ICE board support
- `corriscope/hardware/zcu111.py` - ZCU111 support  
- `corriscope/hardware/zuboard.py` - ZUBoard support
- `corriscope/hardware/mezzanine.py` - Mezzanine support
- Any ICE-specific firmware in `fpga_firmware/`

### Code to Clean:
- Remove ICE/ZCU111/ZUBoard imports from `fpga_array.py`
- Remove ICE-specific initialization code
- Remove ICE-specific command-line arguments
- Update hardware map processing to CRS-only

### Files to Keep:
- `corriscope/hardware/crs/` - CRS board implementation
- `corriscope/hardware/motherboard.py` - Base classes
- `corriscope/hardware/crate.py` - Crate abstraction
- All I2C device drivers (used by CRS)

## Firmware Path Configuration Design

### Requirements:
- FPGA bitstreams no longer stored in Git repository
- User must specify path to firmware directory
- Support for multiple firmware versions
- Clear error messages if firmware not found

### Implementation Approach:
1. Add `--firmware-path` argument to argparse in `fpga_array.py`
2. Pass firmware path through to bitstream loading functions
3. Update `set_fpga_bitstream_async()` to use custom path
4. Provide sensible defaults (e.g., `./firmware/`, `~/corriscope_firmware/`)
5. Environment variable support: `CORRISCOPE_FIRMWARE_PATH`

### Expected Directory Structure:
```
firmware/
├── crs/
│   ├── corr8/
│   │   └── crs_corr8.bit
│   ├── corr32/
│   │   └── crs_corr32.bit
│   ├── corr64/
│   │   └── crs_corr64.bit
│   └── ...
└── README.md
```

## Progress Tracking

### Completed:
- ✓ Import migration from pychfpga to corriscope
- ✓ Understanding of CRS connection architecture
- ✓ Identification of legacy hardware to remove
- ✓ Design of firmware path configuration

### In Progress:
- ⏳ Verification of supporting modules
- ⏳ Removal of non-CRS hardware support
- ⏳ Implementation of firmware path configuration

### Next Session:
- Test CRS board connection with physical hardware
- Validate firmware loading from external path
- Create comprehensive usage documentation
- Update GUI application to use new configuration

---

# February 12, 2026

## Session Overview
Initial exploration and understanding of the `corriscope` repository structure and functionality. `corriscope` was forked from the `pychfpga` repository. Planning for major transformation from developer-focused toolkit to user-friendly GUI application.

## Current System Understanding

**corriscope** is a sophisticated Python software package designed to control and visualize data from fully-FPGA based FX correlators implemented on t0.technology's Control and Readout System (CRS).

### Package Structure
The repository is organized as a unified Python package under `corriscope/`. Note that some of the files inherited from `pychfpga` listed here will not be used in `corriscope`, but are kept as either references to eventually be deleted or changed. Therefore, the summary below corresponds to the purpose of each module for `pychfpga` and are subject to change with `corriscope`.

#### Core Control Modules:
- **fpga_array.py** - Central orchestrator managing arrays of FPGA boards with comprehensive control capabilities
- **fpga_master.py** - High-level control interface with REST API server/client architecture
- **calculate_gains.py** - Automatic gain calculation and calibration systems with filtering algorithms
- **raw_acq.py** - Raw data acquisition, packet processing, and HDF5 data archiving
- **digital_gain.py** - Digital gain management and HDF5 archiving
- **gps.py** - GPS synchronization services with Spectrum Instruments TM4D support
- **ps.py** - Power supply control (Agilent N5700) with REST API
- **pocket_correlator.py** - Compact correlator implementation for smaller arrays
- **mdns_discovery.py** - Network service discovery using mDNS/Zeroconf
- **network_utils.py** - Network interface management and UDP buffer optimization

#### Sub-packages:
- **common/** - Shared utilities including async/sync bridging, enhanced collections, PFB-FFT processing, UDP packet handling
- **fpga_firmware/** - FPGA bitstream management and comprehensive chFPGA interface with F-engine, X-engine, and CT-engine support
- **hardware/** - Hardware abstraction layer including CRS integration, extensive I2C device library, and modular hardware mapping

### Technology Stack
- **Python 3.8** with async/await patterns
- **REST APIs** for service communication
- **HDF5** for data storage and archiving
- **Zeroconf/mDNS** for service discovery
- **Extensive I2C device support** for hardware control
- **UDP networking** with optimized packet handling

### Key Capabilities
- **Multi-scale correlator support** - Designed for various array sizes
- **Real-time data processing** - High-throughput packet processing and correlation
- **Automatic gain control** - Sophisticated calibration with RFI filtering
- **Hardware abstraction** - Modular design supporting different CRS configurations
- **Network optimization** - Advanced UDP buffer management and interface selection
- **Comprehensive monitoring** - Metrics collection and system health monitoring

## Project Transformation Goals

### Target Vision
Transform the current developer-focused toolkit into a **user-friendly GUI application** for operating FPGA-based correlators with the following specifications:

- **8-element correlator** 
- **32-element correlator** 
- **64-element correlator**

### Development Strategy
- **Preserve Core Functionality** - Maintain sophisticated FPGA control and data processing capabilities
- **GUI Development** - Create intuitive interfaces for end-users
- **Modular Approach** - Leverage existing hardware abstraction and FPGA management systems
- **User-Centric Design** - Focus on operator workflows rather than programmatic control

## Existing GUI Application Analysis

### Periscope V2 - The Foundation
Located in `corriscope/apps/`, this is our primary GUI application foundation:

**Current Structure:**
- **app.py** - Main PyQt5 GUI application (~700 lines)
- **correlator.py** - Standalone correlator system (~800 lines)
- **backend/** - Modular high-performance backend components
  - `udp_processor.py` - Optimized UDP receiver (5-10x faster)
  - `lag_computer.py` - Optimized lag computation (1.2x+ faster)  
  - `data_buffers.py` - Efficient data management (50% memory reduction)
- **README.md** - Comprehensive documentation

**Architecture Improvements:**
- **55% code reduction** (2050 lines vs 4500+ lines from original)
- **Modular backend design** with separate high-performance components
- **Standalone correlator system** that can run independently
- **Clean separation** between GUI and processing logic

**Performance Achievements:**
- **UDP processing:** 64,272 packets/sec with batch processing
- **Lag computation:** 1.66 ms/frame with pre-allocated buffers
- **Memory efficiency:** 50% reduction through optimized data structures
- **Thread-safe design** with minimal locking overhead

**Key Features:**
- **PyQt5-based GUI** with pyqtgraph for real-time plotting
- **Familiar tabbed interface** with optimized backend
- **Real-time correlator visualization** - spectra and waterfall plots
- **CRS board integration** via POCKET_CORRELATOR
- **Gain calibration workflow** with user-specified targets
- **Performance monitoring** with FPS counters and metrics
- **Dark theme styling** for professional appearance

**Reusable Components:**

### 1. **High-Performance Data Processing**
- `OptimizedPacketDecoder` - Vectorized UDP packet processing
- `OptimizedLagComputer` - Pre-allocated FFT buffers with caching
- `DataBuffer` - Efficient circular buffer management
- `SharedDataBuffers` - Zero-copy inter-thread communication

### 2. **GUI Framework Patterns**
- **Dynamic plot configuration system** - Product/component selectors
- **Tabbed visualization interface** - Easy to extend with new modes
- **Real-time parameter controls** - Spinners, checkboxes, combo boxes
- **Performance monitoring integration** - FPS counters, metrics display
- **Checkbox-based plot management** - Show/hide individual plots

### 3. **Hardware Integration Layer**
- **POCKET_CORRELATOR interface** - Seamless connection to CRS boards
- **Gain calibration workflow** - Automated with user-specified targets
- **Multi-mode operation** - Easy switching between different capture modes
- **Network interface optimization** - MTU checking, buffer tuning

### 4. **Signal Processing Pipeline**
- **Nyquist zone handling** - Automatic frequency axis adjustment
- **Phase unwrapping algorithms** - For coherent signal analysis
- **Running difference modes** - Change detection and RFI mitigation
- **Symmetric log scaling** - For wide dynamic range visualization

## GUI Development Strategy

### Recommended Approach
**Use the existing Periscope V2 architecture** in `corriscope/apps/` as the direct foundation for the new GUI tool:

1. **Leverage proven architecture** - Modular design with demonstrated 5-10x performance improvements
2. **Extend for multi-array support** - Add 32 and 64-element correlator capabilities
3. **Enhance user interface** - Add array-size-specific configuration options
4. **Maintain performance optimizations** - Keep the optimized backend components

### Key Components Available
- **High-performance backend** - Optimized UDP processing and lag computation in `backend/`
- **Modular GUI framework** - Tabbed interface with dynamic plot configuration in `app.py`
- **Hardware integration** - Seamless CRS board connectivity via existing modules
- **Signal processing pipeline** - Real-time visualization and analysis tools
- **Standalone correlator** - Independent processing system in `correlator.py`

### Development Priorities
1. **Multi-board coordination** - Extend to manage multiple CRS boards for larger arrays
2. **Scalable data handling** - Support increased throughput from 32/64-element systems
3. **Array-specific interfaces** - Tailored controls for different correlator scales
4. **User workflow optimization** - Streamlined operations for different array sizes

## Import Migration Completed

### Package Rename Implementation
Successfully updated all Python files in the repository to use `corriscope` imports instead of `pychfpga`:

**Changes Made:**
- **51 import statements updated** - All `from pychfpga` and `import pychfpga` statements converted to `corriscope`
- **Documentation references updated** - Path references in docstrings and comments updated
- **Variable names updated** - Variables like `pychfpga_path` renamed to `corriscope_path`
- **Complete migration** - Zero remaining `pychfpga` references in Python files

**Files Updated:**
- Core modules: `fpga_array.py`, `fpga_master.py`, `pocket_correlator.py`, `gps.py`, `raw_acq.py`
- Hardware abstraction: All files in `hardware/` subdirectories
- FPGA firmware: All files in `fpga_firmware/` subdirectories  
- GUI application: `apps/app.py` and related files
- Common utilities: All files in `common/` subdirectory

**Verification:**
- **Before:** 51+ pychfpga references found across Python files
- **After:** 0 pychfpga references remaining in Python files
- **Package consistency:** All imports now correctly reference the `corriscope` package structure

This migration ensures that the codebase is fully consistent with the new `corriscope` package name as specified in the updated `pyproject.toml`.

## Next Steps
- Build upon the existing `corriscope/apps/` architecture and performance optimizations
- Extend the modular backend to coordinate multiple CRS boards
- Design array-size-specific GUI enhancements (8/32/64-element modes)
- Implement scalable visualization for higher data rates
- Create unified configuration system for different correlator scales
