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
