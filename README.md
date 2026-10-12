# corriscope

`corriscope` controls t0.technology **CRS** boards (Zynq UltraScale+ RFSoC) running the
`chFPGA` FX-correlator firmware. One code base supports the three firmware
configurations:

| mode     | hardware                                   | bitstream              |
|----------|--------------------------------------------|------------------------|
| `corr8`  | 1 board, 8 inputs ("pocket correlator")    | `chfpga_crs_corr8.bit` |
| `corr32` | 4 boards on one backplane, 32 inputs       | `chfpga_crs_corr32.bit`|
| `corr64` | 8 boards on two backplanes (SFP links), 64 inputs | `chfpga_crs_corr64.bit` |

The CRS `shuffle8`/`chan8` streaming firmware (`chfpga_crs_ct.bit`) is also still
recognized, but is not the focus of this repository.

## Origin

This tree is the CRS slice of
[pychfpga](https://bitbucket.org/winterlandcosmology/pychfpga), extracted from commit
`ecf46505` ("build (crs_corr8/32/64): BITSTREAM with Scaler sync log gains, ADC cal
enabled"). At that commit the `jfc/crs_corr8`, `jfc/crs_corr32` and `jfc/crs_corr64`
branches all point at the same code, so there was nothing to merge: the three modes are
rows of `chFPGA.PLATFORM_SUPPORT` in `corriscope/fpga_firmware/chfpga/chfpga.py`.

What was removed relative to pychfpga (first pass, behaviour of the CRS path kept intact):

- IceBoard / IceCrate / MGADC08 mezzanine, ZCU111 and ZUBoard hardware, Agilent power supplies.
- The BRAM corner-turn engine (`bct_engine`, crossbars, shuffle, 10 GbE `gpu`/`xglink`) and the
  IceBoard `ADCDAQ` / `PROBER` / `CORR44` / FPGA-side SPI and I2C firmware modules.
- ADC delay calibration, mezzanine power sequencing and the `iceboards` / `icecrates` /
  `exclude_iceboards` / `no_mezz` parameters of `FPGAArray`.
- The periscope Qt app, docs, qc and firmware simulations.
- The package was renamed `pychfpga` -> `corriscope`; everything else keeps its module path.

Site-operation modules kept from pychfpga (unchanged apart from the rename and the removal
of the ADC-delay calibration step):

| module | role | entry point |
|--------|------|-------------|
| `fpga_master` | REST daemon that builds the `FPGAArray` from a YAML config (`tests/configs/corr8.yaml`), drives `raw_acq` servers, computes/loads/saves digital gains, starts HDF5 captures, exposes metrics | `fpga_master` |
| `raw_acq` | UDP receiver + packet processors + HDF5 writers for raw ADC/FFT frames and correlator products, behind a REST server/client | `raw_acq` |
| `calculate_gains` | digital-gain computation engine (direct and iterative) fed by `raw_acq` RMS/FFT data; used by `fpga_master` | library |
| `digital_gain` | HDF5 archive of the programmed scaler gains (per input, per bin); `fpga_master` load/save | library |
| `gps` | REST server for the Spectrum Instruments TM-4D GPS receiver that provides IRIG-B time (via a serial-to-ethernet adapter) | `corriscope_gps` |

(`ps`, the Agilent N5700 power-supply server from pychfpga, was dropped: no such supply on CRS systems.)

`scripts/` holds the CRS debugging helpers that go with `raw_acq` (UDP capture checks, live
FFT rms, TCPipe log watchers).

### No private dependencies

pychfpga depended on six private `wtl.*` packages from the Winterland Bitbucket. corriscope
re-implements the parts it used, so `pip install .` needs only PyPI packages:

| replaced | by | what it provides |
|----------|----|------------------|
| `wtl.namespace` | `corriscope/common/namespace.py` | `NameSpace` attribute-access mapping, `merge_dict` |
| `wtl.config` | `corriscope/common/config.py` | `load_yaml_config("file.yaml:section .other key=value")` |
| `wtl.log` | `corriscope/common/log.py` | `get_logger`, `setup_logging` / `stop_logging`, `setup_basic_logging` |
| `wtl.metrics` | `corriscope/common/metrics.py` | `Metrics` container with Prometheus text/gzip output |
| `wtl.rest` | `corriscope/common/rest.py` | `AsyncRESTServer`, `@endpoint`, `AsyncRESTClient`, `run_client`, `SocketContext` |
| `wtl.h5-archive` | `corriscope/digital_gain.py` | the gain archive is now self-contained (same HDF5 layout) |

`tests/test_common_utils.py` covers them.

Two things were *added*: `corriscope/fpga_firmware/__init__.py` imports `chFPGA` so the
firmware class is registered before `FPGAFirmware.get_firmware()` is called (in pychfpga
this relied on an incidental import), and MGADC08-only `set_channelizer()` keyword arguments
now raise if given a non-`None` value instead of silently touching hardware that is not there.

## Installation

Python 3.12 is required. All dependencies are on PyPI (see "No private dependencies" below).

```bash
git clone git@github.com:t0/corriscope.git
cd corriscope
python3.12 -m venv .venv && source .venv/bin/activate
pip install -e .
pip install jupyterlab ipympl      # for the notebooks
```

### Bitstreams

The four CRS bitstreams (`corr8`, `corr32`, `corr64`, `ct`; 33 MB each) are in
`corriscope/fpga_firmware/bitstreams/`, stored with **git-lfs**. Install git-lfs before
cloning (`git lfs install`), or run `git lfs pull` afterwards if the files are small pointer
files. That folder is the default search path; `FPGAArray(..., bitfile='/path/to/folder')`
overrides it.

### Network

UDP capture needs jumbo frames and large socket buffers; see the docstring of
`corriscope/fpga_firmware/chfpga/f_engine/ucap.py` or run
`python -m corriscope.network_utils --status`.

## Usage

The API used by the test notebooks is `FPGAArray`. Switching between the three firmware
modes is only a change of `mode=` and of the hardware map string `hwm`:

```python
from corriscope.fpga_array import FPGAArray

# corr8: one board, by serial number
ca = await FPGAArray('crs 0037', mode='corr8', sync_method='local', stderr_log_level='info')

# corr32: four boards on one backplane, serial:slot
ca = await FPGAArray('crs 55:1 37:2 48:3 39:4', mode='corr32',
                     sync_method='irigb', sync_source='bp_trig',
                     sync_master=1, sync_master_source='sync_out', sync_master_output=1)

# corr64: every board on CRS backplane (crate) serial 1
ca = await FPGAArray('crc4 1', mode='corr64',
                     sync_method='irigb', sync_source='bp_trig',
                     sync_master=1, sync_master_source='sync_out', sync_master_output=1,
                     ignore_missing_boards=True)

await ca.start_data_capture_async(0.050, source='scaler', mode=3, channels=[0], data_type='fft_2x', sync=False)
await ca.start_correlators_async(32768)
r = await ca.get_data_receiver_async(tcp=False)
cr = await ca.get_corr_receiver_async()
t, fft2, c = r.read_raw_frames(ncap=10, split=True)
tt, dd, cc, ss = cr.read_corr_frames(flush=True, soft_integ_period=8, number_of_results=2)
```

Hardware map tokens: `crs` introduces CRS boards (`serial`, `serial:slot`,
`serial:slot:crate`, hostname or IP address), `crc4` introduces a CRS backplane by serial
number and selects every board on it.

Command-line equivalent (programs and initializes the boards, then drops you in a Python
session with `ca`):

```bash
python -m corriscope.fpga_array crs 0037 --mode corr8 --prog 2 --stderr_log_level info
```

## Tests

- `tests/crs_corr_tests/crs_corr_walkthrough.ipynb`: start here. Pick `MODE` in the first
  cell (`corr8`/`corr32`/`corr64`), then: program, raw ADC captures and plots, raw FFT frames,
  digital gains, correlator and visibilities, and an overplot of ADC-derived spectrum,
  firmware FFT, quantized prediction and correlator autocorrelation.
- `tests/crs_corr_tests/crs_corr64_tests.ipynb` is the reference end-to-end test the
  walkthrough was built from. `crs_corr32_tests*.ipynb` and `crs_corr_example.ipynb` (corr8)
  are the earlier variants. Outputs were stripped.
- `tests/crs_corr_tests/pocket_correlator/` exercises `corriscope.pocket_correlator`.
- Hardware-free unit tests: `python -m pytest tests/` (one stale assertion in
  `test_crs_discovery.py::test_legacy_fwreq_format_is_rejected` also fails in pychfpga).
