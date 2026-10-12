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
- `fpga_master`, `gps`, `ps`, `raw_acq`, `calculate_gains`, `digital_gain`, the periscope Qt app,
  docs, qc and firmware simulations. These are candidates to port later if needed.
- The package was renamed `pychfpga` -> `corriscope`; everything else keeps its module path.

Two things were *added*: `corriscope/fpga_firmware/__init__.py` imports `chFPGA` so the
firmware class is registered before `FPGAFirmware.get_firmware()` is called (in pychfpga
this relied on an incidental import), and MGADC08-only `set_channelizer()` keyword arguments
now raise if given a non-`None` value instead of silently touching hardware that is not there.

## Installation

Python 3.12 is required. The private `wtl.*` packages come from the Winterland bitbucket
(ssh access needed), exactly as for pychfpga.

```bash
git clone git@github.com:t0/corriscope.git
cd corriscope
python3.12 -m venv .venv && source .venv/bin/activate
pip install -e .
pip install jupyterlab ipympl      # for the notebooks
```

### Bitstreams

The `.bit` files are not in git (33 MB each). Copy the CRS bitstreams into
`corriscope/fpga_firmware/bitstreams/` (the default search folder) or pass
`FPGAArray(..., bitfile='/path/to/folder')`.

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

- `tests/crs_corr_tests/crs_corr64_tests.ipynb` is the reference end-to-end test (works for the
  other modes by editing the first cell as shown above). `crs_corr32_tests*.ipynb` and
  `crs_corr_example.ipynb` (corr8) are the earlier variants. Outputs were stripped.
- `tests/crs_corr_tests/pocket_correlator/` exercises `corriscope.pocket_correlator`.
- Hardware-free unit tests: `python -m pytest tests/` (one stale assertion in
  `test_crs_discovery.py::test_legacy_fwreq_format_is_rejected` also fails in pychfpga).
