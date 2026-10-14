# corriscope

This page describes corriscope, its main elements, and the basic tasks. The text uses
Simplified Technical English. The README describes the installation.

## 1. What corriscope is

corriscope controls t0.technology CRS boards that run the `chFPGA` FX-correlator firmware.
It programs the FPGA, configures the signal path, and receives the data on the host. The
same code supports the three firmware modes `corr8`, `corr32` and `corr64`.

An FX correlator does two steps. The F-engine makes a spectrum of each input with an FFT.
The X-engine multiplies the spectra of each pair of inputs and accumulates the products.
The accumulated products are the visibilities.

## 2. System overview

### 2.1 Hardware

A CRS board has:

- a Zynq UltraScale+ RFSoC with eight ADC inputs (the FPGA and the ARM processor),
- an ARM processor that runs the `tcpipe` real-time operating system (RTOS). The binary is
  `corriscope/hardware/crs/arm_firmware/tcpipe_crs.elf`,
- one Ethernet port to the host,
- a backplane connector and SFP ports for the links between boards,
- an OLED display that shows the serial number, the IP address, the firmware label and the
  FPGA core temperature.

A CRS backplane (also called a crate) holds four boards. `corr32` uses one backplane.
`corr64` uses two backplanes with SFP links between them.

### 2.2 Host connection

The host talks to a board through its Ethernet port only.

- Register access goes through **TCPipe**. TCPipe is a TCP connection to the ARM RTOS. The
  RTOS forwards register reads and writes to the FPGA. It also offers remote procedure calls
  (RPC) for the ADC, the clocks, the I2C devices and the OLED.
- Data goes from the FPGA to the host on **UDP**. Each board sends its own data packets.

The driver for TCPipe is `corriscope/hardware/interfaces/tcpipe.py`.

### 2.3 Firmware modes

| mode     | boards | inputs | corner turn | bitstream               |
|----------|--------|--------|-------------|-------------------------|
| `corr8`  | 1      | 8      | level 1 (in the board)              | `chfpga_crs_corr8.bit`  |
| `corr32` | 4      | 32     | level 2 (across the backplane)      | `chfpga_crs_corr32.bit` |
| `corr64` | 8      | 64     | level 3 (across two backplanes)     | `chfpga_crs_corr64.bit` |

The modes are rows of the table `chFPGA.PLATFORM_SUPPORT` in
`corriscope/fpga_firmware/chfpga/chfpga.py`. The bitstreams are in
`corriscope/fpga_firmware/bitstreams/` as git-lfs files.

## 3. Main software elements

| element | location | function |
|---------|----------|----------|
| `FPGAArray` | `corriscope/fpga_array.py` | Builds the list of boards from a hardware map. Programs and initializes them. Gives array-wide methods. |
| Hardware map | `corriscope/hardware/hardware_map.py` | Parses the hardware map string and finds the boards on the network. |
| `CRS` | `corriscope/hardware/crs/crs.py` | One CRS board: discovery, ARM firmware, bitstream upload, clocks, OLED. |
| `TCPipe` | `corriscope/hardware/interfaces/tcpipe.py` | Register access and RPCs through the ARM RTOS. |
| `chFPGA` | `corriscope/fpga_firmware/chfpga/chfpga.py` | The firmware driver for one board. Holds the module objects below. |
| F-engine | `corriscope/fpga_firmware/chfpga/f_engine/` | `FUNCGEN`, `FFT`, `SCALER` (one set per input) and `UCAP` (the capture engine). |
| Corner turn | `corriscope/fpga_firmware/chfpga/ct_engine/` | `UCT` with levels `CT1`, `CT2`, `CT3`, and `BPLINKS` for the board-to-board links. |
| X-engine | `corriscope/fpga_firmware/chfpga/x_engine/UCORR.py` | The correlator. |
| System | `corriscope/fpga_firmware/chfpga/system/` | Reference clock (`REFCLK`), GPIO, synchronization. |
| Receivers | `corriscope/common/` | `RawFrameReceiver` for ADC and FFT frames, `UCorrFrameReceiver` for visibilities, and the packet processors. |
| Helpers | `corriscope/common/` | `namespace`, `config`, `log`, `metrics`, `rest`. These replace the private `wtl` packages of pychfpga. |
| `raw_acq` | `corriscope/raw_acq/` | UDP receiver and HDF5 writers behind a REST server. For long captures. |
| `fpga_master` | `corriscope/fpga_master.py` | REST daemon that runs an array from a YAML configuration. Uses `raw_acq`, `calculate_gains` and `digital_gain`. |
| `calculate_gains` | `corriscope/calculate_gains.py` | Computes digital gains from `raw_acq` data. |
| `digital_gain` | `corriscope/digital_gain.py` | HDF5 archive of the programmed digital gains. |
| `gps` | `corriscope/gps.py` | REST server for the TM-4D GPS receiver that gives IRIG-B time. |
| `network_utils` | `corriscope/network_utils.py` | Checks the jumbo-frame and UDP-buffer settings of the capture interface. |
| `pocket_correlator` | `corriscope/pocket_correlator.py` | An older single-board application. See section 9. |
| scripts | `scripts/` | Debug helpers for UDP capture and TCPipe logs. |

## 4. The data path

The signal goes through these stages in each board. One frame is 16384 ADC samples.

1. **ADC**. Each input is sampled at 3.2 GS/s with 14 bits.
2. **FUNCGEN**. Selects the data source: the ADC or a test signal. Can shift the data.
3. **FFT**. A 4-tap polyphase filter bank and a 16384-point FFT. The output is 8192
   complex bins with 32+32 bits. The bin spacing is 195.3 kHz.
4. **SCALER**. Multiplies each bin by a digital gain. Keeps the top 4+4 bits of the product.
   This is the input format of the correlator.
5. **Corner turn** (`UCT`). Sends each bin to the board that correlates it. In `corr32` and
   `corr64`, the data crosses the backplane and the SFP links.
6. **UCORR**. Correlates all input pairs for its set of bins. Accumulates the products over
   `integ_period` frames. Changes the bin set after each integration.
7. **UCAP**. Packs the data in UDP packets. UCAP can also capture the output of FUNCGEN or
   SCALER. That is how the host gets raw ADC and FFT frames.

The host receives the UDP packets with a receiver object. Section 8 describes the receivers.

## 5. The hardware map

The hardware map is a string. It lists the boards of the array.

- `crs <id> [<id> ...]` adds CRS boards. An `<id>` is a serial number (`127`), a serial
  number and a slot (`127:1`), a serial number, slot and crate (`127:1:1`), a hostname, or
  an IP address.
- `crc4 <serial>` adds every board that is found on the backplane with that serial number.

The slot number sets the order of the boards in `ca.ib`. In `corr32` and `corr64`, the
slot numbers must match the physical slots, because the corner turn depends on them.

Examples:

```text
crs 127:1
crs 127:1 134:2 129:3 121:4
crs 127:1 134:2 129:3 121:4 124:5 133:6 131:7 120:8
crc4 1
```

## 6. Synchronization

All boards must start their frame counters at the same time. `FPGAArray` has these
parameters:

| parameter | values | function |
|-----------|--------|----------|
| `sync_method` | `'local'`, `'trig'`, `'irig-b'` | `local`: software sync on one board. `trig`: one board sends a trigger on the backplane. `irig-b`: the boards sync to IRIG-B time. |
| `sync_source` | `'bp_trig'`, `'bp_time'`, `'sma_a'`, ... | The input that each board listens to. |
| `sync_master` | slot number | The board that generates the sync signal. |
| `sync_master_source` | `'sync_out'`, `'irigb_gen'`, ... | The signal that the master board sends. |
| `sync_master_output` | output number | The backplane output of the master board. |

Use `sync_method='local'` for `corr8`. Use `sync_method='trig'` with `sync_master=1` for
`corr32` and `corr64`. Call `await ca.sync_async()` before each capture that must be
simultaneous on all boards.

## 7. Basic tasks

Run these tasks in a notebook. Each code cell can use `await`.

### 7.1 Program and initialize the boards

```python
from corriscope.fpga_array import FPGAArray

ca = await FPGAArray('crs 127:1', mode='corr8', sync_method='local', stderr_log_level='info')
```

- `mode` selects the bitstream and the initialization sequence.
- `prog=2` (the default when `mode` is given) always programs the FPGA. `prog=1` keeps a
  programmed FPGA.
- `bitfile='/folder'` changes the bitstream folder.
- `ignore_missing_boards=True` continues when a board of the map is not found.

`ca.ib` is the list of boards. `ca.ib[k]` is a `chFPGA` object. `ca.ib[k].chan[c]` holds
the `FUNCGEN`, `FFT` and `SCALER` of input `c`. A flat input index is `k * 8 + c`.

After the initialization, the OLED of each board shows the mode in the firmware label.

### 7.2 Get raw ADC data

```python
await ca.set_channelizers_async(data_source='adc', sync=False)
await ca.start_data_capture_async(0.02, source='funcgen', mode=0, sync=False)
r = await ca.get_data_receiver_async(tcp=False)
t, adc, cnt = r.read_raw_frames(ncap=4, split=True, flush=True)
```

- The first argument of `start_data_capture_async` is the burst period in seconds.
- UCAP `mode=0` sends 2 frames of all 8 inputs per burst. `mode=3` sends 16 frames of one
  input. Give the input with `channels=[c]`.
- `adc` has the shape `(inputs, frames, 16384)`. The values are 14-bit signed integers.

### 7.3 Get raw FFT data

```python
await ca.start_data_capture_async(0.05, source='scaler', mode=0, data_type='fft_2x', sync=False)
t, fft, cnt = r.read_raw_frames(ncap=16, split=True, flush=True)
```

- `data_type='fft_2x'` gives the FFT output with 32+32 bits, before the gain.
- `data_type='fft_4bit'` gives the 4+4-bit output of the scaler. Read it with
  `format='16+16'`.
- `fft` has the shape `(inputs, frames, 8192)` and is complex. Bin `b` is at frequency
  `b * 3200 / 16384` MHz in the first Nyquist zone.

### 7.4 Set the digital gains

The digital gain is a number for each bin of each input. The scaler multiplies the FFT bin by
the gain and keeps 4+4 bits. The gain must put the signal in the range of the 4-bit value.
The target rms of the 4-bit value is `1.5 * sqrt(2)`. The gain is not a calibration of the
visibilities.

A flat gain for all bins:

```python
await ca.set_channelizers_async(gains=1 << 25, sync=False)
```

A gain for each bin, from a measured FFT:

```python
import numpy as np
target = 1.5 * np.sqrt(2) * 2**44
mag = np.abs(fft).mean(axis=1)                       # (inputs, bins)
gain = target / mag
for k, board in enumerate(ca.ib):
    board.set_gains(gain={c: gain[k * 8 + c] for c in range(8)}, bank=0)
await ca.sync_async()
```

`SCALER.get_gain_table(bank=0)` reads back the gains that the FPGA applies. The notebook
`crs_corr_walkthrough.ipynb` shows the full procedure.

### 7.5 Get visibilities

```python
await ca.start_correlators_async(32768)
cr = await ca.get_corr_receiver_async()
tt, vis, counts, sats = cr.read_corr_frames(flush=True, soft_integ_period=8, number_of_results=2)
```

- The argument of `start_correlators_async` is `integ_period`, the number of frames per
  firmware integration. Use 8192 to 32768.
- Each board correlates a subset of the bins. The subset changes after each integration.
  `soft_integ_period=8` sums 8 integrations in software, so that all bins are present.
- `vis` has the shape `(results, 8192, products)`. `cr.get_ij_to_prod_map()[i, j]` is the
  product index of inputs `i` and `j`. `cr.get_good_bin_mask()` marks the bins that have
  data. A few bins are used for the readout and have no data.
- `sats` counts the saturated products. `counts` is the number of integrations in each
  result.

### 7.6 Stop the data flow

```python
for board in ca.ib:
    board.stop_data_capture()
    board.CORR.stop_correlator()
```

The boards stay programmed.

### 7.7 Use the command line

This command programs and initializes the boards. It then opens a Python session with the
array in `ca`:

```bash
uv run python -m corriscope.fpga_array crs 127:1 --mode corr8 --prog 2 --stderr_log_level info
```

### 7.8 Record long captures to HDF5

`fpga_master` runs an array from a YAML configuration file. It starts `raw_acq` servers,
computes and loads the digital gains, and writes the data to HDF5 files. The configuration
example is `tests/configs/corr8.yaml`.

```bash
uv run fpga_master --help
uv run raw_acq --help
```

## 8. The receivers

| receiver | method | returns |
|----------|--------|---------|
| `RawFrameReceiver` (`ca.get_data_receiver_async`) | `read_raw_frames(ncap, split, format, flush)` | `(timestamps, data, counts)`. `data` is `(inputs, frames, samples)` for ADC data and `(inputs, frames, bins)` for FFT data. |
| `UCorrFrameReceiver` (`ca.get_corr_receiver_async`) | `read_corr_frames(number_of_results, soft_integ_period, flush)` | `(timestamps, data, counts, saturations)`. `data` is `(results, bins, products)`. |

The receivers return the inputs in the order of `ca.ib`, then the order of the channels on
each board. Call `await ca.sync_async()` before a read when the frames of all boards must
have the same time stamp.

## 9. Known limits

- `corriscope.pocket_correlator` builds `FPGAArray` without `await`. Its constructor fails.
  The notebooks do not use it. The fix is one line:
  `run_async(FPGAArray(...).init())`.
- `tests/test_crs_discovery.py::test_legacy_fwreq_format_is_rejected` fails. It also fails
  in pychfpga.
- The code lines marked `# verify on hardware` in `crs_corr_walkthrough.ipynb` were written
  without a board. Check them on the first run.
- pychfpga has no license file. Check the license before you publish corriscope.

## 10. Origin

corriscope is the CRS slice of [pychfpga](https://bitbucket.org/winterlandcosmology/pychfpga)
at commit `ecf46505`. At that commit, the branches `jfc/crs_corr8`, `jfc/crs_corr32` and
`jfc/crs_corr64` point at the same code. The package was renamed `pychfpga` to `corriscope`.
The module paths are the same.

### 10.1 Removed from pychfpga

These elements are for other hardware. They are not on the CRS data path.

- IceBoard, IceCrate, MGADC08 mezzanine, ZCU111, ZUBoard and Agilent power supplies.
- The BRAM corner-turn engine (`bct_engine`), the crossbars, the shuffle engine and the
  10 GbE `gpu` and `xglink` modules. The CRS uses `UCT` and 25G `BPLINKS`.
- The IceBoard `ADCDAQ`, `PROBER` and `CORR44` modules. The CRS uses the RFSoC ADCs, `UCAP`
  and `UCORR`.
- The FPGA-side SPI and I2C masters. The CRS reaches its I2C devices through TCPipe RPCs.
- The SSD1306 Python driver. The ARM RTOS drives the OLED.
- The FMC mezzanine code, the direct UDP register transport and the non-CRS platform IDs.
- ADC delay calibration, mezzanine power sequencing and the `iceboards`, `icecrates`,
  `exclude_iceboards` and `no_mezz` parameters of `FPGAArray`.
- The periscope Qt application, the firmware simulations and the `ps` power-supply server.

### 10.2 Replaced private dependencies

pychfpga used six private `wtl.*` packages. corriscope has its own versions of the parts
it used. `tests/test_common_utils.py` tests them.

| pychfpga | corriscope | content |
|----------|------------|---------|
| `wtl.namespace` | `corriscope/common/namespace.py` | `NameSpace`, `merge_dict` |
| `wtl.config` | `corriscope/common/config.py` | `load_yaml_config("file.yaml:section .other key=value")` |
| `wtl.log` | `corriscope/common/log.py` | `get_logger`, `setup_logging`, `stop_logging`, `setup_basic_logging` |
| `wtl.metrics` | `corriscope/common/metrics.py` | `Metrics` with Prometheus text and gzip output |
| `wtl.rest` | `corriscope/common/rest.py` | `AsyncRESTServer`, `@endpoint`, `AsyncRESTClient`, `run_client`, `SocketContext` |
| `wtl.h5-archive` | `corriscope/digital_gain.py` | the gain archive, same HDF5 layout |

### 10.3 Added

- `corriscope/fpga_firmware/__init__.py` imports `chFPGA`. This registers the firmware class
  before `FPGAFirmware.get_firmware()` runs.
- `set_channelizer()` raises `ValueError` when it gets a MGADC08-only keyword argument.
- The OLED firmware label shows the firmware mode after the initialization.
- `raw_acq` finds the MAC address of the capture interface with `ifaddr` and `/sys/class/net`.
  pychfpga used `netifaces`, which has no binary wheels and needs a C compiler.
