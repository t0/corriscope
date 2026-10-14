# corriscope

corriscope is the Python control software for the t0.technology **CRS** board. The CRS board is
a Zynq UltraScale+ RFSoC board. The board runs the `chFPGA` FX-correlator firmware. One code
base supports three firmware modes:

| mode     | hardware                                              | bitstream               |
|----------|-------------------------------------------------------|-------------------------|
| `corr8`  | 1 board, 8 inputs                                     | `chfpga_crs_corr8.bit`  |
| `corr32` | 4 boards on one backplane, 32 inputs                  | `chfpga_crs_corr32.bit` |
| `corr64` | 8 boards on two backplanes with SFP links, 64 inputs  | `chfpga_crs_corr64.bit` |

The full description of the software is in [docs/corriscope.md](docs/corriscope.md).

## Set up the environment

The reference environment is a [uv](https://docs.astral.sh/uv/) virtual environment.
`uv` installs Python 3.12, corriscope, Jupyter, `ipympl` and `pytest` in one step.

1. Install `uv` if it is not installed:

   ```bash
   curl -LsSf https://astral.sh/uv/install.sh | sh
   ```

2. Install git-lfs. The bitstreams are git-lfs files:

   ```bash
   git lfs install
   ```

3. Clone the repository and go to its root folder. The CRS code is on the branch
   `crs-corr-unified`. The branch `main` is the old pocket-correlator code and has no
   bitstreams.

   ```bash
   git clone -b crs-corr-unified git@github.com:t0/corriscope.git
   cd corriscope
   ```

   If you have a clone on `main`, change the branch:

   ```bash
   git fetch origin
   git checkout crs-corr-unified
   git lfs pull
   ```

4. Make the environment. This command creates `.venv/` from `uv.lock`:

   ```bash
   uv sync --all-extras
   ```

5. Make sure that the bitstreams are complete. Each `.bit` file must be about 33 MB. If a
   file is small, it is a git-lfs pointer. Then run:

   ```bash
   ls -l corriscope/fpga_firmware/bitstreams/
   git lfs pull
   ```

6. Start Jupyter from the repository root:

   ```bash
   uv run jupyter lab
   ```

Each `uv run <command>` runs the command in `.venv/`. You do not activate the environment.

If you open the notebooks from a different Jupyter installation, register the kernel once:

```bash
uv run python -m ipykernel install --user --name corriscope
```

Then select the `corriscope` kernel in the notebook.

### Alternative: an existing conda environment

If you have the `crs` conda environment, you can use it instead of `uv`. The environment
must have Python 3.12.

```bash
conda activate crs
pip install -e '.[notebooks,test]'
```

The `uv` environment is the reference. Use the conda environment only when `uv` is not an
option.

### Network

The data arrives on UDP. The capture interface must have jumbo frames and large socket
buffers. Check the interface with:

```bash
uv run python -m corriscope.network_utils --status
```

## Quick start

`FPGAArray` is the API that the notebooks use. To change the firmware mode, change `mode=`
and the hardware map string. Edit the serial numbers for your boards.

```python
from corriscope.fpga_array import FPGAArray

# corr8: one board, given as serial:slot
ca = await FPGAArray('crs 127:1', mode='corr8', sync_method='local', stderr_log_level='info')

# corr64: eight boards on two backplanes; the board in slot 1 drives the sync
hwm = 'crs 127:1 134:2 129:3 121:4 124:5 133:6 131:7 120:8'
ca = await FPGAArray(hwm, mode='corr64',
                     sync_method='trig', sync_source='bp_trig',
                     sync_master=1, sync_master_source='sync_out', sync_master_output=1,
                     stderr_log_level='info')

# raw ADC samples from all inputs
await ca.set_channelizers_async(data_source='adc', sync=False)
await ca.start_data_capture_async(0.02, source='funcgen', mode=0, sync=False)
r = await ca.get_data_receiver_async(tcp=False)
t, adc, cnt = r.read_raw_frames(ncap=4, split=True, flush=True)

# visibilities
await ca.set_channelizers_async(gains=1 << 25, sync=False)
await ca.start_correlators_async(32768)
cr = await ca.get_corr_receiver_async()
tt, vis, counts, sats = cr.read_corr_frames(flush=True, soft_integ_period=8, number_of_results=2)
```

The command line does the same program-and-initialize step. It then opens a Python session
with the array in `ca`:

```bash
uv run python -m corriscope.fpga_array crs 127:1 --mode corr8 --prog 2 --stderr_log_level info
```

## Notebooks

The notebooks are in `tests/crs_corr_tests/`. Set `MODE` in the first code cell of each
notebook.

| notebook | content |
|----------|---------|
| `crs_corr_demo.ipynb` | Start here. Program `corr8`, `corr32` or `corr64`. Get ADC data, FFT data and correlator data. |
| `crs_corr_walkthrough.ipynb` | The demo, plus per-bin digital gains and a comparison of the ADC model, the firmware FFT and the correlator. |
| `correlator_notebooks/` | Correlator characterization: lag spectra, closure residuals and a beamformer. See its `README.md`. |
| `crs_corr64_tests.ipynb`, `crs_corr32_tests*.ipynb`, `crs_corr_example.ipynb` | The reference tests from pychfpga. |

## Tests

The unit tests do not need hardware:

```bash
uv run pytest -q
```

One test is known to fail: `test_crs_discovery.py::test_legacy_fwreq_format_is_rejected`.
It also fails in pychfpga.

## Origin

corriscope is a fork of [pychfpga](https://bitbucket.org/winterlandcosmology/pychfpga) by
JF Cliche (McGill University / Winterland Cosmology), reduced to the CRS correlator
firmware. It has no private dependencies. The section "Origin" in
[docs/corriscope.md](docs/corriscope.md#origin) lists what was removed and what was replaced.
