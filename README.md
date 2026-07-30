# `corriscope`

`corriscope` is a software package used for controlling and visualizing data from a fully-FPGA based FX correlator implemented on t0.technology's Control and Readout System (CRS).

`corriscope` is forked from `pychfpga`: branch `jfc/crs_corr64` with Git commit hash `220fc1b2ac907e0eb42ab188d71a6dcfd0ce6a6b`.

To connect to a CRS board, launch the `corriscope_venv` virtual environment on `mikoshi` with

```bash
conda activate corriscope_venv
```

To run the test script for checking imports work properly, the network is configured correctly, and you can program a CRS board with radio firmware, run from the command line

```python
python ~/Codes/corriscope/tests/test_crs_connection.py --serial 0166 --firmware-path ~/Codes/bitstreams --log-level DEBUG
```

Note that the above test script simply programs a board and confirms it is properly configured. It does not allow the user to collect data.

If you would like to collect data, use the Pocket Correlator firmware. Note that as of the time of this writing, this currently has unfixed bugs that need to be addressed. To start the Pocket Correlator, run in either an `ipython` session or Jupyter notebook 

```python
run ~/Codes/corriscope/corriscope/pocket_correlator.py --serial 0166 --stderr_log_level debug
```

Likewise, if you'd like to go to a slightly lower level in the software to help debug the Pocket Correlator, you can program the CRS directly with

```python
run ~/Codes/corriscope/corriscope/fpga_array.py crs 0166 --stderr_log_level debug --mode corr8 --prog 2 --open
```

Note that `fpga_array` does not provide simple functions for recovering data like the Pocket Correlator.