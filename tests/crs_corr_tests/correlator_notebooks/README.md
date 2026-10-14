# Correlator characterization notebooks

Ported from the `correlator_notebooks` set written against pychfpga for an 8-board `corr64`
array. Data flow:

1. `get_adc_and_corr_data_nyq.ipynb` (needs hardware). Programs the array, syncs the ADCs,
   captures full-resolution FFT frames from all 64 inputs (`fft_2x`, scaler bypassed), computes
   per-bin floating-point gains (`target * 2**44 / mean|FFT|`) and writes them to the scalers,
   captures the 4+4-bit scaler output (`fft_4bit`, `format='16+16'`) to check the gains land on
   target, starts the correlator with automatic bin-set rotation and software-integrates all
   bin sets, then captures raw ADC frames (`fft_bypass=True`, `source='funcgen'`). Writes two
   pickles per Nyquist zone: `corr_gain_cal_fft_data_nyq<N>.pkl` (visibilities, product map,
   gains) and `corr_adc_data_nyq<N>.pkl` (ADC frames, stream ids).
2. `compare_nyq_plots.ipynb` and `compare_newf_plots.ipynb` (offline). Load those pickles and
   compare correlator products against ADC-derived products: lag spectra per baseline, peak
   lag and amplitude, closure-phase residuals, and a wideband delay-and-sum beamformer. The
   two differ only in which pickles they load (Nyquist zones 1-4 versus three "new firmware"
   runs); the analysis code is identical. Their load cells hardcode the filenames of the
   original runs (`corr_gain_cal_fft_data.pkl`, `..._2ndnyq_s4.pkl`, `..._newf_1.pkl`, ...),
   which the acquisition notebook does not write; edit those cells to the `_nyq<N>` files you
   actually generated.

All corriscope APIs used by the acquisition notebook exist on this branch (`TCPipe.adc_sync`,
scaler capture types 3 and 4, formats `'14'` and `'16+16'`, `sync_method='trig'`,
`UCORR.AUTO_BIN_SET`, `get_good_bin_mask`, `get_ij_to_prod_map`). The port fixed three API
mismatches noted at the top of that notebook. All three use `%matplotlib ipympl` (needs the `ipympl` package, or change to `inline`); the
analysis notebooks also use `tqdm` (optional) and `mpl_toolkits.mplot3d` (ships with matplotlib).
