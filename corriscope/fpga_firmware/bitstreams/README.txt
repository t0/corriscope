CRS FPGA bitstreams, stored with git-lfs (see .gitattributes). This folder is the default
search path of FPGABitstream / FPGAFirmware.get_firmware():

    chfpga_crs_corr8.bit    corr8  (1 board, pocket correlator)
    chfpga_crs_corr32.bit   corr32 (4 boards on one backplane)
    chfpga_crs_corr64.bit   corr64 (8 boards on two backplanes)
    chfpga_crs_ct.bit       shuffle8 / chan8 streaming firmware

Run `git lfs pull` after cloning if the files are small LFS pointers instead of ~33 MB each.
A different folder or file can be used with FPGAArray(..., bitfile=<folder or file>).
