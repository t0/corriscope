Drop the CRS FPGA bitstreams here (this folder is the default search path of
FPGABitstream / FPGAFirmware.get_firmware):

    chfpga_crs_corr8.bit
    chfpga_crs_corr32.bit
    chfpga_crs_corr64.bit
    chfpga_crs_ct.bit      (shuffle8 / chan8, optional)

They are gitignored (*.bit). Copy them from pychfpga/pychfpga/fpga_firmware/bitstreams/
or point FPGAArray(..., bitfile=<folder>) at a folder that contains them.
