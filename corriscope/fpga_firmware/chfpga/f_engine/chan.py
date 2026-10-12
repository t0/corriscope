#!/usr/bin/python
# Disable pylint Line too long (=C0301)
# pylint: disable=C0301

"""
chan.py module
    Implements interface to the channelizer modules

History:
    2011-07-12 : JFC : Created from test code in chFPGA.py
    2012-08-31 JFC: Swapped addresses of PROBER and SCALER to match the same change in firmware
    2012-09-25 JFC: Renamed to FRAMER and FR_DIST to SRCSEL
"""

# Standard packages

import logging

# Pypi packages

from numpy import nan as npNaN

# Local packages

from ..mmi import MMIRouter
from . import fft
from . import scaler
from . import funcgen


class Chan(MMIRouter):
    """ Implements the interface to an individual channelizer"""
    ROUTER_PORT_NUMBER_WIDTH = 3
    ROUTER_PORT_MAP = {
        'ADCDAQ': 0,
        'FFT': 2,
        'SCALER': 3,
        'PROBER': 4,
        'FUNCGEN': 5
    }

    def __init__(self, router, router_port, instance_number, config):
        self.chan_number = instance_number  # store current channelizer number for this instance
        self.logger = logging.getLogger(__name__)
        super().__init__(router=router, router_port=router_port)
        self.config = config

        self.FFT = fft.FFT(router=self, router_port='FFT', instance_number=instance_number, config=self.config.add("fft"))
        self.SCALER = scaler.SCALER(router=self, router_port='SCALER', instance_number=instance_number, config=self.config.add("scaler"))
        self.FUNCGEN = funcgen.FUNCGEN(router=self, router_port='FUNCGEN', instance_number=instance_number, config=self.config.add("funcgen"))
        self.frame_length = self.fpga.FRAME_LENGTH

    def __repr__(self):
        """ Return a string that represents this object and its parent object.
        """
        return f"{self.fpga!r}.{self.__class__.__name__}({int(self.chan_number)})"

    def init(self):
        """ Initializes the channelizer modules"""
        self.FFT.init()
        self.SCALER.init()
        self.FUNCGEN.init()

    def get_id(self):
        """ Return the channel ID as a (board, slot, channel) tuple.

        Returns:

            channel ID as a (board, slot, channel) tuple
        """

        return self.fpga.get_id(self.chan_number)

    def status(self):
        """ Displays the status of the channelizer modules"""
        pass

    def get_sim_output(self, analog_input):
        funcgen_out = self.FUNCGEN.get_sim_output(analog_input)
        fft_out = self.FFT.get_sim_output(funcgen_out)
        scaler_out = self.SCALER.get_sim_output(fft_out)
        return scaler_out

class ChanArray(MMIRouter):
    """
    Instantiates a container for all channelizers available on the FPGA.
    It mimics the basin functionnalities of a 'dict'.

    """

    ROUTER_PORT_NUMBER_WIDTH = 4

    def __init__(self, *, router, router_port, config, verbose=0):
        self.verbose = verbose
        self.logger = logging.getLogger(__name__)
        super().__init__(router=router, router_port=router_port)
        self.config = config
        # Create an instance of ADC_chip for each chip of the FMC board
        self.frame_length = self.fpga.FRAME_LENGTH
        self.chan = []
        for i in range(self.fpga.NUMBER_OF_CHANNELIZERS):
            self.chan.append(Chan(router=self, router_port=i, instance_number=i, config=self.config.add(i)))

    def update_config(self):
        """Refresh per-channel FUNCGEN/SCALER/FFT configs.

        Array raw/corr config assembly reads those submodule States directly.
        """
        ctx = ('ChanArray.update_config', self)
        board = self.fpga.config
        board_attrs = (
            'platform_id', 'adc_bits_per_sample', 'adc_bytes_per_sample',
            'fft_bins_per_word',
        )
        board_stale = board.has_changed(board_attrs, context=ctx)
        for chan in self.chan:
            ch_ctx = (ctx, chan.chan_number)
            if not board_stale and not chan.config.has_changed(context=ch_ctx):
                continue
            chan.FUNCGEN.update_config()
            chan.SCALER.update_config()
            chan.FFT.update_config()
            chan.config.ack(context=ch_ctx)
        if board_stale:
            board.ack(board_attrs, context=ctx)
        return self.config

    def __repr__(self):
        """ Return a string that represents this object and its parent object.
        """
        return f"{self.fpga!r}.{self.__class__.__name__}"

    def __getitem__(self, key):
        """If the user indexes this object (chan[n] instead of chan) then return the channelizer instance"""
        return self.chan[key]

    def __len__(self):
        """Returns the number of channelizers"""
        return len(self.chan)

    def __contains__(self, value):
        """
        """
        return value in self.keys()

    def __iter__(self):
        return iter(self.chan)

    def keys(self):
        """
        """
        return list(range(self.fpga.NUMBER_OF_CHANNELIZERS))

    def values(self):
        """
        """
        return self.chan

    def items(self):
        """
        """
        return list(zip(self.keys(), self.values()))

    # Low-level access functions

    # def read(self, chan_number, module_number, addr, *args, **kwargs):
    #     """ Reads from the register of a module of a specified channelizer"""
    #     fpga = self.fpga
    #     data = fpga.read(fpga.ANT_PORT[chan_number], module_number, addr, *args, **kwargs)
    #     return data

    # def write(self, chan_number, module_number, addr, data, *args, **kwargs):
    #     """ Writes to the register of a module of a specified channelizer"""
    #     fpga = self.fpga
    #     fpga.write(fpga.ANT_PORT[chan_number], module_number, addr, data, *args, **kwargs)

    def init(self):
        """ Initializes all channelizer modules.

        Clock selection: the CRS has no ADC mezzanine, so the channelizers are
        always clocked from the internal system clock (CHAN_CLK_SRC=1).
        """
        self.logger.debug(f'{self!r}: Using the internal clock to generate the channelizer clock')
        self.fpga.GPIO.CHAN_CLK_SRC = 1

        for chan in self.chan:
            chan.init()

        for key in ("fft", "scaler", "funcgen"):
            if key not in self.config:
                self.config.add(key)

    def status(self):
        """ Displays the status of all channelizer modules"""
        for chan in self.chan:
            chan.status()

        #self.chan[1].ADCDAQ.set_divclk_phase(1) # Adjust phase of the DIVCLK signal to allow proper sampling of the deserialized words

    def set_data_width(self, width):
        """
        Set the number of bits used to represent the values computed by the channelizers.

        All channelizers are set to the new setting.

        Parameters:

            width (int): Target data width:

                - width=4: data is 4 bits Real + 4 bits Imaginary
                - width=8: data is 8 bits Real + 8 bits Imaginary
        """

        # Set the channelizer data width
        for ch in self.chan:
            ch.SCALER.set_output_bit_width(width)

    def get_data_width(self):
        """
        Returns number of bits used to represent the values computed by the channelizers.
        If all the channelizer are not set in the same mode, an error is raised.
        """

        four_bits = set() # use a set to uniquely record all the possible encountered states

        for ch in self.chan:
            four_bits.add(ch.SCALER.FOUR_BITS)

        if four_bits == set([0]):
            return 8
        elif four_bits == set([1]):
            return 4
        else:
            raise RuntimeError("The channelizers are not set to the same data width.")


