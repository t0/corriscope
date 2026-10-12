"""Implements the interface to the REFCLK FPGA module.

.. History:
    2011-09-22 JFC: Created
    2011-09-25 JFC: Modified to support new method on incrementing phase (pulse PS_EN unstead of PS_CLK)
    2011-11-15 JFC: Lots of modifications done to debug SYNC clock alignment.
    2012-05-xx JFC: Added disabling SYNC detect when the board is not there, because a floating input create spurious clocks and cause intermittent resets
    2012-09-05 JFC: Updated registers to match firmware. Includes a few status registers to debug SYNC generation mechanism. Added ENABLE_SYNC_GENERATION flag handling to fix spurious generation of SERDES_RST when FMC boar dis not present (the software LOCAL_SYNC and REFCLK noise got the SYNC state machine started and left it in SERDES_RST=1 state)
    2012-09-23 JFC: Removed MMCM status registers. Converted bitfield list to independent variables. Commented out set_refclk200_phase.
"""

import logging
import time
import numpy as np
from ..mmi import MMI, BitField, CONTROL, STATUS, DRP

class REFCLK(MMI):

    ADDRESS_WIDTH = 12

    sync_delay = 9  # default value.

     # CONTROL bytes
    ADC_SYNC               = BitField(CONTROL, 0, 7, doc='Force a SYNC to the ADC, synchronized on the FMC Reference clock, but bypasses the SYNC state machine that resets the IOSERDES and BUFR')
    LOCAL_SYNC             = BitField(CONTROL, 0, 5, doc='Force the generation of a local SYNC sequence on the local board only. Has the same effect as a SYNC signed received on the 10 MHz clock.  The SYNC is synchronized to the 10 MHz output (transitions on its falling edge)')
    REMOTE_SYNC            = BitField(CONTROL, 0, 4, doc='Generate a SYNC signal encoded on the 10 MHz clock output. Will SYNC the local FMC board only if the 10 MHz output is connected to the 10 MHz input of the local FMC board')
    SYNC_SOURCE            = BitField(CONTROL, 0, 0, width=3, doc="Selects the source of the SYNC signal: 0: local sync only, 1: local sync or sync recovered from the clock, 2: local sync or backplane sync, 3: local sync or IRIG-B-based sync")

    REFCLK_SEL             = BitField(CONTROL, 0x01, 7, doc='Selects the source of the REFCLK needed for SYNC generation. 0=FMC, 1=internal REFCLK generator.')
    # ENABLE_SYNC_GENERATION = BitField(CONTROL, 0x01, 6, doc='Allows the internal state machine to generate the SYNC sequence (generate the ADC SYNC and resets the ADCDAQ SERDES and BUFG)')
    # ENABLE_SYNC_DETECTION  = BitField(CONTROL, 0x01, 5, doc='When 1, enable SYNC detection based on the Refecence clock pulse length. Disable if the FMC board is not present to prevent spurious resets of the data path.')
    REFCLK1_DELAY           = BitField(CONTROL, 0x01, 0, width=5, doc='Delay applied to the FMC1 Reference clock. Must pulse REFCLK_DELAY_RST to load.')

    REFCLK_DELAY_RST       = BitField(CONTROL, 0x02, 7, doc='Resets the REFCLK line IODELAY and loads the delay value specified in REFCLK0_DELAY.')
    REFCLK0_DELAY           = BitField(CONTROL, 0x02, 0, width=5, doc='Delay applied to the FMC0 Reference clock. Must pulse REFCLK_DELAY_RST to load.')

    # STATUS bytes
    SYNC_CTR               = BitField(STATUS, 0x00, 4, width=4, doc='Counts the SYNC events')
    DCI_LOCKED             = BitField(STATUS, 0x00, 3, doc='1 when DCI is locked')
    RECOVERED_SYNC         = BitField(STATUS, 0x00, 2, doc='1 when a SYNC signal encoded on the 10 MHz is detected ')
    SYNC                   = BitField(STATUS, 0x00, 1, doc='Status on the internal SYNC signal, which is a combination of various sources (recovered from RefClk, from pin, from bit etc)')
    SERDES_RST             = BitField(STATUS, 0x00, 0, doc='Status of the SERDER Reset output line')

    DIFF_COUNTER           = BitField(STATUS, 0x01, 0, width=8, doc='DIfference between clocks')

    SYNC_DONE              = BitField(STATUS, 0x02, 5, doc='1 when the local SYNC process is completed')
    SYNC_IN                = BitField(STATUS, 0x02, 0, doc='reflects the level on the sync_in port')
    # SYNC_DELAY_READBACK  = BitField(STATUS, 0x02, 0, width=5,doc='Reads back the delay set onthe SYNC IODELAY')

    def __init__(self, *, router, router_port, config):
        self.logger = logging.getLogger(__name__)
        super().__init__(router=router, router_port=router_port)
        self.config = config

    def init(self):
        # Sets the REFCLK delay to zero by default.
        # self.set_refclk_delay(0)
        self.set_sync_delays(1)
        self.logger = logging.getLogger(__name__)

        # If the board is not present, disable SYNC detection on REFCLK to
        # prevent noise on the floating REFCLK lien to generate spurious
        # resets.

        # self.ENABLE_SYNC_DETECTION = 1
        # self.ENABLE_SYNC_GENERATION = 1
        if self.fpga.HAS_FMC and self.fpga.is_fmc_present(0):
            self.logger.debug(f'{self.fpga!r}:   REFCLK is using the 10 MHz reference clock from the ADC board')
            self.REFCLK_SEL = 0  # Use REFCLK coming from the FMC
        else:
            self.logger.debug(f'{self.fpga!r}:   REFCLK is using the 10 MHz reference clock from FPGA since the ADC board is not present in FMC slot 0')
            self.REFCLK_SEL = 1  # Use internally generated REFCLK

    SYNC_SOURCE_TABLE = {
        'local': 0,  # No external trigger, software only
        'refclk': 1,  # Refclk pulse width
        'bp_trig': 2,  # Backplane trig line
        'irigb': 3,  # Output of the IRIG-B timestamp comparator
        'bp_time': 4,  # Backplane time signal
        'bp_gpio_int': 5,  # backplane GPIO interrupt line
        'sma_a': 6,  # IceBoard SMA_A
        'sma_b': 7}  # Iceboard SMA B

    def set_sync_source(self, source):
        """ Set the source of the SYNC signal."""
        if source not in self.SYNC_SOURCE_TABLE:
            raise ValueError(f"Invalid SYNC source name. Valid names are {', '.join(self.SYNC_SOURCE_TABLE)}")
        self.SYNC_SOURCE = self.SYNC_SOURCE_TABLE[source]

    def get_sync_source(self):
        """ Get the name of the current source of the SYNC signal."""
        source = self.SYNC_SOURCE
        for (source_name, source_number) in self.SYNC_SOURCE_TABLE.items():
            if source == source_number:
                return source_name
        raise ValueError('The REFCLK module has an unknown SYNC source')

    def remote_sync(self, delay=None):
        """
        """
        self.set_sync_delays(delay)
        self.pulse_bit('REMOTE_SYNC')
        self.wait_for_bit('SYNC_DONE')

    async def remote_sync_async(self, delay=None):
        """Async version of :meth:`remote_sync`."""
        self.set_sync_delays(delay)
        self.pulse_bit('REMOTE_SYNC')
        await self.wait_for_bit_async('SYNC_DONE')

    def local_sync(self, delay=None, max_trials=3):
        """
        Locally generates a SYNC pulse on the current board's ADCs and reset the data acquisition logic.
        This is the same as receiving a SYNC signal encoded on the 10 MHz reference clock.
        The timing of the sync pulse (delay relative to FMC 10 MHz reference clock can optionally be specified).

        Parameters:
            delay: Specify sync signal timing delays

                - If delay=None or is omited, the previous SYNC timing will be used.
                - If delay is an integer between 0 and 31, the timing delay is set to that value.

            max_trials (int):
        """
        self.set_sync_delays(delay)
        # self.pulse_bit('LOCAL_SYNC') # Force the REFCLK state machine to initiate a SYNC event

        trial = 0
        while True:
            self.LOCAL_SYNC = 1
            self.LOCAL_SYNC = 0
            if self.wait_for_bit('SYNC_DONE', no_error=True):  # Wait until the SYNC process is completed
                return
            trial += 1
            if trial >= max_trials:
                raise RuntimeError(f'Local SYNC failed after {int(trial)} trials')
            else:
                self.logger.warning(f'{self!r}: Local SYNC failed on trial {int(trial)}/{int(max_trials)}. Retrying...')

    async def local_sync_async(self, delay=None, max_trials=3):
        """Async version of :meth:`local_sync`. Yields while waiting for ``SYNC_DONE``."""
        self.set_sync_delays(delay)
        trial = 0
        while True:
            self.LOCAL_SYNC = 1
            self.LOCAL_SYNC = 0
            if await self.wait_for_bit_async('SYNC_DONE', no_error=True):
                return
            trial += 1
            if trial >= max_trials:
                raise RuntimeError(f'Local SYNC failed after {int(trial)} trials')
            self.logger.warning(
                f'{self!r}: Local SYNC failed on trial {int(trial)}/{int(max_trials)}. Retrying...')

    def set_sync_delays(self, delay):
        """
        Sets the delay of the ADC SYNC pulse relative to the Reference Clock.


        The delay is stored in an internal variable (self.sync_delays) which is used to set the
        REFCLK delay prior to generate SYNC events.

        Parameters:

        delay (tuple): (mez0_delay, mezz1_delay) tuple specifying the SYNC
            delay for each mezzanine. Valid range for delay values is 0-31.
            If delay is None, the SYNC delays are set to the stored
            sync_delay.
        """
        if delay is None:
            self.set_refclk_delay(self.sync_delay)

        else:
            self.set_refclk_delay(delay)
            self.sync_delay = self.get_refclk_delay()  # Save the current delay value

        # if set_sync_delay was called with -1 we didn't actually set anything so the saved value
        # shouldnt be -1 it should be what ever is actually used

    def get_sync_delays(self):
        return self.sync_delay

    def get_refclk_delay(self):
        """
        Get the current delays on the Mezzanine Reference clock for both mezzanines.
        """
        return (self.REFCLK0_DELAY, self.REFCLK1_DELAY)

    def set_refclk_delay(self, delay):
        """
        Sets the delay applied on the Mezzanine Reference clock of the two
        mezzanines as they enter the FPGA. Valid range is 0-31. These delays
        are used to scan the ADC clock waveform and to set the ADC SYNC
        timing.
        """
        # print(f'Setting REFCLK delays to {delay}')
        try:
            d0, d1 = delay
        except TypeError:
            d0 = d1 = delay
        if d0 != -1:
            self.REFCLK0_DELAY = d0
        if d1 != -1:
            self.REFCLK1_DELAY = d1
        self.pulse_bit('REFCLK_DELAY_RST')

    def status(self):
        """
        Displays the status of the REFCLK module.
        """
        # self.logger.info('-----------------------REFCLK------------------------------------')
        # self.logger.info('SYNC Detection Enabled: %s' % (bool(self.ENABLE_SYNC_DETECTION)))
