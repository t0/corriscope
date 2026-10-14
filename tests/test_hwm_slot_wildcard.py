"""Hardware-map parser: '*:slot' wildcards record the requested crate slots."""
import logging

import pytest

from corriscope.fpga_array import FPGAArray
from corriscope.hardware import HardwareMap, Motherboard


@pytest.fixture(autouse=True)
def clean_hardware_map():
    HardwareMap.clear_hardware_map()
    yield
    HardwareMap.clear_hardware_map()


def _parse(hwm):
    fa = FPGAArray(hwm)  # not awaited: no hardware access
    fa.logger = logging.getLogger('test')  # normally set by init_async
    fa.process_str_hwm(hwm)
    return fa


def test_slot_wildcards_are_recorded():
    fa = _parse('crs *:1 *:2 0229:3')
    assert fa._wildcard_slots == {1, 2}
    boards = Motherboard.get_all_registered_instances()
    serials = sorted(str(mb.serial) for mb in boards)
    assert serials == ['*', '0229']  # leading zero: the parser keeps the serial as a string
    (explicit,) = [mb for mb in boards if mb.serial != '*']
    assert explicit.slot == 3


def test_plain_wildcard_has_no_slots():
    fa = _parse('crs *')
    assert fa._wildcard_slots == set()


def test_slot_zero_is_rejected():
    with pytest.raises(ValueError):
        _parse('crs *:0')


def test_deleting_a_board_frees_its_crate_slot():
    """init_async drops boards outside the requested slots; the crate must forget them too."""
    from corriscope.hardware.crs.crs import CRS
    from corriscope.hardware.crs.crs_bp import CRSBackplane
    crate = CRSBackplane.get_unique_instance(serial='008')
    # discovery creates the board, then attaches it to its crate slot with update_instance
    kept = CRS.get_unique_instance(serial='0229').update_instance(slot=1, crate=crate)
    dropped = CRS.get_unique_instance(serial='0213').update_instance(slot=2, crate=crate)
    assert set(crate.slot) == {1, 2}
    dropped.delete_instance()
    assert crate.slot == {1: kept}
    assert dropped not in Motherboard.get_all_registered_instances()
