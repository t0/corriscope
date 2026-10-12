"""Declarative list of hardware platforms supported by corriscope.

Edit this file to add, remove, or update a hardware platform. Each entry
tells ``HardwareMap`` how to locate and import the corresponding class on
demand:

- ``type_``            : hardware base class (``Motherboard`` or ``Crate``). Determines which in-memory class
                         registry the entry belongs to.
- ``name``             : class name to import from the ``package`` below.
- ``part_number``      : official part number for the class, or ``None`` for
                         generic / base classes that do not represent real
                         hardware by themselves.
- ``ipmi_part_numbers``: tuple of equivalent part numbers that may appear in
                         IPMI / mDNS records.
- ``package``          : subpackage under ``corriscope.hardware``. The class is
                         imported as ``from corriscope.hardware.<package> import
                         <name>``.

Do not import ``HARDWARE`` from outside the ``corriscope.hardware`` package;
use :class:`HardwareMap` classmethods (``load_class``, ``get_class_by_*``,
``get_all_supported_classes``, ``get_supported_ipmi_part_numbers`` ...)
instead.
"""

from .motherboard import Motherboard
from .crate import Crate


HARDWARE = (
    dict(type_=Motherboard, name='CRS',                  part_number='CRS',        ipmi_part_numbers=('CRS',),               package='crs'),
    dict(type_=Crate,       name='CRSBackplane',         part_number='CRS_BP',     ipmi_part_numbers=('CRS_BP', 'CRSBP', 'CRS-BP', 'CRS'), package='crs'),
)
