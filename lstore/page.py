class Page:

  def __init__(self):
    self.num_records = 0
    self.data = bytearray(4096)

  def has_capacity(self):
    # each page can hold 512 8 bytes records
    return self.num_records < 512

  def write(self, value, slot):
    """
    Writing to the page. Slot number has to be specified. Value is converted into bytes because
    the page stores data in a byte array.
    """
    # Value is a 64 bit (aka 8byte) int

    if slot < 0 or slot > self.num_records:
      raise IndexError("Writing invalid slot in physical page")

    val_to_byte = value.to_bytes(8, byteorder='big')
    # Insert by bytes
    i = slot * 8
    for byte in val_to_byte:
      self.data[i] = byte
      i += 1
    self.num_records += 1


  def read(self, slot):
    if slot > 512 or slot < 0 or slot >= self.num_records: raise IndexError("Index out of range")

    return int.from_bytes(self.data[slot * 8: (slot + 1) * 8], byteorder='big')
