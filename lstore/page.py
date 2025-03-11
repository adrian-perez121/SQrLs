
class Page:

    def __init__(self):
        self.num_records = 0
        self.data = bytearray(4096)

    def has_capacity(self):
        pass

<<<<<<< Updated upstream
    def write(self, value):
        self.num_records += 1
        pass
=======
  def write(self, value, slot):
    """
    Writing to the page. Slot number has to be specified. Value is converted into bytes because
    the page stores data in a byte array.
    """
    # Value is a 64 bit (aka 8byte) int
    val_to_byte = value.to_bytes(8, byteorder='big')
    # Insert by bytes
    i = slot * 8
    for byte in val_to_byte:
      self.data[i] = byte
      # TODO: 2PL | Maybe lock?
      i += 1
>>>>>>> Stashed changes

