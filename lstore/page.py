import json
import base64

class Page:

  def __init__(self, num_records=0):
    # self.num_records = num_records
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
    val_to_byte = value.to_bytes(8, byteorder='big')
    # Insert by bytes
    i = slot * 8
    for byte in val_to_byte:
      self.data[i] = byte
      i += 1

  def read(self, slot):
    return int.from_bytes(self.data[slot * 8: (slot + 1) * 8], byteorder='big')

  @classmethod
  def from_dict(cls, path):
    new_page = cls()

    # read the page from disk
    with open(path, 'rb') as data_file:
        new_page.data = bytearray(data_file.read())

    return new_page

  # def to_json_string(self):
  #   return json.dumps(self.to_dict())

  # @classmethod
  # # def from_json_string(cls, json_data):
  # def from_json_string(cls):
  #   # data = json.loads(json_data)
  #   new_page = Page()
  #   # new_page.data = base64.b64decode(data["byte_array"])
  #   new_page.num_records = data["num_records"]
  #   return new_page