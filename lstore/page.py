
class Page:

    def __init__(self):
        self.num_records = 0
        self.data = bytearray(4096)

    def has_capacity(self):
        # each page can hold 512 8 bytes records
        return self.num_records < 512

    def write(self, value):
        # Value is a 64 bit (aka 8byte) int
        if self.has_capacity():
            val_to_byte = value.to_bytes(8, byteorder='big')

            i = self.num_records * 8
            for byte in val_to_byte:
                self.data[i] = byte
                i += 1
            # self.data[self.num_records * 8 :  self.num_records * 8 + 8]
            self.num_records += 1
        else:
            raise "Page is full"

