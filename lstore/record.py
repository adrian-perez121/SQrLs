class Record:

  def __init__(self, indirection, rid, timestamp, schema_encoding, key, columns):
    self.indirection = indirection
    self.rid = rid
    self.timestamp = timestamp
    self.schema_encoding = schema_encoding
    self.key = key
    self.columns = columns