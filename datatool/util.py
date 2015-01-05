
def first(it):
  try:
    return iter(it).next()
  except StopIteration:
    return None