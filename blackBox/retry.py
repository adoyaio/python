import sys
import time

INITIAL_SLEEP_TIME = 5 # seconds
SLEEP_MULTIPLIER   = 1.4
RETRY_LIMIT        = 20

def retry(func):
  def wrapper(*args, **kw):
    doneB = False
    sleepTime = INITIAL_SLEEP_TIME
    retryCount = 0

    while not doneB:
      try:
        return func(*args, **kw)

      except Exception as e:
        if retryCount >= RETRY_LIMIT:
          print(f"{func.__name__} raised exception {e}.  Retried {retryCount} times.  Giving up.")
        
          raise

        print(f"{func.__name__} raised exception {e}.  Retrying in {sleepTime} seconds.",
              file=sys.stderr)

        time.sleep(sleepTime)
        sleepTime *= SLEEP_MULTIPLIER

        retryCount += 1
        kw["@retryCount"] = retryCount

  wrapper.__name__ = f"{func.__name__} decorated with @retry."
  return wrapper



# ------------------------------------------------------------------------------
# ------------------------------------------------------------------------------
# ------------------------------------------------------------------------------
if __name__ == "__main__":
  @retry
  def test(a, b, *args, x=10, y={1:2, "a": "b"}, **kw):
    print(f"Args to test() are a={a}, b={b}, args={args}, x={x}, y={y}, kw={kw}.")

    if kw.get("@retryCount") == 3:
      print("Normal return")
      return

    else:
      print("Raising ValueError")
      raise ValueError("Testing retry decorator")

  test(100, "Hello, world", "Another positional arg", x=33, y={55: 66, "c": "d"}, z="Another kw arg")
