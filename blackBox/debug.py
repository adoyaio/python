#! /usr/bin/python3

import os
import sys
import time


DEBUG = True
debugG = True
debugIndentG = 0



# ------------------------------------------------------------------------------
def disableDebug():
  global debugG


  debugG = False



# ------------------------------------------------------------------------------
def enableDebug():
  global debugG


  debugG = True



# ------------------------------------------------------------------------------
def tag():
  now = time.time()

  return time.strftime("%d-%b-%Y %H:%M:%S", time.localtime(now)) + "." + \
         ("%06d" % (int(now * 1000000) % 1000000)) + \
         ("  " * debugIndentG) + \
         " "



# ------------------------------------------------------------------------------
def debug(func):
  def wrapper(*args, **kw):
    global debugIndentG


    dprint("Entering %s with args=%s, kw=%s." % (func.__name__, args, kw))
    debugIndentG += 1;

    try:
      result = func(*args, **kw)

    except Exception as e:
      debugIndentG -= 1;
      dprint("EXCEPTION from %s: %s" % (func.__name__, e))
      raise   

    debugIndentG -= 1;
    dprint("Leaving %s with result=%s." % (func.__name__, result))
    return result

  wrapper.__name__ = "%s decorated by @debug" % func.__name__
  return wrapper if DEBUG else func



# ------------------------------------------------------------------------------
def dprint(message):
  if DEBUG and debugG:
    print ("%s%s" % (tag(), message))



# ------------------------------------------------------------------------------
@debug
def testHelper(x, y):
  print ("This is the testHelper() function.  Surrounding debug messages should be indented.")



# ------------------------------------------------------------------------------
@debug
def test(a, b, *args, c=123, d=456, **kw):
  print ("This is the test() function.  Args: a='%s', b='%s', *args=%s, c=%s, d=%s, kw=%s." % \
        (a, b, args, c, d, kw))
  if a != "Argument for a" or \
     b != "Argument for b" or \
     args != ("Varargs here", "and here", "and also here") or \
     c != "Argument for c" or \
     d != "Argument for d" or \
     kw != {"e" : "Extra argument e",
            "f" : "Extra argument f"
           }:
    raise ZeroDivisionError("Incorrect parameters passed to test() function.")

  testHelper(a, b)

  result = [1, 2, 3]
  print ("The test() function will now return the value %s." % result)
  return result



# ------------------------------------------------------------------------------
if not os.getcwd().endswith("cgi-bin"): # If not running as a cgi-bin script...
  dprint("Starting '%s'." % sys.argv[0]) # PRINT START-UP INFORMATION.



# ------------------------------------------------------------------------------
# ------------------------------------------------------------------------------
# ------------------------------------------------------------------------------
if __name__ == "__main__":
  result = test("Argument for a",
                "Argument for b",
                "Varargs here", "and here", "and also here",
                c="Argument for c",
                d="Argument for d",
                e="Extra argument e",
                f="Extra argument f")

  if result != [1, 2, 3]:
    raise ZeroDivisionError("Return value was '%s', should be [1, 2, 3]." % result)
