from config import python
from subprocess import call


call([python, "3_reduce_images.py"])
call([python, "4_align.py"])
call([python, "5_stack.py"])
