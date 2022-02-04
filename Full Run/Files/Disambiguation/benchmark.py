
import pandas as pd
import re
import numpy as np
#import disambiguation.analysis as da
import analysis as da


#Static methods

def get_hn(add):
    hn = re.search('[0-9]+', add)
    return hn.group()

def get_st(add):
    try:
        st = re.search('(?<=[0-9]\\s)([A-Z]|\\s)+(?=,)', add)
        return st.group()
    except:
        return None