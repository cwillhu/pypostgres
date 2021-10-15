import re, subprocess, time, csv
from collections import namedtuple


# Run subprocess 
def subproc(command):
    proc = subprocess.Popen(command,
                            shell=True,
                            stdout=subprocess.PIPE,
                            stderr=subprocess.STDOUT)
    text = proc.stdout.read().decode('utf-8')
    returnVal = proc.wait()
    time.sleep(1)

    ProcResult = namedtuple("ProcResult", "text returnVal")
    return ProcResult(text, returnVal)


# String with substitution method
class SubbableStr():
    def __init__(self, s):
        self.str = s

    def sub(self, pat, repl):
        return SubbableStr(re.sub(pat, repl, self.str))    

    
# Make string postgres-friendly (no spaces, caps, leading numbers)    
def postgresify(name):

    name = SubbableStr(name.lower().strip())
    return name.sub(r'"','') \
               .sub(r'\s+','_') \
               .sub(r'^1st','first') \
               .sub(r'^2nd','second') \
               .sub(r'^3rd','third') \
               .sub(r'^4th','fourth') \
               .sub(r'^5th','fifth') \
               .sub(r'^99th','ninetyninth') \
               .sub(r'^98th','ninetyeighth') \
               .sub(r'^95th','ninetyfifth') \
               .sub(r'^90th','ninetieth') \
               .sub(r'^75th','seventyfifth') \
               .sub(r'^50th','fiftieth') \
               .sub(r'^10th','tenth') \
               .sub(r'^1','one') \
               .sub(r'^2','two') \
               .sub(r'^3','three') \
               .sub(r'^4','four') \
               .sub(r'^5','five') \
               .sub(r'^6','six') \
               .sub(r'^7','seven') \
               .sub(r'^8','eight') \
               .sub(r'^9','nine') \
               .str


# Return list of col names found in csvfile. Format names for postgres.
def get_formattedNames(csvfile=None, headerLine=None):

    if csvfile:
        with open(csvfile, 'r') as f:
            reader = csv.reader(f)
            colNames = next(reader)
    elif headerLine:
        f = StringIO(headerLine)
        reader = csv.reader(f)
        colNames = next(reader)
    else:
        raise ValueError('csvfile or headerLine must be specified')
    
    return [postgresify(x) for x in colNames]

