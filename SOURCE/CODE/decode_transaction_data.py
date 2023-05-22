import uu

    
class DecodeTransactionData:
    def __init__(self,infile,outfile,csvfile):
        self.infile = infile
        self.outfile = outfile
        self.csvfile = csvfile

    def decode_data(self):
        uu.decode('SOURCE/RESOURCE/{}'.format(self.infile), 'SOURCE/RESOURCE/{}'.format(self.outfile))
        with open('SOURCE/RESOURCE/{}'.format(self.csvfile),'w') as clndata:
            with open('SOURCE/RESOURCE/{}'.format(self.outfile),'r') as f:
                 i = 0
                 for line in f:
                    if i == 0:
                        newline='id' + line.replace('"','')
                        newline = newline.replace('|', ',')
                        i = 1
                    else:
                        newline = line.replace('"','').replace('|', ',')
            
                    clndata.write(newline) 
