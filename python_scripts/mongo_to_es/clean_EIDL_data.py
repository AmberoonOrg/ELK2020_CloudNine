import csv
import os

for dirpath, dnames, fnames in os.walk("/home/abhilash/EID_loans"):
    for f in fnames:
        with open(os.path.join(dirpath, f), 'r') as infile, open(os.path.join(dirpath, f + '_cleaned.csv'), 'w') as outfile:
            writer = csv.writer(outfile)
            cr =  csv.reader(infile)
            writer.writerow(next(cr)[:29])
            for row in cr:
                writer.writerow(row[:29])