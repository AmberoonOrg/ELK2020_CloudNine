import csv
import os
import re
import numpy as np

for dirpath, dnames, fnames in os.walk("/home/abhilash/SDIAllDefinitions_CSV"):
    with open('concept_mappings.csv', 'w') as write_file:
        writer = csv.writer(write_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        sum = 1
        for f in fnames:
            print(f)
            if (f.endswith(".csv")  and (f is not "Readme.txt")):
                file = open(os.path.join(dirpath, f), encoding='cp1252')
                csv_reader = csv.reader(file)
                if sum == 1:
                    next(csv_reader)
                    sum -= 1
                else:
                    next(csv_reader)
                    next(csv_reader)
                for row in csv_reader:
                    writer.writerow(row)
            file.close()

            