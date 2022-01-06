import antenneregisterparser
import CleaningAndLoadingData
import TDTableInserts
import time
from datetime import datetime

def main():
    f = open('log.txt', 'a')
    t0 = time.time()
    now = datetime.today()
    f.write('Starting parsing and loading process at {0}. \n'.format(now))
    print('Starting parsing and loading process at {0}.'.format(now))
    antenneregisterparser.main()
    CleaningAndLoadingData.main()
    TDTableInserts.main()
    now = datetime.today()
    f.write('Parsing and loading process finished at {0}. \n'.format(now))
    t1 = time.time()
    i, d = divmod((t1 - t0) / 60 / 60, 1)
    print('Whole process took {0} hours and {1} minutes.'.format(round(i), round(d * 60, 1)))
    f.write('Whole process took {0} hours and {1} minutes. \n\n\n'.format(round(i), round(d * 60, 1)))
    f.close()

if __name__ == '__main__':
    main()