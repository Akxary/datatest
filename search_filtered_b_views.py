import os

from main import target_main_dir
import multiprocessing


layer_dir = 's_grnplm_as_t_didsd_016_db_stg\\view'
b_l = os.listdir(target_main_dir+layer_dir)
b_l = list(filter(lambda x: 'v.s_grnplm_as_t_didsd_016_db_stg.b' in x, b_l))
fil_files = []
print(len(b_l))

if __name__ == '__main__':
    for b in b_l:
        with open(target_main_dir+layer_dir+'\\'+b) as file:
            lines = ''.join(file.readlines())
            if ('JOIN ' or 'join ') in lines:
                print(b)
                fil_files.append(b)
    print(len(fil_files))

