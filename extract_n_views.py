import os
import csv

main_path = 'C:\\Users\\pavel.grishaev\\Documents\\01.23\\10-31\\way4\\way4\\' \
            'DWH\\greenplum\\s_grnplm_as_t_didsd_016_db_stg\\view'
files = os.listdir(main_path)

prefix = 'v.s_grnplm_as_t_didsd_016_db_stg.n016'


class FilteredViews:
    def __init__(self, fls, pref, out_name, key=''):
        self.out_name = out_name
        self.key = key
        self.files = list(filter(lambda x: pref in x, fls))

    def reader(self, _file):
        with open(main_path + '\\' + _file, mode='r', encoding='utf-8') as sql_file:
            lines = ''.join(self.f_ltr(sql_file.readlines()))
            return lines

    def f_ltr(self, lines):
        return filter(lambda x: self.key in x, lines)

    def file_writer(self):
        with open(self.out_name + '.csv', mode='w', encoding='utf-8') as csvfile:
            csvwriter = csv.writer(csvfile, delimiter='\t')
            for file in self.files:
                csvwriter.writerow([file, self.reader(file)])


# files = list(filter(lambda x: prefix in x, files))
#
#
# def reader(_file):
#     with open(main_path + '\\' + _file, mode='r', encoding='utf-8') as sql_file:
#         lines = ''.join(sql_file.readlines())
#         return lines


if __name__ == '__main__':
    FV = FilteredViews(fls=files, pref=prefix, out_name='n_views_1')
    FV.file_writer()
    # with open('n_views.csv', mode='w', encoding='utf-8') as csvfile:
    #     csvwriter = csv.writer(csvfile, delimiter='\t')
    #     for file in files:
    #         csvwriter.writerow([file, reader(file)])
