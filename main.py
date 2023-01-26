import os
import openpyexcel

target_main_dir = 'C:\\Users\\pavel.grishaev\\Documents\\01.23\\10, 12\\way4\\way4\\DWH\\greenplum\\'
t_layer_dir = 's_grnplm_as_t_didsd_db_dwh\\table'
b_and_n_layer_dir = 's_grnplm_as_t_didsd_016_db_stg\\table'

c_part_b = 't.s_grnplm_as_t_didsd_016_db_stg.b'
c_part_n = 't.s_grnplm_as_t_didsd_016_db_stg.n'

t_l = os.listdir(target_main_dir + t_layer_dir)
b_n_l = os.listdir(target_main_dir + b_and_n_layer_dir)

b_l = list(filter(lambda file: c_part_b in file, b_n_l))

n_l = list(filter(lambda file: c_part_n in file, b_n_l))


if __name__ == '__main__':

    xl_file = openpyexcel.load_workbook('C:\\Users\\pavel.grishaev\\Documents\\01.23\\10\\notes.xlsx',
                                        read_only=False)
    sheet = xl_file['s_tables']
    b_in = set([str(sheet.cell(i + 1, 1).value).lower() for i in range(1, 68)])
    b_l = set(map(lambda x: 'b' + str(x).removeprefix(c_part_b).removesuffix('.sql'), b_l))

    noneq1 = list(b_l.symmetric_difference(b_in))
    noneq2 = list(b_in.difference(b_l))

    print(len(noneq2))
    sheet = xl_file['B_filtered']
    for i in range(len(noneq2)):
        sheet[f'B{2 + i}'].value = noneq2[i]
    xl_file.save('C:\\Users\\pavel.grishaev\\Documents\\01.23\\10\\notes1.xlsx')
    xl_file.close()
