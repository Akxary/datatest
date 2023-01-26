import csv
import aiofiles
import sqlparse as sp
import re
import os
import asyncio

path_main = 'C:\\Users\\pavel.grishaev\\Documents\\01.23\\10-31\\way4\\' \
            'way4\\DWH\\greenplum\\s_grnplm_as_t_didsd_016_db_stg\\view'
prefix = 'v.s_grnplm_as_t_didsd_016_db_stg.c'
file_name = 'out_c&n_views'


class ViewParser:
    def __init__(self, flr: str, prf: str, out_name: str):
        self.out_name = out_name
        self.flr = flr
        self.files = list(filter(lambda x: prf in x, os.listdir(path_main)))
        self.lock = asyncio.Lock()
        self.out_dict = {}

    def filtered(self, lines: str) -> list[dict[str, str]]:
        tables = None
        col_list = []
        if self.flr in lines:
            tables = re.findall(r'[.]k_' + r'[0-9A-Za-z_]+', lines)
            for tt in tables:
                col_list.append({})
                col_list[-1]['table'] = tt.lstrip('.')
                try:
                    alias = re.findall(tt + r' [0-9A-Za-z_]+', lines)[0].removeprefix(tt).lstrip(' ')
                except IndexError:
                    alias = col_list[-1]['table']
                col_list[-1]['alias'] = alias
                cols = list(set(map(lambda ss: ss.removeprefix(alias).lstrip('.'),
                                    re.findall(alias + r'[.][0-9A-Za-z_]+', lines))))
                col_list[-1]['cols'] = cols
        return col_list

    async def file_opener(self, file) -> None:
        async with aiofiles.open(path_main + "\\" + file, mode='r', encoding='utf-8') as sql_file:
            lines = await sql_file.readlines()
            lines = self.filtered(''.join(lines))
            async with self.lock:
                self.out_dict[file] = lines

    async def main(self):
        await asyncio.gather(*(self.file_opener(f) for f in self.files))

    def file_writer(self):
        with open(self.out_name + ".csv", mode='w', encoding='utf-8') as csvfile:
            csvwriter = csv.DictWriter(csvfile, ['file', 'table', 'alias', 'col'], delimiter='\t')
            csvwriter.writeheader()
            for file in self.files:
                for table in self.out_dict[file]:
                    for col in table['cols']:
                        csvwriter.writerow({'file': file, 'table': table['table'],
                                            'alias': table['alias'], 'col': col})


if __name__ == '__main__':
    VP = ViewParser(flr='.k_', prf=prefix, out_name=file_name)
    asyncio.run(VP.main())
    VP.file_writer()
